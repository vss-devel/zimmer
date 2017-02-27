#!/bin/sh
":" //# -*- mode: js -*-; exec /usr/bin/env TMPDIR=/tmp node --max-old-space-size=2000 --stack-size=42000 "$0" "$@"

"use strict";

/*

MIT License

Copyright (c) 2016 Vadim Shlyakhov

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

*/

var os = require('os');
var process = require('process');
var yargs = require( 'yargs' );
var util = require('util');
var fs = require( 'fs' );
var async = require( 'async' );
var osPath = require( 'path' );
var expandHomeDir = require( 'expand-home-dir' );
var lzma = require('lzma-native');
var htmlparser = require("htmlparser2");
var domutils = require("domutils");
var url = require('url');
var uuid = require("uuid");
var crypto = require("crypto");
var csvParse = require('csv-parse');
var zlib = require('zlib');
var sqlite3 = require('sqlite3').verbose();
var mimeDb = require( 'mime-db' );
var mime = require( 'mime-types' );
var magic = require('mmmagic');

var mimeMagic = new magic.Magic(magic.MAGIC_MIME_TYPE);

var argv;

var cpuCount = os.cpus().length;

var srcPath;
var outPath;
var out; // output file writer

var auxDb;
var dirQueue;

var articleCount = 0;
var redirectCount = 0;
var resolvedRedirectCount = 0;

var mainPage;

var headerLength = 80;
var header = {
    magicNumber: 72173914,  //    integer      0  4   Magic number to recognise the file format, must be 72173914
    version: 5,             //    integer      4  4   ZIM=5, bytes 1-2: major, bytes 3-4: minor version of the ZIM file format
    uuid: null,             //    integer      8 16  unique id of this zim file
    articleCount: null,     //    integer     24  4   total number of articles
    clusterCount: null,     //    integer     28  4   total number of clusters
    urlPtrPos: null,        //    integer     32  8   position of the directory pointerlist ordered by URL
    titlePtrPos: null,      //    integer     40  8   position of the directory pointerlist ordered by Title
    clusterPtrPos: null,    //    integer     48  8   position of the cluster pointer list
    mimeListPos: headerLength, // integer     56  8   position of the MIME type list (also header size)
    mainPage: 0xffffffff,   //    integer     64  4   main page or 0xffffffff if no main page
    layoutPage: 0xffffffff, //    integer     68  4   layout page or 0xffffffffff if no layout page
    checksumPos: null,      //    integer     72  8   pointer to the md5checksum of this file without the checksum itself. This points always 16 bytes before the end of the file.
    //~ geoIndexPos: null,  //    integer     80  8   pointer to the geo index (optional). Present if mimeListPos is at least 80.
};

function fullPath (path) {
    return osPath.join(srcPath, path);
}

function getMimeType (path, realPath) {
    var mType = mime.lookup(realPath || path);
    if (mType == null) {
        console.error('No mime type found', path, realPath);
    }
    return mType;
};

var REDIRECT_MIME = '@REDIRECT@';

var mimeTypeList = [];
var mimeTypeCounter = [];

var maxMimeLength = 512;

function mimeTypeIndex (mimeType) {
    if (mimeType == null) {
        console.trace('No mime type found', mimeType);
        process.exit(1);
    }
    if (mimeType == REDIRECT_MIME)
        return 0xffff;
    var idx = mimeTypeList.indexOf(mimeType);
    if (idx != -1) {
        mimeTypeCounter[idx]++;
    } else {
        idx = mimeTypeList.length;
        mimeTypeList.push(mimeType);
        mimeTypeCounter.push(1);
    }
    return idx;
};

function getNameSpace(mimeType) {
    if (argv.uniqueNamespace)
        return 'A';
    if (!mimeType)
        return null;
    if (mimeType == 'text/html')
        return 'A'
    else if (mimeType.split('/')[0] == 'image')
        return 'I';
    return '-';
};

function log (arg) {
    argv && argv.verbose && console.log.apply(this, arguments);
}

function writeUIntLE(buf, number, offset, byteLength) {
    offset = offset || 0;
    if (typeof number == 'string') {
        byteLength = buf.write(number, offset);
        return offset + byteLength;
    }
    if (byteLength == 8) {
        var low = number & 0xffffffff;
        var high = (number - low) / 0x100000000 - (low < 0 ? 1 : 0);
        buf.writeInt32LE(low, offset);
        buf.writeUInt32LE(high, offset + 4);
        return offset + byteLength;
    } else {
        return buf.writeIntLE(buf, number, offset, byteLength);
    }
}

//
// CallbackQueue
//
function CallbackQueue (consumer, logIdent) {
    this.logIdent = logIdent;
    this.finished = false;
    this.paused = false;
    this.resumeCb = [];
    this.finalCb = null;

    this.queue = async.queue(consumer);
    this.queue.concurrency = cpuCount;
    this.queue.buffer = cpuCount;

    this.queue.drain = this.resume.bind(this, 'drain');
    this.queue.unsaturated = this.resume.bind(this, 'unsaturated');
    this.queue.saturated = function () {
        log(logIdent, 'saturated', this.queue.length(), this.queue.running(), this.queue.buffer);
        this.paused = true;
    }.bind(this);
};

CallbackQueue.prototype.resume = function (logInfo) {
    log(this.logIdent, logInfo, this.finished, this.queue.length(), this.queue.running(), this.queue.buffer, this.finalCb);
    this.paused = false;
    var cb;
    while (cb = this.resumeCb.shift())
        async.setImmediate(cb);
    if (this.finished && this.finalCb && this.queue.idle()) {
        async.setImmediate(this.finalCb);
        this.finalCb = null;
    }
};

CallbackQueue.prototype.push = function (data, callback) {
    // NB: undefined data may get srtipped from the arguments
    var logIdent = this.logIdent;
    var queue = this.queue;
    log(logIdent, queue.length(), queue.running(), queue.buffer, arguments);
    var cb;
    if (data && callback) {
        queue.push(data);
        cb = async.apply(callback, null, true);
        if (this.paused) {
            this.resumeCb.push(cb);
        } else {
            async.setImmediate(cb);
        }
    } else {
        this.finished = true;
        var cb = function () {
            log(logIdent, 'finished');
            (callback || data) (null, false);
        };
        this.finalCb = cb;
        this.resume('finishing');
    }
};

//
// Writer
//

function Writer (path, callback) {
    this.position = 0;

    this.queue = async.queue(this.writer.bind(this));

    this.stream = fs.createWriteStream(path, {highWaterMark: 1024*1024*10});
    this.stream.once('open', function (fd) {
        callback && callback(null, fd);
    });
    this.stream.on('error', this.error.bind(this));
};

Writer.prototype.error = function (err) {
    console.trace('Writer error', this.stream.path, err);
    process.exit(1);
};

Writer.prototype.writer = function (data, callback) {
    var offset = this.position;

    var saturated = !this.stream.write(data);
    this.position += data.length;
    if (!callback)
        return
    var cb = async.apply(callback, null, offset);
    if (saturated) {
        this.stream.once('drain', cb);
    } else {
        async.setImmediate(cb);
    }
};

Writer.prototype.write = function (request,  callback) {
    this.queue.push(request,  callback);
};

Writer.prototype.close = function (callback) {
    var onclose = function () {
        log(this.stream.path, 'closed', this.position, this.stream.bytesWritten);
        callback(null, this.position);
    }.bind(this);

    this.stream.once('close', onclose);

    var close = function () {
        log('closing', this.stream.path);
        this.stream.end();
    }.bind(this);

    if (this.queue.idle())
        async.setImmediate(close);
    else
        this.queue.drain = close;

    return this.position;
};

//
// Cluster
//
var Cluster = function (ordinal, compressible) {
    this.ordinal = ordinal;
    this.compressible = compressible;
    this.blobs = [];
    this.size = 0;
}

// Cluster.sizeThreshold = 8 * 1024 * 1024;
Cluster.sizeThreshold = 4 * 1024 * 1024;
// Cluster.sizeThreshold = 2 * 1024 * 1024;
// Cluster.sizeThreshold = 2 * 1024 ;

Cluster.prototype.append = function (data) {
    var ordinal = this.ordinal;
    var blobNum = this.blobs.length;
    if (blobNum != 0 && this.size + data.length > Cluster.sizeThreshold)
        return false;

    this.blobs.push(data);
    this.size += data.length;
    return blobNum;
};

// Cluster
// Field Name          Type    Offset  Length  Description
// compression type    integer     0   1   0: default (no compression), 1: none (inherited from Zeno), 4: LZMA2 compressed
// The following data bytes have to be uncompressed!
// <1st Blob>          integer     1   4   offset to the <1st Blob>
// <2nd Blob>          integer     5   4   offset to the <2nd Blob>
// <nth Blob>          integer     (n-1)*4+1   4   offset to the <nth Blob>
// ...                 integer     ...     4   ...
// <last blob / end>   integer     n/a     4   offset to the end of the cluster
// <1st Blob>          data        n/a     n/a     data of the <1st Blob>
// <2nd Blob>          data        n/a     n/a     data of the <2nd Blob>
// ...                 data        ...     n/a     ...

Cluster.prototype.save = function (callback) {
    //~ log('Cluster.prototype.save', this.compressible, this.blobs);

    var nBlobs = this.blobs.length;
    if (nBlobs == 0)
        return async.setImmediate(callback);

    // generate blob offsets
    var offsets = Buffer.allocUnsafe((nBlobs + 1) * 4);
    var blobOffset = offsets.length;
    for (var i=0; i < nBlobs; i++) {
        offsets.writeUIntLE(blobOffset, i * 4, 4);
        blobOffset += this.blobs[i].length;
    }
    //~ log(this.ordinal,'generate blob offsets', nBlobs, offsets.length, i, blobOffset);
    offsets.writeUIntLE(blobOffset, i * 4, 4); // final offset

    // join offsets and article data
    this.blobs.unshift(offsets);
    var buf = Buffer.concat(this.blobs);
    var rawSize = buf.length;

    var compression = this.compressible ? 4 : 0;
    var ordinal = this.ordinal;

    async.waterfall(
        [
            function (cb) { // compress cluster
                if (! compression)
                    return cb(null, buf)
                else
                    lzma.compress(buf, 3 | lzma.PRESET_EXTREME, function(result) {
                    //~ lzma.compress(buf, 3, function(result) {
                        log('Cluster lzma compressed');
                        return cb(null, result);
                    });
            },
            function (data, cb) { // write cluster
                log('Cluster write', ordinal, compression); //, Buffer.concat([Buffer.from([compression]), data]));
                out.write( Buffer.concat([Buffer.from([compression]), data]), cb);
            },
            function (offset, cb) {
                log('Cluster saved', ordinal, offset);
                auxDb.run(
                    'INSERT INTO clusters (ordinal, offset) VALUES (?,?)',
                    [
                        ordinal,
                        offset
                    ],
                    cb
                );
            }
        ],
        callback
    );
};

//
// ClusterBuilder
//
var ClusterBuilder = {
    count: 2,
    true: new Cluster(0, true), // compressible cluster
    false: new Cluster(1, false), // uncompressible cluster
};

ClusterBuilder.append = function (article, callback) {
    //~ log('ClusterBuilder.append', arguments);

    var compressible = article.isCompressible();
    var data = article.data;
    var cluster = ClusterBuilder[compressible];
    var clusterNum = cluster.ordinal;
    var blobNum = cluster.append(data);
    var cb = function () {
        callback(null, clusterNum, blobNum);
    };

    if (blobNum !== false) {
        async.setImmediate(cb);
    } else { // store to a new cluster
        var oldCluster = cluster;
        cluster = ClusterBuilder[compressible] = new Cluster(ClusterBuilder.count++, compressible);
        clusterNum = cluster.ordinal;
        blobNum = cluster.append(data);
        oldCluster.save(cb);
    }

    log('ClusterBuilder.append', compressible, clusterNum, blobNum, data.length, article.url);
};

// The cluster pointer list is a list of 8 byte offsets which point to all data clusters in a ZIM file.
// Field Name  Type    Offset  Length  Description
// <1st Cluster>   integer     0   8   pointer to the <1st Cluster>
// <1st Cluster>   integer     8   8   pointer to the <2nd Cluster>
// <nth Cluster>   integer     (n-1)*8     8   pointer to the <nth Cluster>
// ...     integer     ...     8   ...

ClusterBuilder.storePointers = function (callback) {
    saveIndex (
        `
        SELECT
            offset
        FROM clusters
        ORDER BY ordinal;
        `,
        8, 'offset', 'clusterPtrPos', ClusterBuilder.count, 'ClusterBuilder', callback);
}

ClusterBuilder.finish = function (callback) {
    //~ log('ClusterBuilder.finish', ClusterBuilder);
    async.series([
        function (cb) {ClusterBuilder[true].save(cb)}, // save last compressible cluster
        function (cb) {ClusterBuilder[false].save(cb)}, // save last uncompressible cluster
        ClusterBuilder.storePointers
        ],
        callback
    );
};

//
// Article
//
function Article (path, mimeType, data, nameSpace, title) {
    if (! path)
        return;
    this.mimeType = mimeType;
    this.nameSpace = nameSpace;
    this.url = path;
    this.title = title || '';
    this.data = data;
    this.ordinal = null;
    this.dirEntry = null;

    //~ log('Article', this);
};

Article.prototype.isCompressible = function () {
    var mimeType = this.mimeType;
    //~ log('isCompressible', this);
    if (this.data.length == 0)
        return false;
    if (!mimeType) {
        console.trace('Article.prototype.isCompressible mimeType', mimeType, this);
        process.exit(1);
    }
    if (mimeType == 'image/svg+xml' || mimeType.split('/')[0] == 'text')
        return true;
    return !! (mimeDb[mimeType] && mimeDb[mimeType].compressible);
};

Article.prototype.storeData = function (callback) {
    var data = this.data;
    if (data != null) {
        if (! (data instanceof Buffer)) {
            this.data = Buffer.from(data);
        }
    }
    ClusterBuilder.append( this, callback);
    //~ this.data = null;
};

Article.prototype.load = function (callback) {
    async.setImmediate(callback);
};

Article.prototype.process = function (callback) {
    var url = this.url;
    log('Article.prototype.process', this.url);

    this.load(function (err) {
        if (err || this.data == null)
            return callback(err);
        async.waterfall([
                this.storeData.bind(this),
                this.storeDirEntry.bind(this),
                this.toArticleIndex.bind(this)
            ],
            callback
        );
    }.bind(this));
};

// Article Entry
// Field Name      Type    Offset  Length  Description
// mimetype        integer     0   2   MIME type number as defined in the MIME type list
// parameter       len byte    2   1   (not used) length of extra paramters
// namespace       char        3   1   defines to which namespace this directory entry belongs
// revision        integer     4   4   (optional) identifies a revision of the contents of this directory entry, needed to identify updates or revisions in the original history
// cluster number  integer     8   4   cluster number in which the data of this directory entry is stored
// blob number     integer     12  4   blob number inside the compressed cluster where the contents are stored
// url             string      16  zero terminated     string with the URL as refered in the URL pointer list
// title           string      n/a     zero terminated     string with an title as refered in the Title pointer list or empty; in case it is empty, the URL is used as title
// parameter       data        see parameter len   (not used) extra parameters

Article.prototype.storeDirEntry = function (clusterNum, blobNum, callback) {
    // this also serves redirect dirEntry, but the later is shorter in 4 bytes
    var mimeIndex = mimeTypeIndex(this.mimeType);
    var buf = Buffer.allocUnsafe(blobNum != null ? 16 : 12);

    log('storeDirEntry', clusterNum, blobNum, mimeIndex, this);

    buf.writeUIntLE(mimeIndex,  0, 2);
    buf.writeUIntLE(0,          2, 1); // parameters length
    buf.write(this.nameSpace,   3, 1);
    buf.writeUIntLE(0,          4, 4); // revision
    buf.writeUIntLE(clusterNum, 8, 4); // or redirect target article index
    if (blobNum != null)
        buf.writeUIntLE(blobNum,12, 4);

    var urlBuf = Buffer.from(this.url + '\0');
    var titleBuf = Buffer.from(this.title + '\0');

    out.write(
        Buffer.concat([buf, urlBuf, titleBuf]),
        function (err, offset) {
            this.dirEntry = offset;
            log('storeArticleEntry done', err, offset, buf.length, this.url);
            callback(err);
        }.bind(this)
    );
}

Article.prototype.indexDirEntry = function (callback) {
    auxDb.run(
        'INSERT INTO dirEntries (articleId, offset) VALUES (?,?)',
        [
            this.articleId,
            this.dirEntry,
        ],
        callback
    );
};

Article.prototype.toArticleIndex = function (callback) {
    if (!this.url) {
        console.trace('Article no url', this);
        process.exit(1);
    }
    this.articleId = ++ articleCount;
    auxDb.serialize( function () {
        if (this.dirEntry)
            this.indexDirEntry();
        auxDb.run(
            'INSERT INTO articles (articleId, nsUrl, nsTitle, redirect) VALUES (?,?,?,?)',
            [
                this.articleId,
                this.nameSpace + this.url,
                this.nameSpace + (this.title || this.url),
                this.redirect
            ],
            callback
        );
    }.bind(this));
};

//
// class RedirectArticle
//
function RedirectArticle (path, nameSpace, title, redirect, redirectNameSpace) {
    Article.call(this, path, REDIRECT_MIME, null, nameSpace, title);
    this.redirect = (redirectNameSpace || 'A') + redirect;
    log('RedirectArticle', nameSpace, path, this.redirect, this);
};

util.inherits(RedirectArticle, Article);

RedirectArticle.prototype.process = function (callback) {
    redirectCount ++;
    this.toArticleIndex(callback);
};

//
// class ResolvedRedirect
//
function ResolvedRedirect (articleId, nameSpace, url, title, target) {
    Article.call(this, url, REDIRECT_MIME, null, nameSpace, title);
    this.target = target;
    this.articleId = articleId;
};

util.inherits(ResolvedRedirect, Article);

ResolvedRedirect.prototype.process = function (callback) {
    log('ResolvedRedirect.prototype.process', this.url);
    resolvedRedirectCount ++;
    async.series([
            this.storeDirEntry.bind(this),
            this.indexDirEntry.bind(this)
        ],
        callback
    );
};

// Redirect Entry
// Field Name      Type    Offset  Length  Description
// mimetype        integer 0       2       0xffff for redirect
// parameter len   byte    2       1       (not used) length of extra paramters
// namespace       char    3       1       defines to which namespace this directory entry belongs
// revision        integer 4       4       (optional) identifies a revision of the contents of this directory entry, needed to identify updates or revisions in the original history
// redirect index  integer 8       4       pointer to the directory entry of the redirect target
// url             string  12      zero terminated     string with the URL as refered in the URL pointer list
// title           string  n/a     zero terminated     string with an title as refered in the Title pointer list or empty; in case it is empty, the URL is used as title
// parameter       data    see parameter len   (not used) extra parameters

ResolvedRedirect.prototype.storeDirEntry = function (callback) {
    //~ log('RedirectArticle.prototype.storeDirEntry', this);

    // redirect dirEntry shorter on one 4 byte field
    Article.prototype.storeDirEntry.call(this, this.target, null, callback);
};

//
// class FileArticle
//
function FileArticle (path, realPath) {
    var mimeType = getMimeType(path, realPath);
    Article.call(this, path, mimeType);
    //~ log(this);
};

util.inherits (FileArticle, Article);

FileArticle.prototype.parse = function () {
    if (this.mimeType != 'text/html' ) {
        return null;
    }
    //~ log('FileArticle.prototype.parse', this);
    var text = this.data.toString();

    var parseError;
    var handler = new htmlparser.DomHandler(function (err, dom) {
            if (err)
                console.error(err, dom);
            parseError = err;
        });
    var parser = new htmlparser.Parser(
        handler,
        {decodeEntities: true}
    );

    parser.write(text);
    parser.end();

    if (parseError)
        return null;

    return handler.dom;
};

FileArticle.prototype.setTitle = function (dom) {
    var elem = domutils.getElementsByTagName('title', dom, true)[0];
    if (elem)
        this.title = domutils.getText(elem);
    return this.title;
};

FileArticle.prototype.alterLinks = function (dom) {
    var base = '/' + this.url;
    var nsBase = '/' + this.nameSpace + base;
    var baseSplit = nsBase.split('/');
    var baseDepth = baseSplit.length - 1;

    function toRelativeLink (elem, attr) {
        if (! (elem.attribs && elem.attribs[attr]))
            return false;
        try {
            var link = url.parse(elem.attribs[attr], true, true);
        } catch (err) {
            console.warn('alterLinks', err.message, elem.attribs[attr], 'at', base);
            return false;
        }
        var path = link.pathname;
        if ( link.protocol || link.host || ! path )
            return false;
        var nameSpace = getNameSpace(getMimeType(path));
        if ( ! nameSpace )
            return false;

        // convert to relative path
        var absPath = '/' + nameSpace + url.resolve(base, path);
        var to = absPath.split('/');
        var i = 0;
        for (; baseSplit[i] === to[0] && i < baseDepth; i++) {
            to.shift();
        }
        for (; i < baseDepth; i++) {
            to.unshift('..');
        }
        var relPath = to.join('/');
        log('alterLinks', nsBase, path, absPath, relPath);

        link.pathname = relPath;
        elem.attribs[attr] = url.format(link);
        return true;
    };

    var res = domutils.filter(
        function (elem) {
            return toRelativeLink(elem, 'src') || toRelativeLink(elem, 'href');
        },
        dom,
        true
    );
    log('alterLinks', res.length);

    return res.length != 0;
};

FileArticle.prototype.getRedirect = function (dom) {
    var target = null;

    var isRedirectLink = function (elem) {
        if (! (elem.attribs && elem.attribs['http-equiv'] == "refresh" && elem.attribs['content']))
            return false;

        var content = elem.attribs['content'].split(';');
        if (!(content[0] == 0 && content[1]))
            return false;

        log('FileArticle.prototype.getRedirect', this.url, content, content[1].split('=', 2)[1]);
        var link = url.parse(content[1].split('=', 2)[1], true, true);
        if (link.protocol || link.host || ! link.pathname || link.pathname[0] =='/')
            return false;

        target = decodeURIComponent(link.pathname);

        return true;
    } .bind(this);

    domutils.filter(
        isRedirectLink,
        dom,
        true
    );

    return target;
};

FileArticle.prototype.load = function (callback) {
    async.waterfall([
            async.apply(fs.readFile, fullPath(this.url)),

            function inflateData (data, cb) {
                if (argv.inflateHtml && this.mimeType == 'text/html')
                    zlib.inflate(data, cb)
                else
                    cb(null, data);
            }.bind(this),

            function mimeFromData (data, cb) {
                this.data = data;
                if (this.mimeType)
                    return cb();
                mimeMagic.detect(data, function(err, mimeType) {
                    log('FileArticle.prototype.load mimeMagic.detect', err, mimeType, this.url);
                    this.mimeType = mimeType;
                    cb(err);
                }.bind(this));
            }.bind(this),

            function parseHtml (cb) {
                this.nameSpace = this.nameSpace || getNameSpace(this.mimeType);
                var dom = this.parse();
                if (dom) {
                    this.setTitle (dom);
                    var target = this.getRedirect (dom);
                    if (target) { // convert to redirect
                        this.data = null;
                        var redirect = new RedirectArticle (this.url, this.nameSpace, this.title, target);
                        return redirect.process(cb);
                    }
                    if (this.alterLinks (dom))
                        this.data = Buffer.from(domutils.getOuterHTML(dom));
                }
                cb();
            }.bind(this),
        ],
        callback
    );
}

//
// General functions
//

// Keys
// Key             Mandatory   Description     Example
// Title           yes     title of zim file   English Wikipedia
// Creator         yes     creator(s) of the ZIM file content  English speaking Wikipedia contributors
// Publisher       yes     creator of the ZIM file itself  Wikipedia user Foobar
// Date            yes     create date (ISO - YYYY-MM-DD)  2009-11-21
// Description     yes     description of content  This ZIM file contains all articles (without images) from the english Wikipedia by 2009-11-10.
// Language        yes     ISO639-3 language identifier (if many, comma separated)     eng
// Relation        no      URI of external related ressources
// Source          no      URI of the original source  http://en.wikipedia.org/
// Counter         no      Number of non-redirect entries per mime-type    image/jpeg=5;image/gif=3;image/png=2;...

function loadMetadata (callback) {
    async.each([
            new Article ('Title', 'text/plain', argv.title, 'M'),
            new Article ('Creator', 'text/plain', argv.creator, 'M'),
            new Article ('Publisher', 'text/plain', argv.publisher, 'M'),
            new Article ('Date', 'text/plain', new Date().toISOString().split('T')[0], 'M'),
            new Article ('Description', 'text/plain', argv.description, 'M'),
            new Article ('Language', 'text/plain', argv.language, 'M'),
            new RedirectArticle ('favicon', '-', null, argv.favicon, 'I'),
            new RedirectArticle ('mainPage', '-', null, mainPage.path, 'A')
        ],
        function (article, cb) {
            article.process(cb);
        },
        callback
    );
}

function createAuxIndex(callback) {
    var dbName = '';
    if (argv.verbose) {
        var parsed = osPath.parse(outPath);
        dbName = osPath.join(parsed.dir, parsed.base + '.db');
    }
    fs.unlink(
        dbName,
        function () {
            auxDb = new sqlite3.Database(dbName);
            //~ auxDb.on('trace', function (sql) {log(sql);});
            //~ auxDb.serialize();
            auxDb.exec(
                'PRAGMA synchronous = OFF;' +
                'PRAGMA journal_mode = OFF;' +
                //~ 'PRAGMA journal_mode = WAL;' +
                'CREATE TABLE articles (' +
                    'articleId INTEGER PRIMARY KEY,' +
                    //~ 'ordinal INTEGER,' +
                    //~ 'dirEntry INTEGER,' +
                    'nsUrl TEXT,' +
                    'nsTitle TEXT,' +
                    'redirect TEXT ' +
                    ');' +
                'CREATE TABLE dirEntries (' +
                    'articleId INTEGER PRIMARY KEY,' +
                    'offset INTEGER' +
                    ');' +
                'CREATE TABLE clusters (' +
                    'ordinal INTEGER PRIMARY KEY,' +
                    'offset INTEGER ' +
                    ');',
                callback
            );
        }
    );
}

function sortArticles (callback) {
    auxDb.exec(
        'CREATE INDEX articleNsUrl ON articles (nsUrl);' +

        'CREATE TABLE urlSorted AS ' +
            'SELECT ' +
                'articleId ' +
            'FROM articles ' +
            'ORDER BY nsUrl;' +

        'CREATE INDEX urlSortedArticleId ON urlSorted (articleId);' +

        'CREATE INDEX articleNsTitle ON articles (nsTitle);' +
        '',
        callback
    );
}

function loadRedirects (callback) {
    if (!argv.redirects) {
        return callback();
    }
    var inp = fs.createReadStream(expandHomeDir(argv.redirects));
    var finished = false;

    var parser = csvParse(
        {
            delimiter: '\t',
            columns:['ns','path','title','target']
        }
    );
    parser.on('error', function (err) {
        console.log(err.message);
        callback(err);
    });
    parser.on('end', function () {
        log('loadRedirects finished');
        finished = true;
    });

    log('loadRedirects start');
    inp.pipe(parser);

    function reader (cb) {
        var row = parser.read();
        if (row || finished ) {
            async.setImmediate(cb, null, row);
        } else {
            parser.once('readable', async.apply(reader, cb));
        }
    }

    async.doDuring(
        reader,
        function (row, cb) {
            if (row && cb) {
                new RedirectArticle (row.path, row.ns, row.title, row.target)
                    .process( function (err) {
                        cb(err, true)
                    });
            } else {
                async.setImmediate(cb || row, null, false);
            }
        },
        callback
    );
};

function resolveRedirects (callback) {

    var stmt = auxDb.prepare(
        'SELECT ' +
            'src.articleId, ' +
            'src.nsUrl, ' +
            'src.nsTitle, ' +
            //~ 'src.redirect, ' +
            'u.rowid - 1 AS target ' +
        'FROM articles AS src ' +
        'JOIN articles AS dst ' +
        'JOIN urlSorted AS u ' +
        'ON src.redirect = dst.nsUrl AND u.articleId = dst.articleId ' +
        'WHERE src.redirect IS NOT NULL;'
    );
    function consumer (row, cb) {
        var nameSpace = row.nsUrl[0];
        var url = row.nsUrl.substr(1);
        var title = (row.nsTitle == row.nsUrl) ? '' : row.nsTitle.substr(1);
        if (url == 'mainPage')
            mainPage.target = row.target;

        new ResolvedRedirect (row.articleId, nameSpace, url, title, row.target)
            .process(cb);
    }
    var queue = new CallbackQueue(consumer, 'resolveRedirects');

    async.doDuring(
        stmt.get.bind(stmt),
        queue.push.bind(queue),
        callback
    );
};

function saveIndex (query, byteLength, rowField, headerField, count, logInfo, callback) {
    logInfo = logInfo || 'saveIndex';
    var i = 0;
    log(logInfo, 'start', count);

    var stmt = auxDb.prepare(query);

    async.doDuring(
        stmt.get.bind(stmt),
        function (row, cb) {
            log(logInfo, i, count, arguments);
            i++;
            // null row gets srtipped from the arguments
            if (row && cb) {
                var buf = Buffer.allocUnsafe(byteLength);
                buf.writeUIntLE(row[rowField], 0, byteLength);
                out.write(
                    buf,
                    function (err, offset) {
                        log(logInfo, 'finished', headerField, offset);
                        if (!header[headerField])
                            header[headerField] = offset;
                        cb(err, true);
                    }
                );
                return;
            }
            log(logInfo, 'done');
            (cb || row)(null, false);
        },
        callback
    );
}

// URL Pointer List (urlPtrPos)

// The URL pointer list is a list of 8 byte offsets to the directory entries.

// The directory entries are always ordered by URL (including the namespace prefix). Ordering is simply done by comparing the URL strings in binary.

// Since directory entries have variable sizes this is needed for random access.
// Field Name  Type    Offset  Length  Description
// <1st URL>   integer     0   8   pointer to the directory entry of <1st URL>
// <2nd URL>   integer     8   8   pointer to the directory entry of <2nd URL>
// <nth URL>   integer     (n-1)*8     8   pointer to the directory entry of <nth URL>
// ...     integer     ...     8   ...

function storeUrlIndex (callback) {
    saveIndex (
        'SELECT ' +
            'offset ' +
        'FROM dirEntries ' +
        'JOIN urlSorted ' +
        'USING (articleId) ' +
        'ORDER BY urlSorted.rowid; ' +
        '',
        8, 'offset', 'urlPtrPos', articleCount, 'storeUrlIndex', callback);
}

// Title Pointer List (titlePtrPos)

// The title pointer list is a list of article indices ordered by title. The title pointer list actually points to entries in the URL pointer list. Note that the title pointers are only 4 bytes. They are not offsets in the file but article numbers. To get the offset of an article from the title pointer list, you have to look it up in the URL pointer list.
// Field Name      Type        Offset  Length  Description
// <1st Title>     integer     0       4       pointer to the URL pointer of <1st Title>
// <2nd Title>     integer     4       4       pointer to the URL pointer of <2nd Title>
// <nth Title>     integer     (n-1)*4 4       pointer to the URL pointer of <nth Title>
// ...             integer     ...     4       ...

function storeTitleIndex (callback) {
    saveIndex (
        'SELECT ' +
            //~ 'nsTitle, ' +
            'urlSorted.rowid - 1 AS articleNumber ' +
        'FROM urlSorted ' +
        'JOIN articles ' +
        'USING (articleId) ' +
        'ORDER BY nsTitle; ' +
        '',
        4, 'articleNumber', 'titlePtrPos', articleCount, 'storeTitleIndex', callback);
}

function loadFiles(callback) {
    dirQueue = async.queue(scanDirectory, 1);

    dirQueue.drain = function () {
        log('loadFiles finished');
        dirQueue.kill();
        ClusterBuilder.finish(callback);
    };

    log('loadFiles start');
    dirQueue.push('');
}

function scanDirectory(path, callback) {
    log('scanDirectory', path);
    async.waterfall([
            async.apply(fs.readdir, fullPath(path)),
            function (dirEntries, cbk) {
                async.eachLimit(
                    dirEntries,
                    Math.round(cpuCount * 2),
                    function (fname, cb) {
                        parseDirEntry(osPath.join(path, fname), null, cb);
                    },
                    cbk
                );
            }
        ],
        callback
    );
}

function parseDirEntry(path, realPath, callback) {
    var absPath = realPath || fullPath(path);
    fs.lstat(
        absPath,
        function (err, stats) {
            //~ log('parseDirEntry', err, path, absPath, stats.isFile(), stats.isDirectory(), stats.isSymbolicLink() );
            if (err)
                return callback(err);
            if (stats.isDirectory()) {
                dirQueue.push(path);
                return callback();
            }
            if (stats.isSymbolicLink())
                return fs.realpath(fullPath(path), function (err, realPath) {
                    if (err)
                        return callback(err);
                    async.setImmediate(parseDirEntry, path, realPath, callback);
                });
            if (stats.isFile()) {
                return new FileArticle (path, realPath) .process(callback);
            }

            callback(new Error('Invalid dir entry ' + absPath));
        }
    )
}

function initialise (callback) {
    var stat = fs.statSync(srcPath); // check source
    if (!stat.isDirectory()) {
        return callback(new Error(srcPath + ' is not a directory'));
    }

    out = new Writer(outPath); // create output file
    log('reserving space for header and mime type list');
    out.write(Buffer.alloc(headerLength + maxMimeLength));

    createAuxIndex(callback);
}

function loadArticles (callback) {
    async.parallel([
            loadMetadata,
            loadFiles,
            loadRedirects,
        ],
        callback
    );
}

function storeIndices (callback) {
    async.parallel([
            //~ ClusterBuilder.finish,
            storeUrlIndex,
            storeTitleIndex
        ],
        callback
    );
}
/**/
function postProcess (callback) {
    async.series([
            sortArticles,
            resolveRedirects,
            //~ storeIndices,
            //~ ClusterBuilder.finish,
            storeUrlIndex,
            storeTitleIndex,
        ],
        callback
    )
}
/**/
/*
function postProcess (callback) {
    async.auto({
            clusters:   ClusterBuilder.finish,
            sort:       sortArticles,
            titles:     ['sort', function(results, cb) {storeTitleIndex(cb)}],
            resolve:    ['sort', function(results, cb) {resolveRedirects(cb)}],
            urls:       ['resolve', function(results, cb) {storeUrlIndex(cb)}],
        },
        callback
    );
}
/**/

// MIME Type List (mimeListPos)

// The MIME type list always follows directly after the header, so the mimeListPos also defines the end and size of the ZIM file header.

// The MIME types in this list are zero terminated strings. An empty string marks the end of the MIME type list.
// Field Name          Type    Offset  Length  Description
// <1st MIME Type>     string  0       zero terminated     declaration of the <1st MIME Type>
// <2nd MIME Type>     string  n/a     zero terminated     declaration of the <2nd MIME Type>
// ...                 string  ...     zero terminated     ...
// <last entry / end>  string  n/a     zero terminated     empty string - end of MIME type list

function getMimeTypes () {
    var buf = Buffer.from(mimeTypeList.join('\0') + '\0');
    log('MimeTypes', mimeTypeList.length, buf.length);

    if (buf.length > maxMimeLength) {
        console.error('Error: mime type list length >', maxMimeLength);
        process.exit(1);
    }
    return buf;
}

function getHeader () {
    header.articleCount = articleCount;
    header.clusterCount = ClusterBuilder.count;
    log('Header', 'articleCount', articleCount, 'clusterCount', ClusterBuilder.count, 'mainPage', mainPage);
    header.mainPage = mainPage.target || header.mainPage;

    var buf = Buffer.alloc(headerLength);
    buf.writeUIntLE(header.magicNumber,     0, 4);
    buf.writeUIntLE(header.version,         4, 4);

    uuid.v4(null, buf,                      8);

    buf.writeUIntLE(header.articleCount,    24, 4);
    buf.writeUIntLE(header.clusterCount,    28, 4);

    buf.writeUIntLE(header.urlPtrPos,       32, 8);
    buf.writeUIntLE(header.titlePtrPos,     40, 8);
    buf.writeUIntLE(header.clusterPtrPos,   48, 8);
    buf.writeUIntLE(header.mimeListPos,     56, 8);

    buf.writeUIntLE(header.mainPage,        64, 4);
    buf.writeUIntLE(header.layoutPage,      68, 4);

    buf.writeUIntLE(header.checksumPos,     72, 8);

    return buf;
}

function stroreHeader(callback) {
    var buf = Buffer.concat([getHeader(), getMimeTypes()]);
    var fd = fs.openSync(outPath, 'r+');
    fs.writeSync( fd, buf, 0, buf.length, 0);
    fs.close(fd, callback);
}

function calculateFileHash (callback) {
    var outHash;
    var hash = crypto.createHash('md5');
    var stream = fs.createReadStream(outPath);
    stream.on('data', function (data) {
        hash.update(data)
    })
    stream.on('end', function () {
        outHash = hash.digest();
        log('outHash', outHash);
        fs.appendFile(outPath, outHash, callback);
    })
}

function finalise (callback) {
    async.series([
            postProcess,
            function (cb) { // close the output stream
                header.checksumPos = out.close(cb);
            },
            stroreHeader,
            calculateFileHash
            ],
        callback
    )
}

function core () {
    async.series([
            initialise,
            loadArticles,
            //~ loadMetadata,
            //~ loadFiles,
            //~ loadRedirects,
            finalise
        ],
        function (err) {
            if (err) {
                console.trace(err);
                process.exit(1);
            }
            log('Done...');
        }
    );
}

// Mandatory arguments:
// -w, --welcome           path of default/main HTML page. The path must be relative to HTML_DIRECTORY.
// -f, --favicon           path of ZIM file favicon. The path must be relative to HTML_DIRECTORY and the image a 48x48 PNG.
// -l, --language          language code of the content in ISO639-3
// -t, --title             title of the ZIM file
// -d, --description       short description of the content
// -c, --creator           creator(s) of the content
// -p, --publisher         creator of the ZIM file itself

// HTML_DIRECTORY          is the path of the directory containing the HTML pages you want to put in the ZIM file,
// ZIM_FILE                is the path of the ZIM file you want to obtain.

// Optional arguments:
// -v, --verbose           print processing details on STDOUT
// -h, --help              print this help
// -m, --minChunkSize      number of bytes per ZIM cluster (defaul: 2048)
// -x, --inflateHtml       try to inflate HTML files before packing (*.html, *.htm, ...)
// -u, --uniqueNamespace   put everything in the same namespace 'A'. Might be necessary to avoid problems with dynamic/javascript data loading.
// -r, --redirects         path to the CSV file with the list of redirects (url, title, target_url tab separated).
// -i, --withFullTextIndex index the content and add it to the ZIM.

function main () {
    argv = yargs.usage( 'Pack a directory into a zim file\nUsage: $0'
               + '\nExample: $0 ' )
        .options({
            'w': {alias: 'welcome', default: 'index.htm'},
            'f': {alias: 'favicon', default: 'favicon.png'},
            'l': {alias: 'language', default: 'eng'},
            't': {alias: 'title', default: ''},
            'd': {alias: 'description', default: ''},
            'c': {alias: 'creator', default: ''},
            'p': {alias: 'publisher', default: ''},
            'v': {alias: 'verbose', type: 'boolean'},
            'm': {alias: 'minChunkSize', type: 'number'},
            'x': {alias: 'inflateHtml', type: 'boolean'},
            'u': {alias: 'uniqueNamespace', type: 'boolean'},
            //~ 'r': {alias: 'redirects', default: 'redirects.csv'},
            'r': {alias: 'redirects', default: ''},
            //~ 'i': {alias: 'withFullTextIndex', type:'boolean'},
            'h': {alias: 'help'}
        })
        .help('help')
        .strict()
        .argv;

    log(argv);

    var pargs = argv._;

    while (pargs[0] == '') // if mwoffliner prepends with empty extra parameter(s)
        pargs.shift();

    srcPath = expandHomeDir(pargs[0]);
    if (argv._[1])
        outPath = expandHomeDir(pargs[1])
    else {
        var parsed = osPath.parse(srcPath);
        outPath = parsed.name + '.zim';
    }

    if (argv.minChunkSize) {
        Cluster.sizeThreshold = argv.minChunkSize * 1024;
    }

    mainPage = {
        path: argv.welcome
    };

    core ();
}

main ();
