#!/bin/sh
":" //# -*- mode: js -*-; exec /usr/bin/env node --max-old-space-size=9000 --stack-size=42000 "$0" "$@"

"use strict";

/************************************/
/* MODULE VARIABLE SECTION **********/
/************************************/

const os = require('os')
const osProcess = require('process')
const osPath = require( 'path' )

const expandHomeDir = require( 'expand-home-dir' )
const fs = require( 'fs-extra' )
const mimeDb = require( 'mime-db' )
const mime = require( 'mime-types' )

const packageInfo = require('./package.json')
const genericPool = require( 'generic-pool' )
const asyncRead = require('promised-read').read
const cheerio = require('cheerio')
const command = require('commander')

const csvOutput = require('csv-stringify')

const moment = require("moment")
require("moment-duration-format")

const startTime = Date.now()

function elapsedStr( from , to = Date.now()) {
    return moment.duration( to - from ).format('d[d]hh:mm:ss.SSS',{ stopTrim: "h" })
}

function log ( ...args ) {
    console.log( elapsedStr( startTime ), ... args )
}

function warning ( ...args ) {
    log( ...args )
}

function fatal ( ...args ) {
    log( ...args )
    osProcess.exit( 1 )
}

//~ var lzma = require('lzma-native')
try {
  var lzma = require('xz')
} catch (er) {
    if ( os.type() == 'Windows_NT' ) {
        fatal( 'Module "xz" is not available on Windows' )
    } else {
        fatal( 'Module "xz" is required' )
    }
}
//~ var lzma = require('node-liblzma')

var srcPath;
var outPath;
var src; // input file reader

var articles = null;
var metadata = [];

function readUInt64LE(buf, offset) {
    var lowBits = buf.readUInt32LE(offset);
    var highBits = buf.readUInt32LE(offset + 4);
    return highBits * 0x100000000 + lowBits
};

function blobPath(clusterIdx, blobIdx) {
    return osPath.join(outPath, clusterIdx + '-' + blobIdx + '-blob');
}

function articlePath(article) {
    return osPath.join(outPath, article.url);
}

//
// class Reader
//
class Reader {
    constructor ( path ) {
        this.path = path;
        this.position = 0;
        this.file = fs.open( path, 'r' )

        this.queue = genericPool.createPool(
            {
                async create () { return Symbol() },
                async destroy ( resource ) { },
            },
            {}
        )
    }

    async read ( length, position ) {

        const token = await this.queue.acquire()
        const fd = await this.file

        if (typeof position !== 'number')
            position = this.position
        this.position = position + length

        const data = Buffer.alloc(length)
        const bytes = await fs.read( fd, data, 0, length, position )
        this.queue.release( token )
        return data
    }

    async close () {
        await this.queue.drain()
        const fd = await this.file
        await fs.close( fd )
    }

    tell () {
        return this.position
    }
}

var headerLength = 80;

var header = {
    magicNumber: 72173914,  //    integer     0   4   Magic number to recognise the file format, must be 72173914
    version: 5,             //    integer     4   4   ZIM=5, bytes 1-2: major, bytes 3-4: minor version of the ZIM file format
    uuid: 0,                //    integer     8   16  unique id of this zim file
    articleCount: 0,        //    integer     24  4   total number of articles
    clusterCount: 0,        //    integer     28  4   total number of clusters
    urlPtrPos: 0,           //    integer     32  8   position of the directory pointerlist ordered by URL
    titlePtrPos: 0,         //    integer     40  8   position of the directory pointerlist ordered by Title
    clusterPtrPos: 0,       //    integer     48  8   position of the cluster pointer list
    mimeListPos: headerLength, // integer     56  8   position of the MIME type list (also header size)
    mainPage: 0xffffffff,   //    integer     64  4   main page or 0xffffffff if no main page
    layoutPage: 0xffffffff, //    integer     68  4   layout page or 0xffffffffff if no layout page
    checksumPos: 0,         //    integer     72  8   pointer to the md5checksum of this file without the checksum itself. This points always 16 bytes before the end of the file.
    geoIndexPos: 0,         //    integer     80  8   pointer to the geo index (optional). Present if mimeListPos is at least 80.
};

async function readHeader ( ) {
    log('reading header')
    const buf = await src.read( headerLength, 0 )

    header.articleCount = buf.readUInt32LE(24);
    header.clusterCount = buf.readUInt32LE(28);

    header.urlPtrPos = readUInt64LE(buf, 32);
    header.titlePtrPos = readUInt64LE(buf, 40);
    header.clusterPtrPos = readUInt64LE(buf, 48);
    header.mimeListPos = readUInt64LE(buf, 56);

    header.mainPage = buf.readUInt32LE(64);
    header.layoutPage = buf.readUInt32LE(68);

    log('header', header);
}

async function processClusterList ( ) {
    log('reading ClusterPointers')
    const buf = await src.read( header.clusterCount * 8, header.clusterPtrPos )

    try {
        for ( let i=0; i < header.clusterCount; i++ ) {
            await processCluster( buf, i )
        }
    } catch ( err ) {
        fatal( 'processClusterList', err )
    }
};

async function processCluster( buf, clusterIdx ) {
    var eof = false;

    const clusterOfs = readUInt64LE( buf, clusterIdx * 8 )

    async function readCompression () {
        const buf = await src.read( 1, clusterOfs )

        return buf.readUInt8(0) & 4;  // xz compressed
    }

    async function getSource( isCompressed ) {
        var slice = fs.createReadStream(
            src.path,
            {
                start: clusterOfs + 1,
                // autoClose: false,
            }
        );

        slice.on('error', function (err) {
            console.error('processCluster', clusterIdx, 'input error', err);
            //~ process.exit(1);
        });

        slice.on('end', function () {
            log('processCluster', clusterIdx, 'input end');
            eof = true;
            //~ process.exit(1);
        });

        slice.on('close', function () {
            log('processCluster', clusterIdx, 'input closed');
            eof = true;
            //~ process.exit(1);
        });

        slice.on('open', function (fd) {
            log('processCluster', clusterIdx, 'input open', fd);
        });

        if ( isCompressed ) { // xz compressed
            const decompressed = new lzma.Decompressor()
            slice.pipe( decompressed )
            return decompressed
        }
        return slice
    }

    async function readOffsets ( input ) {
        const offsets = []
        let noffsets
        for ( var buf; buf = await asyncRead( input,  4 );) {
            var ofs = buf.readUInt32LE( 0 )
            if ( offsets.length == 0 ) {
                noffsets = ofs / 4
            }
            //~ log('readOffsets', clusterIdx, noffsets, offsets.length, ofs);
            offsets.push(ofs)

            if ( offsets.length == noffsets ) {
                //~ log('readOffsets done', clusterIdx, noffsets, offsets.length, ofs);
                return offsets
            }
        }
        fatal( 'readOffsets prematire stream end' )
    }

    async function dumpBlobs ( input, offsets ) {
        for ( let i=0; i < offsets.length-1; i++ ) {

            const blobLen = offsets[ i + 1 ] - offsets[ i ]
            const blob = blobLen === 0 ?
                Buffer.alloc(0)
                : await asyncRead( input, blobLen )
            await fs.outputFile( blobPath( clusterIdx, i ), blob )

            //~ log('readBlobs', clusterIdx, isCompressed, nblobs, i, blobLen)
        }

            //~ log('readBlobs done', clusterIdx, isCompressed, nblobs, blobIdx, blobLen)
    }

    let input

    try {
        const isCompressed = await readCompression()
        log('processCluster', clusterIdx, header.clusterCount, isCompressed);

        input = await getSource( isCompressed )
        const offsets = await readOffsets( input )
        await dumpBlobs( input, offsets )
    } catch ( err ) {
        if (!eof) {
            //~ slice.fd = null;
            input && input.destroy()
        }
        fatal( 'processCluster error', clusterIdx, header.clusterCount, err )
    }
}

async function getDirEntry ( article ) {
    let chunkLen = 512;
    let dirEntry

    function parseDirEntry () {
        article.mimeIdx = dirEntry.readUInt16LE(0);
        article.nameSpace = dirEntry.toString('utf8', 3, 4);

        var strOfs = 16;
        if (article.mimeIdx ==  0xfffe || article.mimeIdx ==  0xfffd) {
            // linktarget or deleted entry
            return true // noop
        } else if (article.mimeIdx ==  0xffff ) { //redirect
            strOfs = 12;
            article.redirectIndex = dirEntry.readUInt32LE(8);
        } else {
            article.clusterIdx = dirEntry.readUInt32LE(8);
            article.blobIdx = dirEntry.readUInt32LE(12);
        }

        // read url and title
        var end = dirEntry.indexOf(0, strOfs);
        if (end != -1) {
            article.url = dirEntry.toString('utf8', strOfs, end);

            var strOfs = end + 1;
            end = dirEntry.indexOf(0, strOfs);
            if (end != -1) {
                article.title = dirEntry.toString('utf8', strOfs, end);
            }
        }

        if (end == -1) // short buffer -- read more
            return false

        log('parseDirEntry', article.index, header.articleCount, '\n', article);

        articles[article.index] = article

        return true
    }

    try {
        while ( true ) {
            dirEntry = await src.read( chunkLen, article.offset )
            if ( parseDirEntry() )
                return article
            chunkLen *= 2
        }
    } catch ( err ) {
        fatal( 'processdirEntry read error', article.index, header.articleCount, err )
    }
}

async function renameBlob( article ) {

    var bpath = blobPath(article.clusterIdx, article.blobIdx)

    if (article.nameSpace == 'M') { // metadata
        const data = await fs.readFile ( bpath, 'utf8' )
        metadata.push([article.url.toLowerCase(), data])
        return fs.unlink( bpath )
    }
    const apath = articlePath( article )

    log('renameBlob', article.index, header.articleCount, bpath, '->', apath )

    return fs.move( bpath, apath, { clobber: true })
}

async function loadArticle( article ) {
    if (article.nameSpace != 'A')
        return null
    const data = await fs.readFile( articlePath( article ))

    try {
        const dom = cheerio.load( data )
        return dom
    } catch ( e ) {
        log( 'cheerio.load error', e, data )
        return null
    }
}

var nameSpaces = ['-', 'A', 'B', 'I', 'J', 'M', 'U', 'W', 'X'];

function alterLinks( article, dom ) {
    var nameSpaceLink = function (elem, attr) {
        let link
        try {
            link = url.parse(elem.attribs[attr], true, true)
        } catch (err) {
            //~ console.error('alterLinks error', err, article, attr, elem.attribs[attr], elem)
            console.error('alterLinks', err.message, elem.attribs[attr], 'at', article.path)
            return
        }
        if ( (link.protocol && link.protocol != 'http:' && link.protocol != 'https:')
                || link.host || ! link.pathname)
            return

        var chunks = link.pathname.split('/')

        if ( chunks[0] == '' // abs path
                || chunks[0] == '..'
                && nameSpaces.indexOf(chunks[1]) != -1) {
            chunks.shift();
            chunks.shift();
            link.pathname = chunks.join('/');
            //~ log('alterLinks', elem.attribs[attr], url.format(link));
            elem.attribs[attr] = url.format(link);
            return // OK
        }
        return
    }

    dom( '[src]' ).each( (i, elem) => nameSpaceLink( elem, 'src' ))
    dom( '[href]' ).each( (i, elem) => nameSpaceLink( elem, 'href' ))
}

async function processArticle ( articleIndex ) {
    if ( articles[ articleIndex ] != null )
        return true // already processed

    const article = {
        index: articleIndex,
        offset: readUInt64LE( rawDirectory, articleIndex * 8 )
    }

    await getDirEntry( article )

    if ( article.mimeIdx ==  0xfffe || article.mimeIdx ==  0xfffd ) {
        // linktarget or deleted entry
        return true // noop
    }
    if ( article.mimeIdx ==  0xffff ) { //redirect
        return storeRedirect( article )
    }

    const moved = await renameBlob( article )
    if (! moved )
        return null
    const dom = await loadArticle( article )
    if (! dom )
        return null
    await alterLinks( article, dom )
    return fs.outputFile( articlePath( article ), Buffer.from( dom.html() ))
}

var rawDirectory

async function processArticleList () {
    log('reading ArticleList')
    articles = Array( header.articleCount )
    rawDirectory = await src.read(header.articleCount * 8, header.urlPtrPos )

    //~ log( 'articleOffsets', articleOffsets);

    for ( let i=0; i < header.articleCount; i++ ) {
        await processArticle( i )
    }
    log( '*** articles' )
    articles.forEach( (val, i ) => log( i, val.nameSpace, val.url ))

    if ( redirectOut )
        return new Promise( ( resolve, reject ) => {
            redirectOut.end( resolve )
        })
}

async function processTitleList () {
    log('reading Title List')
    const titleDirectory = await src.read( header.articleCount * 4, header.titlePtrPos )

    //~ log( 'articleOffsets', articleOffsets);
    log( '*** titles' )

    for ( let i=0; i < header.articleCount; i++ ) {
        const idx = titleDirectory.readUInt32LE( i * 4 )
        log( i, idx, articles[ idx ].nameSpace, articles[ idx ].title, '>', articles[ idx ].url )
    }
}

var redirectOut = null

function storeRedirect ( article ) {
    log('storeRedirect', article)

    if (article.nameSpace == '-' && (article.url == 'favicon' || article.url == 'mainPage'))
        return

    if (! redirectOut) {
        redirectOut = csvOutput({delimiter: '\t'})
        redirectOut.pipe(fs.createWriteStream(osPath.join(outPath, '..', 'redirects.csv')))
    }

    var target = articles[ article.redirectIndex ]
    if (! target) { // fetch target artcile isn't yet processed
        return processArticle( article.redirectIndex )
            .then(() => storeRedirect( article ))
    }

    var item = [ article.nameSpace, article.url, article.title, target.url ]

    log('storeRedirect', item)

    return new Promise(( resolve, reject ) => {
        var write = function () {
            try {
                if (! redirectOut.write(item))
                    return redirectOut.once('drain', write)
                resolve( false )
            } catch ( err ) {
                reject( err )
            }
        }
        write()
    })
}

function storeMetadata () {
    log('storeMetadata');
    if ( metadata.length == 0 )
        return

    var csv = csvOutput({ delimiter: ' ' })
    csv.pipe( fs.createWriteStream( osPath.join( outPath, '..', 'metadata.csv' )))

    return new Promise(( resolve, reject ) => {
        var write = function () {
            try {
                var i = 0;
                var write = function () {
                    while (true) {
                        if ( i == metadata.length ) {
                            log('storeMetadata finished');
                            return csv.end( resolve );
                        }
                        var item = metadata[i];
                        log('storeMetadata', metadata.length, i, item);
                        if (! csv.write( item ))
                            break;
                        i++
                    }
                    csv.once( 'drain', write )
                }
            } catch ( err ) {
                reject( err )
            }
        }
        write()
    })
}

async function core () {
    src = new Reader(srcPath)

    await readHeader( )
    await processClusterList()
    await processArticleList()
    await processTitleList()
    await storeMetadata()

    await src.close()
}

function main () {
    command
    .version( packageInfo.version )
    .arguments( '<wiki-page-URL>' )
    .description( 'Dumps a ZIM file' )
    .option( '-h -help' )
    .parse( process.argv )

    log( command.opts() )

    srcPath = expandHomeDir( command.args[0] )
    outPath = expandHomeDir( command.args[1] )
    if (! outPath ) {
        var parsed = osPath.parse(srcPath)
        outPath = parsed.name
    }

    core()
}

main ()
