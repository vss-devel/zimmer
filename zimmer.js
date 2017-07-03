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

const Promise = require( 'bluebird' )
const os = require( 'os' )
const process = require( 'process' )
const yargs = require( 'yargs' )
const util = require( 'util' )
const fs = require( 'fs-extra' )
const osPath = require( 'path' )
const expandHomeDir = require( 'expand-home-dir' )
const lzma = require( 'lzma-native' )
const cheerio = require('cheerio')

const url = require( 'url' )
const uuid = require( "uuid" )
const crypto = require( "crypto" )
const csvParse = require( 'csv-parse' )
const csvParseSync = require( 'csv-parse/lib/sync' )
const zlib = require( 'mz/zlib' )
const sqlite = require( 'sqlite' )
const mimeDb = require( 'mime-db' )
const mime = require( 'mime-types' )
const magic = require( 'mmmagic' )
const sharp = require( 'sharp' )

const genericPool = require( 'generic-pool' )
const mozjpeg = require( 'mozjpeg' )

const mimeMagic = new magic.Magic( magic.MAGIC_MIME_TYPE )

const cpuCount = os.cpus().length

var argv

var srcPath
var outPath
var out // output file writer

var zIndex
var dirQueue

var articleCount = 0
var articleId = 0
var redirectCount = 0
var resolvedRedirectCount = 0

var zimFormated = false

var mainPage
var deadEndTarget

// -    layout, eg. the LayoutPage, CSS, favicon.png (48x48), JavaScript and images not related to the articles
// A    articles - see Article Format
// B    article meta data - see Article Format
// I    images, files - see Image Handling
// J    images, text - see Image Handling
// M    ZIM metadata - see Metadata
// U    categories, text - see Category Handling
// V    categories, article list - see Category Handling
// W    categories per article, category list - see Category Handling
// X    fulltext index - see ZIM Index Format

var headerLength = 80
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
}

function fullPath ( path ) {
    return osPath.join( srcPath, path )
}

function getMimeType ( path, realPath ) {
    var mType = mime.lookup( realPath || path )
    if ( mType == null ) {
        console.error( 'No mime type found', path, realPath )
    }
    return mType
}

var REDIRECT_MIME = '@REDIRECT@'
var LINKTARGET_MIME = '@LINKTARGET@'
var DELETEDENTRY_MIME = '@DELETEDENTRY@'

var mimeTypeList = []
var mimeTypeCounter = []

var maxMimeLength = 512

function mimeTypeIndex ( mimeType ) {
    if ( mimeType == null ) {
        console.trace( 'No mime type found', mimeType )
        process.exit( 1 )
    }
    if ( mimeType == REDIRECT_MIME )
        return 0xffff
    if ( mimeType == LINKTARGET_MIME )
        return 0xfffe
    if ( mimeType == DELETEDENTRY_MIME )
        return 0xfffd
    var idx = mimeTypeList.indexOf( mimeType )
    if ( idx != -1 ) {
        mimeTypeCounter[ idx ]++
    } else {
        idx = mimeTypeList.length
        mimeTypeList.push( mimeType )
        mimeTypeCounter.push( 1 )
    }
    return idx
}

function getNameSpace( mimeType ) {
    if ( argv.uniqueNamespace )
        return 'A'
    if ( !mimeType )
        return null
    if ( mimeType == 'text/html' )
        return 'A'
    else if ( mimeType.split( '/' )[ 0 ] == 'image' )
        return 'I'
    return '-'
}

function log ( arg ) {
    argv && argv.verbose && console.log.apply( this, arguments )
}

function writeUIntLE( buf, number, offset, byteLength ) {
    offset = offset || 0
    if ( typeof number == 'string' ) {
        byteLength = buf.write( number, offset )
        return offset + byteLength
    }
    if ( byteLength == 8 ) {
        var low = number & 0xffffffff
        var high = ( number - low ) / 0x100000000 - ( low < 0 ? 1 : 0 )
        buf.writeInt32LE( low, offset )
        buf.writeUInt32LE( high, offset + 4 )
        return offset + byteLength
    } else {
        return buf.writeIntLE( buf, number, offset, byteLength )
    }
}

function spawn ( command, args, input ) { // after https://github.com/panosoft/spawn-promise

    var child = child_process.spawn( command, args )

    // Capture errors
    var errors = {}
    child.on( 'error', error => errors.spawn = error )
    child.stdin.on( 'error', error => errors.stdin = error )
    child.stdout.on( 'error', error => errors.stdout = error )
    child.stderr.setEncoding( 'utf8' )
    child.stderr.on( 'error', error => errors.stderr = error )
    child.stderr.on( 'data', data => {
        if ( !errors.process ) errors.process = ''
        errors.process += data
    })

    // Capture output
    var buffers = []
    child.stdout.on( 'data', data => buffers.push( data ))

    // input
    child.stdin.end( input )

    // Run
    return new Promise( resolve => {
        child.on( 'close', ( code, signal ) => resolve( code ))
        child.stdin.end( input )
    })
    .then( exitCode => {
        if ( exitCode !== 0 )
            return Promise.reject( new Error( `Command failed: ${exitCode}` ))

        if ( Object.keys( errors ).length !== 0 )
            return Promise.reject( new Error( JSON.stringify( errors )))

        return Buffer.concat( buffers )
    })
}

//
// Writer
//
class Writer {
    constructor ( path ) {
        this.position = 0

        this.stream = fs.createWriteStream( path, { highWaterMark: 1024*1024*10 })
        this.stream.once( 'open', fd => { })
        this.stream.on( 'error', err => {
            console.trace( 'Writer error', this.stream.path, err )
            process.exit( 1 )
        })

        this.queue = genericPool.createPool(
            {
                create () { return Promise.resolve( Symbol( )) },
                destroy ( resource ) { return Promise.resolve() },
            },
            {}
        )
    }

    write ( data ) {
        return this.queue.acquire()
        .then( token => {
            const result =  this.position
            this.position += data.length

            const saturated = ! this.stream.write( data )
            if ( saturated ) {
                this.stream.once( 'drain', () => this.queue.release( client ))
            } else {
                this.queue.release( token )
            }
            return result
        })
    }

    close () {
        return this.queue.drain()
        .then( () => new Promise( resolve => {
            this.queue.clear()
            this.stream.once( 'close', () => {
                log( this.stream.path, 'closed', this.position, this.stream.bytesWritten )
                resolve( this.position )
            })
            log( 'closing', this.stream.path )
            this.stream.end()
        }))
    }
}

//
// Cluster
//

// ClusterSizeThreshold = 8 * 1024 * 1024
var ClusterSizeThreshold = 4 * 1024 * 1024
// ClusterSizeThreshold = 2 * 1024 * 1024
// ClusterSizeThreshold = 2 * 1024

class Cluster {
    constructor ( ordinal, compressible ) {
        this.ordinal = ordinal
        this.compressible = compressible
        this.blobs = []
        this.size = 0
    }

    append ( data ) {
        var ordinal = this.ordinal
        var blobNum = this.blobs.length
        if ( blobNum != 0 && this.size + data.length > ClusterSizeThreshold )
            return false

        this.blobs.push( data )
        this.size += data.length
        return blobNum
    }

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

    save () {
        //~ log( 'Cluster.prototype.save', this.compressible, this.blobs )

        var nBlobs = this.blobs.length
        if ( nBlobs == 0 )
            return Promise.resolve()

        // generate blob offsets
        var offsets = Buffer.allocUnsafe(( nBlobs + 1 ) * 4 )
        var blobOffset = offsets.length
        for ( var i=0; i < nBlobs; i++ ) {
            offsets.writeUIntLE( blobOffset, i * 4, 4 )
            blobOffset += this.blobs[ i ].length
        }
        //~ log( this.ordinal,'generate blob offsets', nBlobs, offsets.length, i, blobOffset )
        offsets.writeUIntLE( blobOffset, i * 4, 4 ) // final offset

        // join offsets and article data
        this.blobs.unshift( offsets )
        var data = Buffer.concat( this.blobs )
        var rawSize = data.length

        var compression = this.compressible ? 4 : 0
        var ordinal = this.ordinal

        return Promise.coroutine( function* () {
            if ( compression ) {
                // https://tukaani.org/lzma/benchmarks.html
                data = yield lzma.compress( data, 7 ) // 3 | lzma.PRESET_EXTREME )
                log( 'Cluster lzma compressed' )
            }

            log( 'Cluster write', ordinal, compression )
            const offset = yield out.write( Buffer.concat([ Buffer.from([ compression ]), data ]))

            log( 'Cluster saved', ordinal, offset )
            return zIndex.run(
                'INSERT INTO clusters (ordinal, offset) VALUES (?,?)',
                [
                    ordinal,
                    offset
                ]
            )
        }) ()
    }
}

//
// ClusterWriter
//
var ClusterWriter = {
    count: 2,
    true: new Cluster( 0, true ), // compressible cluster
    false: new Cluster( 1, false ), // uncompressible cluster
    pool: genericPool.createPool(
        {
            create () { return Promise.resolve( Symbol() ) },
            destroy ( resource ) { return Promise.resolve() },
        },
        { max: 8, }
    ),

    append: function ( mimeType, data, id /* for debugging */ ) {
        //~ log( 'ClusterWriter.append', arguments )

        var compressible = ClusterWriter.isCompressible( mimeType, data, id )
        var cluster = ClusterWriter[ compressible ]
        var clusterNum = cluster.ordinal
        var blobNum = cluster.append( data )

        if ( blobNum === false ) { // store to a new cluster
            ClusterWriter[ compressible ] = new Cluster( ClusterWriter.count ++, compressible )
            const ready = ClusterWriter.pool.acquire()

            ready.then( token => cluster.save()
                .then( () => ClusterWriter.pool.release( token ))
            )

            return ready
            .then( () => ClusterWriter.append( mimeType, data, id ))
        }

        log( 'ClusterWriter.append', compressible, clusterNum, blobNum, data.length, id )
        return Promise.resolve([ clusterNum, blobNum ])
    },

    isCompressible: function ( mimeType, data, id ) {
        //~ log( 'isCompressible', this )
        if ( data == null || data.length == 0 )
            return false
        if ( !mimeType ) {
            console.trace( 'Article.prototype.isCompressible mimeType', mimeType, id )
            process.exit( 1 )
        }
        if ( mimeType == 'image/svg+xml' || mimeType.split( '/' )[ 0 ] == 'text' )
            return true
        return !! ( mimeDb[ mimeType ] && mimeDb[ mimeType ].compressible )
    },

    // The cluster pointer list is a list of 8 byte offsets which point to all data clusters in a ZIM file.
    // Field Name  Type    Offset  Length  Description
    // <1st Cluster>   integer     0   8   pointer to the <1st Cluster>
    // <1st Cluster>   integer     8   8   pointer to the <2nd Cluster>
    // <nth Cluster>   integer     (n-1)*8     8   pointer to the <nth Cluster>
    // ...     integer     ...     8   ...

    storeIndex: function () {
        return saveIndex (
            `
            SELECT
                offset
            FROM clusters
            ORDER BY ordinal
            ;
            `,
            8, 'offset', ClusterWriter.count, 'storeClusterIndex'
        )
        .then( offset => header.clusterPtrPos = offset )
    },

    finish: function () {
        //~ log( 'ClusterWriter.finish', ClusterWriter )

        return ClusterWriter[ true ].save() // save last compressible cluster
        .then( () => ClusterWriter[ false ].save()) // save last uncompressible cluster
        .then( () => ClusterWriter.pool.drain())
        .then( () => ClusterWriter.pool.clear())
        .then( () => ClusterWriter.storeIndex())
    },
}

//
// Article
//
class Article {
    constructor ( path, mimeType, nameSpace, title, data ) {
        if ( ! path )
            return
        this.mimeType = mimeType
        this.nameSpace = nameSpace
        this.url = path
        this.title = title || ''
        this.data = data
        this.ordinal = null
        this.dirEntry = null
        this.revision = 0
        this.articleId = ++ articleId
    }

    nsUrl () {
        return this.nameSpace + this.url
    }

    nsTitle () {
        return this.nameSpace + ( this.title || this.url )
    }

    storeData ( data ) {
        if ( data == null )
            return Promise.resolve()
        if ( !( data instanceof Buffer )) {
            data = Buffer.from( data )
        }
        return ClusterWriter.append( this.mimeType, data, this.url )
    }

    load () {
        return Promise.resolve( this.data )
    }

    process () {
        log( 'Article process', this.url )

        return this.load()
        .then( data => this.storeData( data ))
        .then( ([ clusterIdx, blobIdx ]) => this.storeDirEntry( clusterIdx, blobIdx ))
        .then( () => this.indexArticle())
    }

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

    storeDirEntry ( clusterIdx, blobIdx ) {
        // this also serves redirect dirEntry, which is shorter in 4 bytes
        var needsSave = clusterIdx != null

        if ( ! needsSave )
            return Promise.resolve()

        var shortEntry = blobIdx == null

        var buf = Buffer.allocUnsafe( shortEntry ? 12 : 16 )
        var mimeIndex = mimeTypeIndex( this.mimeType )

        log( 'storeDirEntry', clusterIdx, blobIdx, mimeIndex, this )

        buf.writeUIntLE( mimeIndex,     0, 2 )
        buf.writeUIntLE( 0,             2, 1 ) // parameters length
        buf.write( this.nameSpace,      3, 1 )
        buf.writeUIntLE( this.revision, 4, 4 ) // revision
        buf.writeUIntLE( clusterIdx,    8, 4 ) // or redirect target article index
        if ( ! shortEntry )
            buf.writeUIntLE( blobIdx,  12, 4 )

        var urlBuf = Buffer.from( this.url + '\0' )
        var titleBuf = Buffer.from( this.title + '\0' )

        return out.write( Buffer.concat([ buf, urlBuf, titleBuf ]))
        .then( offset => {
            log( 'storeDirEntry done', offset, buf.length, this.url )
            return this.dirEntry = offset
        })
        .then( offset => this.indexDirEntry( offset ))
    }

    indexDirEntry ( offset ) {
        log( 'indexDirEntry', this.articleId, offset, this.url )
        return zIndex.run(
            'INSERT INTO dirEntries (articleId, offset) VALUES (?,?)',
            [
                this.articleId,
                offset,
            ]
        )
    }

    indexArticle () {
        if ( !this.url ) {
            console.trace( 'Article no url', this )
            process.exit( 1 )
        }
        articleCount++

        return zIndex.run(
            'INSERT INTO articles (articleId, nsUrl, nsTitle) VALUES (?,?,?)',
            [
                this.articleId,
                this.nsUrl(),
                this.nsTitle()
            ]
        )
    }
}

//
// class Linktarget
//
class Linktarget extends Article {
    constructor ( path, nameSpace, title ) {
        super( path, LINKTARGET_MIME, nameSpace, title )
        log( 'Linktarget', nameSpace, path, this )
    }

    process () {
        log( 'Linktarget.prototype.process', this.url )
        return this.storeDirEntry()
        .then( () => this.indexArticle())
    }

    storeDirEntry () {
        return super.storeDirEntry( true, true )
    }
}

//
// class DeletedEntry
//
class DeletedEntry extends Linktarget {
    constructor ( path, nameSpace, title ) {
        super( path, DELETEDENTRY_MIME, nameSpace, title )
        log( 'DeletedEntry', nameSpace, path, this )
    }
}

//
// class Redirect
//
class Redirect extends Article {
    constructor ( path, nameSpace, title, redirect, redirectNameSpace ) {
        super( path, REDIRECT_MIME, nameSpace, title )

        this.redirect = ( redirectNameSpace || nameSpace ) + redirect
        log( 'Redirect', nameSpace, path, this.redirect, this )
        redirectCount ++
    }

    process () {
        return this.indexArticle()
        .then( () => this.toRedirectIndex() )
    }

    toRedirectIndex () {
        return zIndex.run(
            'INSERT INTO redirects (articleId, redirect) VALUES (?,?)',
            [
                this.articleId,
                this.redirect
            ]
        )
    }
}

//
// class ResolvedRedirect
//
class ResolvedRedirect extends Article {
    constructor ( articleId, nameSpace, url, title, target ) {
        super( url, REDIRECT_MIME, nameSpace, title )
        this.target = target
        this.articleId = articleId
    }

    process () {
        log( 'ResolvedRedirect.prototype.process', this.url )
        resolvedRedirectCount ++

        return this.storeDirEntry()
    }

    storeDirEntry () {
        // Redirect Entry
        // Field Name      Type    Offset  Length  Description
        // mimetype        integer 0       2       0xffff for redirect
        // parameter len   byte    2       1       (not used) length of extra paramters
        // namespace       char    3       1       defines which namespace this directory entry belongs to
        // revision        integer 4       4       (optional) identifies a revision of the contents of this directory entry, needed to identify updates or revisions in the original history
        // redirect index  integer 8       4       pointer to the directory entry of the redirect target
        // url             string  12      zero terminated     string with the URL as refered in the URL pointer list
        // title           string  n/a     zero terminated     string with an title as refered in the Title pointer list or empty; in case it is empty, the URL is used as title
        // parameter       data    see parameter len   (not used) extra parameters

        // redirect dirEntry shorter on one 4 byte field
        return super.storeDirEntry( this.target, null )
    }
}

//
// class File
//
class File extends Article {

    constructor ( path, realPath ) {
        var url = path
        var mimeType = getMimeType( path, realPath )
        var nameSpace
        if ( zimFormated ) {
            nameSpace = path[ 0 ]
            url = path.slice( 2 )
            path = realPath || path
        }
        super( url, mimeType, nameSpace )
        this.path = path
        //~ log( this )
    }

    alterLinks ( dom ) {
        var base = '/' + this.url
        var nsBase = '/' + this.nameSpace + base
        var baseSplit = nsBase.split( '/' )
        var baseDepth = baseSplit.length - 1
        var changes = 0

        function toRelativeLink ( elem, attr ) {
            try {
                var link = url.parse( elem.attribs[ attr ], true, true )
            } catch ( err ) {
                console.warn( 'alterLinks', err.message, elem.attribs[ attr ], 'at', base )
                return
            }
            var path = link.pathname
            if ( link.protocol || link.host || ! path )
                return
            var nameSpace = getNameSpace( getMimeType( path ))
            if ( ! nameSpace )
                return

            // convert to relative path
            var absPath = '/' + nameSpace + url.resolve( base, path )
            var to = absPath.split( '/' )
            var i = 0
            for ( ; baseSplit[ i ] === to[ 0 ] && i < baseDepth; i++ ) {
                to.shift()
            }
            for ( ; i < baseDepth; i++ ) {
                to.unshift( '..' )
            }
            var relPath = to.join( '/' )
            log( 'alterLinks', nsBase, decodeURI( path ), decodeURI( absPath ), decodeURI( relPath ))

            link.pathname = relPath
            elem.attribs[ attr ] = url.format( link )

            changes ++
            return
        }

        ['src', 'href'].map( attr => {
            dom( '['+attr+']' ).each( (index, elem) => toRelativeLink ( elem, attr ))
        })

        log( 'alterLinks', changes )

        return changes > 0
    }

    isRedirect ( dom ) {
        var content = dom( 'meta[http-equiv="refresh"]' ).attr( 'content' )
        if ( content == null )
            return null
        var splited = content.split( ';' )
        if ( ! splited[ 1 ] )
            return null

        log( 'File.prototype.getRedirect', this.url, splited)
        var link = url.parse( splited[ 1 ].split( '=', 2 )[ 1 ], true, true )
        if ( link.protocol || link.host || ! link.pathname || link.pathname[ 0 ] =='/' )
            return null

        var target = decodeURIComponent( link.pathname )
        return target
    }

    processHtml ( data ) {
        var dom = cheerio.load( data.toString())
        if ( dom ) {
            var title = dom( 'title' ).text()
            this.title = this.title || title
            var redirect = this.isRedirect ( dom )
            if ( redirect ) { // convert to redirect
                this.data = null
                var redirect = new Redirect( this.path, this.nameSpace, this.title, redirect )
                return redirect.process()
            }
            if ( ! zimFormated && this.alterLinks( dom ))
                data = Buffer.from( dom.html())
        }
        return data
    }

    processJpeg ( data ) {
        return spawn( mozjpeg, [ '-quality', argv.jpegquality ], data )
    }

    processImage ( data ) {
        return Promise.coroutine( function* () {
            const image = sharp( data )
            let toJpeg = true

            const metadata = yield image.metadata()

            if ( metadata.hasAlpha && metadata.channels == 1 ) {
                log( 'metadata.channels == 1', this.url )
            }

            if ( metadata.hasAlpha && metadata.channels > 1 && this.data.length > 20000 ) {
                const alpha = yield image
                    .clone()
                    .extractChannel( metadata.channels - 1 )
                    .raw()
                    .toBuffer()

                const opaqueAlpha = Buffer.alloc( alpha.length, 0xff )
                const isOpaque = alpha.equals( opaqueAlpha )

                isOpaque && log( 'isOpaque', this.url )

                toJpeg = isOpaque
            }

            if ( toJpeg ) {
                image.jpeg({
                    force: true,
                    quality: argv.jpegquality,
                    progressive: this.data.length < 20000 ? false : true,
                })
                this.mimeType = 'image/jpeg'
            }

            return image.toBuffer()
        }).call( this )
    }

    load () {
        return Promise.coroutine( function* () {
            let data = yield fs.readFile( fullPath( this.path ))
            if ( argv.inflateHtml && this.mimeType == 'text/html' )
                data = yield zlib.inflate( data ) // inflateData

            if ( ! this.mimeType ) { // mimeFromData
                this.mimeType = yield new Promise( resolve =>
                    mimeMagic.detect( data, ( err, mimeType ) => {
                        log( 'File  mimeMagic.detect', err, mimeType, this.url )
                        resolve( err ? Promise.reject( err ) : mimeType )
                    })
                )
            }
            this.nameSpace = this.nameSpace || getNameSpace( this.mimeType )
            switch ( this.mimeType ) {
                case 'text/html':
                    return this.processHtml( data )
                //~ case 'image/gif':
                case 'image/png':
                    //~ if ( argv.optimg )
                        //~ return this.processImage( data )
                case 'image/jpeg':
                    if ( argv.optimg )
                        return this.processImage( data )
                        //~ return this.processJpeg()
                default:
                    return data
            }
        }).call( this )
    }
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

function loadMetadata () {
    var metadata = zimFormated.metadata || [
        [ 'Title', argv.title ],
        [ 'Creator', argv.creator ],
        [ 'Publisher', argv.publisher ],
        [ 'Date', new Date().toISOString().split( 'T' )[ 0 ]],
        [ 'Description', argv.description ],
        [ 'Language', argv.language ],
        [ 'logo', argv.favicon ],
        [ 'mainpage', argv.welcome ],
    ]

    const done = []
    metadata.forEach( item => {
        var article
        var name = item[ 0 ]
        var val = item[ 1 ]
        switch ( name ) {
            case 'mainpage':
                article  = mainPage = new Redirect( 'mainpage', '-', null, val, 'A' )
                break
            case 'logo':
                article  = new Redirect( 'favicon', '-', null, val, 'I' )
                break
            default:
                article = new Article( name, 'text/plain', 'M', null, val )
        }
        done.push( article.process())
    })

    deadEndTarget = new Linktarget ( 'deadend', '-' )
    done.push( deadEndTarget.process())

    return Promise.all( done )
}

function createAuxIndex() {
    var dbName = ''
    if ( argv.verbose ) {
        var parsed = osPath.parse( outPath )
        dbName = osPath.join( parsed.dir, parsed.base + '.db' )
    }
    return fs.unlink( dbName )
    .catch( () => null )
    .then( () => sqlite.open( dbName ))
    .then( db => {
        zIndex = db
        return zIndex.exec(
                'PRAGMA synchronous = OFF;' +
                'PRAGMA journal_mode = OFF;' +
                //~ 'PRAGMA journal_mode = WAL;' +
                'CREATE TABLE articles (' +
                    'articleId INTEGER PRIMARY KEY,' +
                    //~ 'ordinal INTEGER,' +
                    //~ 'dirEntry INTEGER,' +
                    'nsUrl TEXT,' +
                    'nsTitle TEXT' +
                    ');' +
                'CREATE TABLE redirects (' +
                    'articleId INTEGER PRIMARY KEY,' +
                    'redirect TEXT ' +
                    ');' +
                'CREATE TABLE dirEntries (' +
                    'articleId INTEGER PRIMARY KEY,' +
                    'offset INTEGER' +
                    ');' +
                'CREATE TABLE clusters (' +
                    'ordinal INTEGER PRIMARY KEY,' +
                    'offset INTEGER ' +
                    ');'
            )
        }
    )
}

function sortArticles () {
    return zIndex.exec(`
        CREATE INDEX articleNsUrl ON articles (nsUrl);

        CREATE TABLE urlSorted AS
            SELECT
                articleId,
                nsUrl
            FROM articles
            ORDER BY nsUrl;

        CREATE INDEX urlSortedArticleId ON urlSorted (articleId);

        CREATE INDEX articleNsTitle ON articles (nsTitle);
        `
    )
}

function loadRedirects () {
    var redirectsFile
    if ( zimFormated )
        redirectsFile = zimFormated.redirects
    else if ( argv.redirects )
        redirectsFile = expandHomeDir( argv.redirects )
    else
        return Promise.resolve()

    return Promise.coroutine( function* () {
        var inp = fs.createReadStream( redirectsFile )
        var finished = false

        var parser = csvParse(
            {
                delimiter: '\t',
                columns:[ 'ns', 'path', 'title', 'target', 'targetFragment' ]
            }
        )
        parser.on( 'error', function ( err ) {
            console.error( 'loadRedirects ' + err.message )
            throw err
        })
        parser.on( 'end', function () {
            log( 'loadRedirects end' )
            finished = true
            parser.emit( 'readable' )
        })

        log( 'loadRedirects start' )
        inp.pipe( parser )

        function getRow () {
            return new Promise( resolve => {
                var row = parser.read()
                if ( row || finished ) {
                    resolve( row )
                } else {
                    parser.once( 'readable', () => resolve( getRow()))
                }
            })
        }

        while ( true ) {
            const row = yield getRow()
            log( 'loadRedirects', row )
            if ( row ) {
                yield new Redirect( row.path, row.ns, row.title, row.target )
                .process()
            }
            if ( finished ) {
                return
            }
        }
    }) ()

}

function resolveRedirects () {
    return Promise.coroutine( function* () {
        var stmt = yield zIndex.prepare( `
            SELECT
                src.articleId AS articleId,
                src.nsUrl AS nsUrl,
                src.nsTitle AS nsTitle,
                redirect,
                targetUrl,
                targetIdx
            FROM (
                SELECT
                    *,
                    u.rowid - 1 AS  targetIdx
                FROM (
                    SELECT
                        redirects.articleId,
                        redirect,
                        d.nsUrl AS targetUrl,
                        COALESCE ( d.articleId, (
                            SELECT articleId FROM articles WHERE nsUrl = "${deadEndTarget.nsUrl()}")
                        ) AS targetId
                        -- d.articleId AS targetId
                    FROM redirects
                    LEFT OUTER JOIN articles AS d
                    ON redirect = targetUrl
                ) AS r
                LEFT OUTER JOIN urlSorted AS u
                ON r.targetId = u.articleId
            ) AS dst
            JOIN articles AS src
            USING (articleId)
            -- WHERE targetIdx IS NULL
            ;`)
        while ( true ) {
            const row = yield stmt.get()
            if ( ! row ) {
                return
            }
            var nameSpace = row.nsUrl[ 0 ]
            var url = row.nsUrl.substr( 1 )
            var title = ( row.nsTitle == row.nsUrl ) ? '' : row.nsTitle.substr( 1 )
            if ( url == 'mainpage' )
                mainPage.target = row.targetIdx

            yield new ResolvedRedirect ( row.articleId, nameSpace, url, title, row.targetIdx )
                .process()
        }
    }) ()
}

function saveIndex ( query, byteLength, rowField, count, logInfo ) {
    logInfo = logInfo || 'saveIndex'
    log( logInfo, 'start', count )

    return Promise.coroutine( function* () {
        var startOffset
        var i = 0
        var stmt = yield zIndex.prepare( query )
        while ( true ) {
            const row = yield stmt.get()
            if ( ! row )
                break
            log( logInfo, i, row )
            i++
            var buf = Buffer.allocUnsafe( byteLength )
            buf.writeUIntLE( row[ rowField ], 0, byteLength )

            var offset = yield out.write( buf )
            if ( ! startOffset )
                startOffset = offset
        }
        log( logInfo, 'done', i, count, startOffset )
        return Promise.resolve( startOffset )
    }) ()
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

function storeUrlIndex () {
    return saveIndex (`
        SELECT
            urlSorted.rowid,
            articleId,
            nsUrl,
            offset
        FROM urlSorted
        LEFT OUTER JOIN dirEntries
        USING (articleId)
        ORDER BY urlSorted.rowid
        ;`,
        8, 'offset', articleCount, 'storeUrlIndex'
    )
    .then( offset => header.urlPtrPos = offset )
}

// Title Pointer List (titlePtrPos)

// The title pointer list is a list of article indices ordered by title. The title pointer list actually points to entries in the URL pointer list. Note that the title pointers are only 4 bytes. They are not offsets in the file but article numbers. To get the offset of an article from the title pointer list, you have to look it up in the URL pointer list.
// Field Name      Type        Offset  Length  Description
// <1st Title>     integer     0       4       pointer to the URL pointer of <1st Title>
// <2nd Title>     integer     4       4       pointer to the URL pointer of <2nd Title>
// <nth Title>     integer     (n-1)*4 4       pointer to the URL pointer of <nth Title>
// ...             integer     ...     4       ...

function storeTitleIndex () {
    return saveIndex (
        'SELECT ' +
            'nsTitle, ' +
            'urlSorted.rowid - 1 AS articleNumber ' +
        'FROM urlSorted ' +
        'JOIN articles ' +
        'USING (articleId) ' +
        'ORDER BY nsTitle ' +
        ';',
        4, 'articleNumber', articleCount, 'storeTitleIndex'
    )
    .then( offset => header.titlePtrPos = offset )
}

const fileLoader = {
    dirs: [''],
    start: function () {
        log( 'fileLoader start' )
        return fileLoader.scanDirectories()
        .then( () => log( 'fileLoader finished !!!!!!!!!' ))
    },
    scanDirectories: function ( path ) {
        return Promise.coroutine( function* () {
            for ( let path; ( path = fileLoader.dirs.shift()) != null; ) {
                log( 'scanDirectory', path )

                yield Promise.map(
                    fs.readdir( fullPath( path )),
                    fname => fileLoader.parseDirEntry( osPath.join( path, fname ), null ),
                    { concurrency: 4 }
                )
            }
        }) ()
    },
    parseDirEntry: function ( path, realPath ) {
        if ( path == 'metadata.csv' || path == 'redirects.csv' )
            return Promise.resolve()

        return fs.lstat( realPath || fullPath( path ) )
        .then( stats => {
            if ( stats.isDirectory())
                return fileLoader.dirs.push( path )
            if ( stats.isSymbolicLink())
                return fs.realpath( fullPath( path ))
                .then( realPath => fileLoader.parseDirEntry( path, realPath ))
            if ( stats.isFile()) {
                return new File( path, realPath ).process()
            }
            return Promise.reject( new Error( 'Invalid dir entry ' + absPath ))
        })
    },
}

// MIME Type List (mimeListPos)

// The MIME type list always follows directly after the header, so the mimeListPos also defines the end and size of the ZIM file header.

// The MIME types in this list are zero terminated strings. An empty string marks the end of the MIME type list.
// Field Name          Type    Offset  Length  Description
// <1st MIME Type>     string  0       zero terminated     declaration of the <1st MIME Type>
// <2nd MIME Type>     string  n/a     zero terminated     declaration of the <2nd MIME Type>
// ...                 string  ...     zero terminated     ...
// <last entry / end>  string  n/a     zero terminated     empty string - end of MIME type list

function getMimeTypes () {
    var buf = Buffer.from( mimeTypeList.join( '\0' ) + '\0' )
    log( 'MimeTypes', mimeTypeList.length, buf.length )

    if ( buf.length > maxMimeLength ) {
        console.error( 'Error: mime type list length >', maxMimeLength )
        process.exit( 1 )
    }
    return buf
}

function getHeader () {
    header.articleCount = articleCount
    header.clusterCount = ClusterWriter.count
    header.mainPage = mainPage.target || header.mainPage

    //~ log( 'Header', 'articleCount', articleCount, 'clusterCount', ClusterWriter.count, 'mainPage', mainPage )
    log( 'Header', header )

    var buf = Buffer.alloc( headerLength )
    buf.writeUIntLE( header.magicNumber,     0, 4 )
    buf.writeUIntLE( header.version,         4, 4 )

    uuid.v4( null, buf,                      8 )

    buf.writeUIntLE( header.articleCount,    24, 4 )
    buf.writeUIntLE( header.clusterCount,    28, 4 )

    buf.writeUIntLE( header.urlPtrPos,       32, 8 )
    buf.writeUIntLE( header.titlePtrPos,     40, 8 )
    buf.writeUIntLE( header.clusterPtrPos,   48, 8 )
    buf.writeUIntLE( header.mimeListPos,     56, 8 )

    buf.writeUIntLE( header.mainPage,        64, 4 )
    buf.writeUIntLE( header.layoutPage,      68, 4 )

    buf.writeUIntLE( header.checksumPos,     72, 8 )

    return buf
}

function stroreHeader() {
    var buf = Buffer.concat([ getHeader(), getMimeTypes() ])
    var fd = fs.openSync( outPath, 'r+' )
    fs.writeSync( fd, buf, 0, buf.length, 0 )
    fs.closeSync( fd )
    return Promise.resolve()
}

function calculateFileHash () {
    var outHash
    var hash = crypto.createHash( 'md5' )
    var stream = fs.createReadStream( outPath )
    var resolve

    stream.on( 'data', data => hash.update( data ))
    stream.on( 'end', () => {
        outHash = hash.digest()
        log( 'outHash', outHash )
        fs.appendFileSync( outPath, outHash )
        resolve()
    })

    return new Promise( r => resolve = r )
}


function initialise () {
    var stat = fs.statSync( srcPath ) // check source
    if ( ! stat.isDirectory() ) {
        return Promise.reject( new Error( srcPath + ' is not a directory' ))
    }
    var mtdPath = osPath.join( srcPath, 'metadata.csv' )
    var mtdTxt = null
    try {
        mtdTxt = fs.readFileSync( mtdPath, 'utf8' )
    } catch ( err ) {
    }
    if ( mtdTxt ) {
        var mtdArr = csvParseSync( mtdTxt, { delimiter: '\t' })
        zimFormated = {
            metadata: mtdArr,
            redirects: osPath.join( srcPath, 'redirects.csv' )
        }
    }

    out = new Writer( outPath ); // create output file
    log( 'reserving space for header and mime type list' )
    out.write( Buffer.alloc( headerLength + maxMimeLength ))

    return createAuxIndex()
}

//~ function loadArticles () {
    //~ return loadMetadata()
    //~ .then( () => Promise.all( fileLoader.start(), loadRedirects() ))
//~ }
function loadArticles () {
    return Promise.coroutine( function* () {
        yield loadMetadata()
        yield fileLoader.start()
        yield loadRedirects()
    }) ()
}
function postProcess () {
    return Promise.coroutine( function* () {
        yield ClusterWriter.finish()
        yield sortArticles()
        yield resolveRedirects()
        yield storeUrlIndex()
        yield storeTitleIndex()
    }) ()
}

function finalise () {
    return Promise.coroutine( function* () {
        header.checksumPos = yield out.close() // close the output stream
        yield stroreHeader()
        yield calculateFileHash()
    }) ()
}

function core () {
    return Promise.coroutine( function* () {
        yield initialise()
        yield loadArticles()
        yield postProcess()
        yield finalise()
    }) ()
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
        //~ .boolean( 'optimg' )
        .options({
            'w': {alias: 'welcome', default: 'index.htm'},
            'f': {alias: 'favicon', default: 'favicon.png'},
            'l': {alias: 'language', default: 'eng'},
            't': {alias: 'title', default: ''},
            'd': {alias: 'description', default: ''},
            'c': {alias: 'creator', default: ''},
            'p': {alias: 'publisher', default: ''},
            'v': {alias: 'verbose', type: 'boolean', default: false},
            'm': {alias: 'minChunkSize', type: 'number', default: ClusterSizeThreshold / 1024},
            'x': {alias: 'inflateHtml', type: 'boolean', default: false},
            'u': {alias: 'uniqueNamespace', type: 'boolean', default: false},
            //~ 'r': {alias: 'redirects', default: 'redirects.csv'},
            'r': {alias: 'redirects', default: ''},
            //~ 'i': {alias: 'withFullTextIndex', type:'boolean', default: false},
            'h': {alias: 'help'},
            'optimg': {type: 'boolean', default: false},
            'jpegquality': {type: 'number', default: 60},
        })
        .help( 'help' )
        //~ .strict()
        .argv

    log( argv )

    var pargs = argv._

    while ( pargs[ 0 ] == '' ) // if mwoffliner prepends with empty extra parameter(s)
        pargs.shift()

    srcPath = expandHomeDir( pargs[ 0 ])
    if ( argv._[ 1 ])
        outPath = expandHomeDir( pargs[ 1 ])
    else {
        var parsed = osPath.parse( srcPath )
        outPath = parsed.base + '.zim'
    }

    if ( argv.minChunkSize ) {
        ClusterSizeThreshold = argv.minChunkSize * 1024
    }

    //~ mainPage = {
        //~ title: argv.welcome
    //~ }

    core ()
    .then( () => log( 'Done...' ))
}

main ()
;
