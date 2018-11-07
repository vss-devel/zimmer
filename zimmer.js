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

const packageInfo = require('./package.json');
const Promise = require( 'bluebird' )
const os = require( 'os' )
const osPath = require( 'path' )
const process = require( 'process' )
const url = require( 'url' )
const crypto = require( "crypto" )

const argv = require('commander')
const fs = require( 'fs-extra' )
const expandHomeDir = require( 'expand-home-dir' )
const lzma = require( 'lzma-native' )
const cheerio = require('cheerio')

const uuid = require( "uuid" )
const csvParse = require( 'csv-parse' )
const csvParseSync = require( 'csv-parse/lib/sync' )
const zlib = require( 'mz/zlib' )
const sqlite = require( 'sqlite' )
const sharp = require( 'sharp' )

const genericPool = require( 'generic-pool' )
const mozjpeg = require( 'mozjpeg' )
const childProcess = require('child_process')
const isAnimated = require('animated-gif-detector')

const mimeDb = require( 'mime-db' )
const mimeTypes = require( 'mime-types' )
const mmmagic = require( 'mmmagic' )
const mimeMagic = new mmmagic.Magic( mmmagic.MAGIC_MIME_TYPE )

const cpuCount = os.cpus().length

var srcPath
var outPath
var out // output file writer

var indexerDb
var dirQueue

var articleCount = 0

let preProcessed = false

var mainPage = {}

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

function mimeFromPath ( path ) {
    var mType = mimeTypes.lookup( path )
    if ( mType == null ) {
        console.error( 'No mime type found', path )
    }
    return mType
}

function mimeFromData ( data ) {
    return new Promise(( resolve, reject ) =>
        mimeMagic.detect( data, ( error, mimeType ) => {
            if ( error )
                return reject( error )
            return resolve( mimeType )
        })
    )
}

var REDIRECT_MIME = '@REDIRECT@'
var LINKTARGET_MIME = '@LINKTARGET@'
var DELETEDENTRY_MIME = '@DELETEDENTRY@'

var mimeTypeList = []

var maxMimeLength = 512

function mimeFromIndex ( idx ) {
    return mimeTypeList[ idx ]
}

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
    let idx = mimeTypeList.indexOf( mimeType )
    if ( idx == -1 ) {
        idx = mimeTypeList.length
        mimeTypeList.push( mimeType )
    }
    return idx
}

function getNameSpace ( mimeType ) {
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

function writeUIntLE( buf, value, offset, byteLength ) {
    offset = offset || 0
    if ( typeof value == 'string' ) {
        byteLength = buf.write( value, offset )
        return offset + byteLength
    }
    if ( byteLength == 8 ) {
        var low = value & 0xffffffff
        var high = ( value - low ) / 0x100000000 - ( low < 0 ? 1 : 0 )
        buf.writeInt32LE( low, offset )
        buf.writeUInt32LE( high, offset + 4 )
        return offset + byteLength
    } else {
        return buf.writeUIntLE( value, offset, byteLength )
    }
}

function spawn ( command, args, input ) { // after https://github.com/panosoft/spawn-promise
    var child = childProcess.spawn( command, args )

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
    child.stdin.write( input )
    child.stdin.end()

    // Run
    return new Promise( resolve => {
        child.on( 'close', ( code, signal ) => resolve( code ))
        child.stdin.end( input )
    })
    .then( exitCode => {
        if ( exitCode !== 0 )
            return Promise.reject( new Error( `Command failed: ${ exitCode } ${ JSON.stringify( errors ) }` ))

        //~ if ( Object.keys( errors ).length !== 0 )
            //~ return Promise.reject( new Error( JSON.stringify( errors )))

        return Buffer.concat( buffers )
    })
}

function cvsReader ( path, options ) {
    let finished = false
    const inp = fs.createReadStream( path )
    const parser = csvParse( options )

    parser.on( 'error', function ( err ) {
        console.error( 'cvsReader ' + err.message )
        throw err
    })
    parser.on( 'end', function () {
        log( 'cvsReader end', path )
        finished = true
        parser.emit( 'readable' )
    })

    log( 'cvsReader start', path )
    inp.pipe( parser )

    function getRow () {
        return new Promise( resolve => {
            const row = parser.read()
            if ( row || finished ) {
                resolve( row )
            } else {
                parser.once( 'readable', () => resolve( getRow()))
            }
        })
    }

    return getRow
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
                create () { return Promise.resolve( Symbol() ) },
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
                this.stream.once( 'drain', () => this.queue.release( token ))
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
    constructor ( id, compressible ) {
        this.id = id
        this.compressible = compressible
        this.blobs = []
        this.size = 0
    }

    append ( data ) {
        var id = this.id
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
        //~ log( this.id,'generate blob offsets', nBlobs, offsets.length, i, blobOffset )
        offsets.writeUIntLE( blobOffset, i * 4, 4 ) // final offset

        // join offsets and article data
        this.blobs.unshift( offsets )
        var data = Buffer.concat( this.blobs )
        var rawSize = data.length

        var compression = this.compressible ? 4 : 0
        var id = this.id

        return Promise.coroutine( function* () {
            if ( compression ) {
                // https://tukaani.org/lzma/benchmarks.html
                data = yield lzma.compress( data, 7 ) // 3 | lzma.PRESET_EXTREME )
                log( 'Cluster lzma compressed' )
            }

            log( 'Cluster write', id, compression )
            const offset = yield out.write( Buffer.concat([ Buffer.from([ compression ]), data ]))

            log( 'Cluster saved', id, offset )
            return indexerDb.run(
                'INSERT INTO clusters (id, offset) VALUES (?,?)',
                [
                    id,
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
        var clusterNum = cluster.id
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
        if ( data == null || data.length == 0 )
            return false
        if ( !mimeType ) {
            console.error( 'isCompressible !mimeType', mimeType, data, id )
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
            ORDER BY id
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

class NoProcessingRequired extends Error {
    // For non-error promise rejection
}

//
// Item
//
class Item {
    constructor ( params ) {
        Object.assign( this, {
            nameSpace: null,
            path: null,
            title: '',
            mimeType: null,
            revision: 0,
            dirEntry: null,
            id: null,
        })
        Object.assign( this, params )
    }

    process () {
        //~ log( 'Item process', this.path )

        return this.storeDirEntry()
    }

    urlKey () {
        return this.nameSpace + this.path
    }

    titleKey () {
        return this.nameSpace + ( this.title || this.path )
    }

    mimeId () {
        return mimeTypeIndex( this.mimeType )
    }

    getId () {
        if ( ! this.id )
            this.id = this.saveItemIndex()
        return Promise.resolve( this.id )
    }

    saveItemIndex () {
        if ( ! this.path ) {
            console.trace( 'Item no url', this )
            process.exit( 1 )
        }

        const row = [
            this.urlKey(),
            this.titleKey(),
            this.revision,
            this.mimeId(),
        ]

        return indexerDb.run(
            'INSERT INTO articles ( urlKey, titleKey, revision, mimeId ) VALUES ( ?,?,?,? )',
            row
        )
        .then( res => {
            const id = res.stmt.lastID
            log( 'saveItemIndex', id, this )
            return id
        })
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

    storeDirEntry ( clusterIdx, blobIdx, redirectTarget ) {
        if ( clusterIdx == null ) {
            console.error( 'storeDirEntry error: clusterIdx == null', this )
            pprocess.exit( 1 )
            return Promise.resolve()
        }

        articleCount++

        const isRedirect = redirectTarget != null // redirect dirEntry is shorter in 4 bytes
        var buf = Buffer.allocUnsafe( isRedirect ? 12 : 16 )
        var mimeId = this.mimeId()

        log( 'storeDirEntry', mimeId, this )
        writeUIntLE( buf, mimeId,      0, 2 )
        writeUIntLE( buf, 0,           2, 1 ) // parameters length
        buf.write( this.nameSpace,     3, 1 )
        buf.writeIntLE( this.revision, 4, 4 )
        writeUIntLE( buf, clusterIdx || redirectTarget || 0, 8, 4 ) // or redirect target article index
        if ( ! isRedirect )
            writeUIntLE( buf, blobIdx,  12, 4 )

        var urlBuf = Buffer.from( this.path + '\0' )
        var titleBuf = Buffer.from( this.title + '\0' )

        return out.write( Buffer.concat([ buf, urlBuf, titleBuf ]))
        .then( offset => {
            log( 'storeDirEntry done', offset, buf.length, this.path )
            return this.dirEntry = offset
        })
        .then( offset => this.saveDirEntryIndex( offset ))
    }

    saveDirEntryIndex ( offset ) {
        return this.getId()
        .then( id => {
            log( 'saveDirEntryIndex', id, offset, this.path )
            return indexerDb.run(
                'INSERT INTO dirEntries (id, offset) VALUES (?,?)',
                [
                    id,
                    offset,
                ]
            )
            .catch( err => {
                console.error( 'saveDirEntryIndex error', err, this )
                process.exit( 1 )
            })
        })
    }
}

//
// class Linktarget
//
class Linktarget extends Item {
    constructor ( path, nameSpace, title ) {
        super({
            path,
            nameSpace,
            title,
            mimeType: LINKTARGET_MIME,
        })
        log( 'Linktarget', nameSpace, path, this )
    }

    storeDirEntry () {
        return super.storeDirEntry( 0, 0 )
    }
}

//
// class DeletedEntry
//
class DeletedEntry extends Linktarget {
    constructor ( path, nameSpace, title ) {
        super({
            path,
            nameSpace,
            title,
            mimeType: DELETEDENTRY_MIME,
        })
        log( 'DeletedEntry', nameSpace, path, this )
    }
}

//
// class TargetItem
//
class TargetItem extends Item {
    constructor ( params ) {
        params.fragment = params.fragment === undefined ? null : params.fragment;
        super( params )
    }
}

//
// class Redirect
//
class Redirect extends Item {
    constructor ( params ) {
        // params: path, nameSpace, title, to, revision
        // to: path, nameSpace, fragment
        let to = params.to
        delete params.to

        params.mimeType = REDIRECT_MIME

        super( params )

        if ( typeof to == 'string' )
            to = { path: to }
        to.nameSpace = to.nameSpace || this.nameSpace

        this.target = new TargetItem( to )
        log( 'Redirect', this.nameSpace, this.path, to, this )
    }

    process () {
        return this.saveRedirectIndex()
    }

    saveRedirectIndex () {
        return this.getId()
        .then( id => {
            return indexerDb.run(
                'INSERT INTO redirects (id, targetKey, fragment) VALUES (?,?,?)',
                [
                    id,
                    this.target.urlKey(),
                    this.target.fragment,
                ]
            )
        })
    }
}

//
// class ResolvedRedirect
//
class ResolvedRedirect extends Item {
    constructor ( id, nameSpace, path, title, target, revision ) {
        super({
            path,
            nameSpace,
            title,
            mimeType: REDIRECT_MIME,
            revision,
        })
        this.target = target
        this.id = id
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

        // redirect dirEntry shorter on 4 byte field
        return super.storeDirEntry( 0, 0, this.target )
    }
}

//
// DataItem
//
class DataItem extends Item {
    constructor ( params ) {
        params.data = params.data === undefined ? null : params.data;
        super( params )
    }

    process () {
        //~ log( 'DataItem process', this.path )
        return this.store()
        .then( () => super.process())
        .catch( err => {
            if ( err instanceof NoProcessingRequired )
                return
            console.error( 'Item process error', this.path, err )
            process.exit( 1 )
        })
    }

    store () {
        return this.getData()
        .then( data => {
            if ( data == null ) {
                console.error( 'DataItem.store error: data == null', this )
                process.exit( 1 )
            }
            if ( !( data instanceof Buffer )) {
                data = Buffer.from( data )
            }
            return ClusterWriter.append( this.mimeType, data, this.path )
            .then( ([ clusterIdx, blobIdx ]) => Object.assign( this, { clusterIdx, blobIdx }))
        })
    }

    getData () {
        return Promise.resolve( this.data )
    }

    storeDirEntry () {
        return super.storeDirEntry( this.clusterIdx, this.blobIdx )
    }
}

//
// class File
//
class File extends DataItem {
    //~ id ,
    //~ mimeId ,
    //~ revision ,
    //~ urlKey ,
    //~ titleKey
    getData () {
        if ( this.data == null )
            this.data = fs.readFile( this.srcPath())
            .then( data => this.preProcess( data ))

        return Promise.resolve( this.data )
    }

    srcPath () {
        return fullPath( this.nameSpace + '/' + this.path )
    }

    preProcess ( data ) {
        switch ( this.mimeType ) {
            case 'image/jpeg':
                return this.processJpeg( data )
            //~ case 'image/gif':
            case 'image/png':
                return this.processImage( data )
            default:
                return data
        }
    }

    processJpeg ( data ) {
        if ( ! argv.optimg )
            return data
        this.mimeType = 'image/jpeg'
        return spawn(
            mozjpeg,
            [ '-quality', argv.jpegquality, data.length < 20000 ? '-baseline' : '-progressive' ],
            data
        )
        .catch( err => {
            log( 'Error otimizing jpeg', err, this )
            return data
        })
    }

    processImage ( data ) {
        if ( ! argv.optimg )
            return data
        return Promise.coroutine( function* () {

            const image = sharp( data )

            const metadata = yield image.metadata()

            if ( metadata.format == 'gif' && isAnimated( data ))
                return data

            if ( metadata.hasAlpha && metadata.channels == 1 ) {
                log( 'metadata.channels == 1', this.path )
            } else if ( metadata.hasAlpha && metadata.channels > 1 ) {
                if ( data.length > 20000 ) {
                    // Is this rather opaque?
                    const alpha = yield image
                        .clone()
                        .extractChannel( metadata.channels - 1 )
                        .raw()
                        .toBuffer()

                    const opaqueAlpha = Buffer.alloc( alpha.length, 0xff )
                    const isOpaque = alpha.equals( opaqueAlpha )

                    if ( isOpaque ) { // convert to JPEG
                        log( 'isOpaque', this.path )
                        if ( metadata.format == 'gif' )
                            data = yield image.toBuffer()
                        return this.processJpeg ( data )
                    }
                }
            }
            if ( metadata.format == 'gif' )
                return data
            return image.toBuffer()
        }).call( this )
        .catch( err => {
            log( 'Error otimizing image', err, this )
            return data
        })
    }
}

//
// class RawFile
//
class RawFile extends File {
    constructor ( path ) {
        const mimeType = mimeFromPath( path )
        const nameSpace = getNameSpace( mimeType )
        super({
            path,
            mimeType,
            nameSpace,
        })
    }

    srcPath () {
        return fullPath( this.path )
    }

    preProcess ( data ) {
        return Promise.coroutine( function* () {
            if ( ! this.mimeType ) {
                this.mimeType = yield mimeFromData( data )
                this.nameSpace = this.nameSpace || getNameSpace( this.mimeType )
            }
            if ( argv.inflateHtml && this.mimeType == 'text/html' )
                data = yield zlib.inflate( data ) // inflateData
            return data
        }).call( this )
        .then( data => this.preProcessHtml( data ))
        .then( data => super.preProcess( data ))
    }

    preProcessHtml ( data ) {
        const dom = ( this.mimeType == 'text/html' ) && cheerio.load( data.toString())
        if ( dom ) {
            const title = dom( 'title' ).text()
            this.title = this.title || title
            const redirectTarget = this.isRedirect( dom )
            if ( redirectTarget ) { // convert to redirect
                const redirect = new Redirect({
                    path: this.path,
                    nameSpace: this.nameSpace,
                    title: this.title,
                    to: redirectTarget,
                })
                return redirect.process()
                .then( () => Promise.reject( new NoProcessingRequired()))
            }
            if ( this.alterLinks( dom ))
                data = Buffer.from( dom.html())
        }
        return Promise.resolve( data )
    }

    alterLinks ( dom ) {
        var base = '/' + this.path
        var nsBase = '/' + this.nameSpace + base
        var baseSplit = nsBase.split( '/' )
        var baseDepth = baseSplit.length - 1
        var changes = 0

        function toRelativeLink ( elem, attr ) {
            try {
                var link = url.parse( elem.attribs[ attr ], true, true )
            } catch ( err ) {
                console.warn( 'alterLinks error', err.message, elem.attribs[ attr ], 'at', base )
                return
            }
            var path = link.pathname
            if ( link.protocol || link.host || ! path )
                return
            var nameSpace = getNameSpace( mimeFromPath( path ))
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
            //~ log( 'alterLinks', nsBase, decodeURI( path ), decodeURI( absPath ), decodeURI( relPath ))

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

        log( 'File.prototype.getRedirect', this.path, splited)
        var link = url.parse( splited[ 1 ].split( '=', 2 )[ 1 ], true, true )
        if ( link.protocol || link.host || ! link.pathname || link.pathname[ 0 ] =='/' )
            return null

        var target = {
            path: decodeURIComponent( link.pathname ),
            fragment: link.hash,
        }
        return target
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
    const metadata = [
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
                article  = mainPage = new Redirect({
                    path: 'mainpage',
                    nameSpace: '-',
                    to: { path: val, nameSpace: 'A' }
                })
                break
            case 'logo':
                article  = new Redirect({
                    path: 'favicon',
                    nameSpace: '-',
                    to: { path: val, nameSpace: 'I' }
                })
                break
            default:
                article = new DataItem({
                    path: name,
                    mimeType: 'text/plain',
                    nameSpace: 'M',
                    data: val
                })
        }
        done.push( article.process())
    })

    return Promise.all( done )
}

function openMetadata( dbName ) {
    return Promise.resolve()
    .then( () => sqlite.open( dbName ))
    .then( db => {
        indexerDb = db
        return indexerDb.exec(`
            PRAGMA synchronous = OFF;
            PRAGMA journal_mode = WAL;
            DROP INDEX IF EXISTS articleUrlKey ;
            DROP INDEX IF EXISTS urlSortedArticleId ;
            DROP INDEX IF EXISTS articleTitleKey ;

            DROP TABLE IF EXISTS urlSorted ;
            DROP TABLE IF EXISTS dirEntries ;
            DROP TABLE IF EXISTS clusters ;

            CREATE TABLE dirEntries (
                id INTEGER PRIMARY KEY,
                offset INTEGER
                );
            CREATE TABLE clusters (
                id INTEGER PRIMARY KEY,
                offset INTEGER
                );
            `
            )
        }
    )
}

function newMetadata() {
    var dbName = ''
    if ( argv.verbose ) {
        var parsed = osPath.parse( outPath )
        dbName = osPath.join( parsed.dir, parsed.base + '.db' )
    }
    return fs.unlink( dbName )
    .catch( () => null )
    .then( () => sqlite.open( dbName ))
    .then( db => {
        indexerDb = db
        return indexerDb.exec(
            'PRAGMA synchronous = OFF;' +
            'PRAGMA journal_mode = OFF;' +
            //~ 'PRAGMA journal_mode = WAL;' +
            'CREATE TABLE articles (' + [
                    'id INTEGER PRIMARY KEY',
                    'mimeId INTEGER',
                    'revision INTEGER',
                    'mwId INTEGER',
                    'urlKey TEXT',
                    'titleKey TEXT',
                ].join(',') +
                ');' +
            'CREATE TABLE redirects (' +
                'id INTEGER PRIMARY KEY,' +
                'targetKey TEXT, ' +
                'fragment TEXT ' +
                ');' +
            'CREATE TABLE dirEntries (' +
                'id INTEGER PRIMARY KEY,' +
                'offset INTEGER' +
                ');' +
            'CREATE TABLE clusters (' +
                'id INTEGER PRIMARY KEY,' +
                'offset INTEGER ' +
                ');'
            )
        }
    )
}

function sortArticles () {
    return indexerDb.exec(`
        CREATE INDEX articleUrlKey ON articles (urlKey);

        CREATE TABLE urlSorted AS
            SELECT
                id,
                urlKey
            FROM articles
            ORDER BY urlKey;

        CREATE INDEX urlSortedArticleId ON urlSorted (id);

        CREATE INDEX articleTitleKey ON articles (titleKey);
        `
    )
}

function loadRedirects () {
    var redirectsFile
    if ( preProcessed )
        redirectsFile = osPath.join( srcPath, 'redirects.csv' )
    else if ( argv.redirects )
        redirectsFile = expandHomeDir( argv.redirects )
    else
        return Promise.resolve()

    const getRow = cvsReader( redirectsFile, {
        columns:[ 'nameSpace', 'path', 'title', 'to' ],
        delimiter: '\t',
        relax_column_count: true
    })

    return Promise.coroutine( function* () {
        while ( true ) {
            const row = yield getRow()
            log( 'loadRedirects', row )
            if ( row ) {
                yield new Redirect( row )
                .process()
            } else {
                return
            }
        }
    }) ()

}

function resolveRedirects () {
    return Promise.coroutine( function* () {
        var stmt = yield indexerDb.prepare( `
            SELECT
                src.id AS id,
                src.urlKey AS urlKey,
                src.titleKey AS titleKey,
                src.revision AS revision,
                targetKey,
                targetRow,
                targetId
            FROM (
                SELECT
                    *,
                    urlSorted.rowid AS targetRow
                FROM (
                    SELECT
                        redirects.id AS id,
                        redirects.targetKey AS targetKey,
                        dst.id AS targetId
                    FROM redirects
                    LEFT OUTER JOIN articles AS dst
                    ON redirects.targetKey = dst.urlKey
                ) AS fromTo
                LEFT OUTER JOIN urlSorted
                ON fromTo.targetId = urlSorted.id
            ) AS dstResolved
            JOIN articles AS src
            USING (id)
            WHERE targetId IS NOT NULL
            ;`)
        while ( true ) {
            const row = yield stmt.get()
            if ( ! row ) {
                break
            }
            var nameSpace = row.urlKey[ 0 ]
            var path = row.urlKey.substr( 1 )
            var title = ( row.titleKey == row.urlKey ) ? '' : row.titleKey.substr( 1 )
            var target = row.targetRow - 1
            if ( path == 'mainpage' ) {
                mainPage.target = target
            }

            yield new ResolvedRedirect ( row.id, nameSpace, path, title, target, row.revision )
            .process()
        }
        yield stmt.finalize()
    }) ()
}

function saveIndex ( query, byteLength, rowField, count, logInfo ) {
    logInfo = logInfo || 'saveIndex'
    log( logInfo, 'start', count )

    return Promise.coroutine( function* () {
        var startOffset
        var i = 0
        var stmt = yield indexerDb.prepare( query )
        while ( true ) {
            const row = yield stmt.get()
            if ( ! row )
                break
            log( logInfo, i, row )
            i++
            var buf = Buffer.allocUnsafe( byteLength )
            writeUIntLE( buf, row[ rowField ], 0, byteLength )

            var offset = yield out.write( buf )
            if ( ! startOffset )
                startOffset = offset
        }
        yield stmt.finalize()

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
            id,
            urlKey,
            offset
        FROM urlSorted
        LEFT OUTER JOIN dirEntries
        USING (id)
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
            'titleKey, ' +
            'urlSorted.rowid - 1 AS articleNumber ' +
        'FROM urlSorted ' +
        'JOIN articles ' +
        'USING (id) ' +
        'ORDER BY titleKey ' +
        ';',
        4, 'articleNumber', articleCount, 'storeTitleIndex'
    )
    .then( offset => header.titlePtrPos = offset )
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
    writeUIntLE( buf, header.magicNumber,     0, 4 )
    writeUIntLE( buf, header.version,         4, 4 )

    uuid.v4( null, buf,                      8 )

    writeUIntLE( buf, header.articleCount,    24, 4 )
    writeUIntLE( buf, header.clusterCount,    28, 4 )

    writeUIntLE( buf, header.urlPtrPos,       32, 8 )
    writeUIntLE( buf, header.titlePtrPos,     40, 8 )
    writeUIntLE( buf, header.clusterPtrPos,   48, 8 )
    writeUIntLE( buf, header.mimeListPos,     56, 8 )

    writeUIntLE( buf, header.mainPage,        64, 4 )
    writeUIntLE( buf, header.layoutPage,      68, 4 )

    writeUIntLE( buf, header.checksumPos,     72, 8 )

    return buf
}

function storeHeader() {
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

    out = new Writer( outPath ); // create output file
    log( 'reserving space for header and mime type list' )
    out.write( Buffer.alloc( headerLength + maxMimeLength ))

    var metadata = osPath.join( srcPath, 'metadata.db' )
    if ( fs.existsSync( metadata )) {
        preProcessed = true
        return openMetadata( metadata )
        .then( () => loadMimeTypes())
    }
    return newMetadata()
    .then( () => loadMetadata())
}

function rawLoader () {
    const dirs = [ '' ]

    function scanDirectories ( path ) {
        return Promise.coroutine( function* () {
            for ( let path; ( path = dirs.shift()) != null; ) {
                log( 'scanDirectory', path )

                yield Promise.map(
                    fs.readdir( fullPath( path )),
                    fname => parseDirEntry( osPath.join( path, fname )),
                    { concurrency: 8 }
                )
            }
        }) ()
    }

    function parseDirEntry ( path ) {
        if ( path == 'metadata.csv' || path == 'redirects.csv' )
            return Promise.resolve()

        return fs.lstat( fullPath( path ))
        .then( stats => {
            if ( stats.isDirectory())
                return dirs.push( path )
            if ( stats.isFile() || stats.isSymbolicLink()) {
                return new RawFile( path ).process()
            }
            return Promise.reject( new Error( 'Invalid dir entry ' + absPath ))
        })
    }

    log( 'rawLoader start' )
    return scanDirectories()
    .then( () => log( 'rawLoader finished !!!!!!!!!' ))
}

function loadPreProcessedArticles () {
    return Promise.coroutine( function* () {
        var stmt = yield indexerDb.prepare( `
            SELECT
                id ,
                mimeId ,
                revision ,
                urlKey ,
                titleKey
            FROM articles
            WHERE mimeId IS NOT 0xffff
            ;`)
        while ( true ) {
            const row = yield stmt.get()
            if ( ! row ) {
                break
            }
            var nameSpace = row.urlKey[ 0 ]
            var path = row.urlKey.substr( 1 )
            var title = ( row.titleKey == row.urlKey ) ? '' : row.titleKey.substr( 1 )
            yield new File( {
                nameSpace,
                path,
                title,
                mimeType: mimeFromIndex( row.mimeId ),
                id: row.id,
                revision: row.revision,
            } )
            .process()
        }
        yield stmt.finalize()
    }) ()
}

function loadMimeTypes () {
    return Promise.coroutine( function * () {
        var stmt = yield indexerDb.prepare( `
            SELECT
                id ,
                value
            FROM mimeTypes
            ORDER BY id
            ;`)
        while ( true ) {
            const row = yield stmt.get()
            if ( ! row ) {
                break
            }
            mimeTypeList.push( row.value )
        }
        yield stmt.finalize()
    }) ()
}

function loadRawArticles () {
    return Promise.resolve()
    .then( () => rawLoader())
    .then( () => loadRedirects())
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
        yield indexerDb.close()
        yield storeHeader()
        yield calculateFileHash()
    }) ()
}

function core () {
    return Promise.coroutine( function* () {
        yield initialise()
        yield preProcessed ? loadPreProcessedArticles() : loadRawArticles()
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

    argv
    .version( packageInfo.version )
    .arguments( '<source-directory> [zim-file...]' )
    .description( `Pack a directory into a zim file

  Where:
    source-directory \t path to the directory with HTML pages to pack into a ZIM file
    zim-file \t\t optional path for the output` )
    // Mandatory arguments:
    .option( '-w, --welcome <page>', 'path of default/main HTML page. The path must be relative to HTML_DIRECTORY', 'index.htm' )
    .option( '-f, --favicon <file>', 'path of ZIM file favicon. The path must be relative to HTML_DIRECTORY and the image a 48x48 PNG', 'favicon.png' )
    .option( '-l, --language <id>', 'language code of the content in ISO639-3', 'eng' )
    .option( '-t, --title <title>', 'title of the ZIM file', '' )
    .option( '-d, --description <text>', 'short description of the content', '' )
    .option( '-c, --creator  <text>', 'creator(s) of the content', '' )
    .option( '-p, --publisher  <text>', 'creator of the ZIM file itself', '' )
    // Optional arguments:
    .option( '-v, --verbose', 'print processing details on STDOUT' )
    .option( '-m, --minChunkSize <size>', 'number of bytes per ZIM cluster (default: 2048)', parseInt, 2048 )
    .option( '-x, --inflateHtml', 'try to inflate HTML files before packing (*.html, *.htm, ...)' )
    .option( '-u, --uniqueNamespace', 'put everything in the same namespace "A". Might be necessary to avoid problems with dynamic/javascript data loading' )
    .option( '-r, --redirects <path>', 'path to the CSV file with the list of redirects (url, title, target_url tab separated)', '' )
    // Not implemented
    //~ .option( '-i, --withFullTextIndex', 'index the content and add it to the ZIM' )
    // Extra arguments:
    .option( '--optimg', 'optimise images' )
    .option( '--jpegquality <factor>', 'JPEG quality', parseInt, 60 )
    .parse( process.argv )

    log( argv )

    const args = argv.args

    while ( args[ 0 ] == '' ) // if mwoffliner prepends with empty extra parameter(s)
        args.shift()

    srcPath = expandHomeDir( args[ 0 ])
    if ( args[ 1 ])
        outPath = expandHomeDir( args[ 1 ])
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
