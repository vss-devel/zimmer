#!/bin/sh
":" //# -*- mode: js -*-; exec /usr/bin/env TMPDIR=/tmp node --max-old-space-size=2000 --stack-size=42000 "$0" "$@"

// node --inspect-brk

"use strict"

/*

MIT License

Copyright (c) 2017 Vadim Shlyakhov

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
const os = require('os')
const osProcess = require('process')
const osPath = require( 'path' )
const urlconv = require('url')
const crypto = require("crypto")

const command = require('commander')
const fs = require('fs-extra')
const requestPromise = require('request-promise-native')
const sqlite = require( 'sqlite' )
const cheerio = require('cheerio')
const minify = require('html-minifier').minify

const langs = require('langs')
const encodeurl = require('encodeurl')
const iconv = require('iconv-lite')
const lru = require('quick-lru')

const mimeTypes = require( 'mime-types' )
const mmmagic = require( 'mmmagic' )
const mimeMagic = new mmmagic.Magic( mmmagic.MAGIC_MIME_TYPE )

const moment = require("moment")
require("moment-duration-format")

const startTime = Date.now()

const cpuCount = os.cpus().length

const mimeIds = []

let articleCount = 0
let redirectCount = 0
let http // http request

// https://msdn.microsoft.com/en-us/library/windows/desktop/aa365247
// just in case https://www.mediawiki.org/wiki/Manual:Page_title
let sanitizeRE = /(?:[\x00-\x1F<>:"~\\\?\*]|%(?:[^0-9A-Fa-f]|[0-9A-Fa-f][^0-9A-Fa-f])|(?:[. ]$))+/g

function sanitizeFN ( name ) { // after https://github.com/pillarjs/encodeurl
    return String( name ).replace( sanitizeRE, encodeURIComponent ).replace( /%/g, '~' )
    //~ return sanitizeFilename( name, { replacement: '.' })
}

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
    console.trace( elapsedStr( startTime ), ... args )
    osProcess.exit( 1 )
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

let UserAgent = `wikizimmer/${packageInfo.version} (https://github.com/vadp/zimmer email:vadp.devl@gmail.com)`
const UserAgentFirefox = 'Mozilla/5.0 (X11; Linux x86_64; rv:12.0) Gecko/20100101 Firefox/12.0'

function pooledRequest( request, referenceUri, maxTokens = 1, interval = 10 ) {
    const retryErrorCodes = [ 'EPROTO', 'ECONNRESET', 'ESOCKETTIMEDOUT' ]
    const retryStatusCodes = [ 408, 420, 423, 429, 500, 503, 504, 509, 524 ]
    const retryLimit = 10
    const retryExternal = command.retryExternal == null ? retryLimit : command.retryExternal
    const requestTimeout = 5 * 60 * 1000
    const refHost = urlconv.parse( referenceUri ).host
    const hostQueues = {}

    class Queue {
        constructor () {
            this.queue = []
            this.timer = null
            this.supressTimer = null
            this.supressTimeout = 60 * 1000
            this.tokenCounter = 0
            this.interval = interval
        }

        reshedule () {
            if ( this.supressTimer )
                return
            this.timer = setTimeout(
                () => ( this.timer = null, this.run() ),
                this.interval
            )
        }

        pause ( query ) {
            clearTimeout( this.timer )
            this.timer = null

            clearTimeout( this.supressTimer )
            this.supressTimer = setTimeout(
                () => ( this.supressTimer = false, this.reshedule()),
                query.retries * this.supressTimeout
            )
        }

        retry ( query, error ) {
            const retryCause = retryStatusCodes.includes( error.statusCode ) ? error.statusCode :
                error.cause && retryErrorCodes.includes( error.cause.code ) ? error.cause.code : false
            const maxRetries = query.external ? retryExternal : retryLimit
            if ( ! retryCause || query.retries > maxRetries)
                return false

            if ( query.retries > maxRetries / 2 ) {
                this.interval = this.interval * 2
            }
            query.retries ++

            log( 'retry request', query.retries, this.interval, error.name, retryCause, error.options.uri || error.options.url ) // , query )
            this.queue.push( query )
            this.pause( query )
            return true
        }

        submit ( query ) {
            this.tokenCounter ++
            return request( query )
            .then( reply => {
                this.tokenCounter --
                if ( reply )
                    query.resolve( reply )
                else
                    query.reject( )
                this.reshedule()
            })
            .catch( error => {
                this.tokenCounter --
                if ( ! this.retry( query, error )) {
                    warning( 'HTTP error',  error.cause && error.cause.code || error.statusCode, error.options.uri || error.options.url )
                    query.reject( error )
                    this.reshedule()
                }
            })
        }

        run () {
            if ( this.timer || this.supressTimer || this.tokenCounter >= maxTokens )
                return
            const query = this.queue.shift()
            if ( query ) {
                //~ if ( query.retries > 0 )
                    //~ debugger
                this.submit( query )
                this.reshedule()
            }
        }

        append ( query ) {
            return new Promise(( resolve, reject ) => {
                query.resolve = resolve
                query.reject = reject
                query.retries = 0

                if ( query.priority )
                    this.queue.unshift( query )
                else
                    this.queue.push( query )

                this.run()
            })
        }
    }

    function processOptions ( query ) {
        let url
        if ( typeof query === 'string' || query.href !== undefined ) {
            // string or URL object
            url = query
            query = {}
        } else {
            url = query.uri || query.url
            delete query.uri
        }
        query.url = urlconv.resolve( referenceUri, url )
        query.host = urlconv.parse( query.url ).host
        query.external = query.host != refHost

        if ( ! query.headers )
            query.headers = {}
        query.headers[ 'User-Agent' ] = UserAgent
        query.headers[ 'Referer' ] = referenceUri
        query.resolveWithFullResponse = true
        query.timeout = requestTimeout
        query.forever = true

        log( '^', decodeURI( query.url ), query.qs || '' )

        return query
    }

    return function ( query, queueId ) {
        processOptions( query )
        if ( ! queueId )
            queueId = query.host
        let queue = hostQueues[ queueId ]
        if ( ! queue ) {
            queue = new Queue
            hostQueues[ queueId ] = queue
        }
        return queue.append( query )
    }
}

function api ( params, options = {} ) {
    if ( options.method == 'POST' && options.form )
        options.form.format = 'json'
    else
        params.format = 'json'
    Object.assign( options, {
        url: wiki.apiUrl,
        qs: params,
    })

    return http( options )
    .then( reply => {
        const res = JSON.parse( reply.body )
        return res.error || res.warning ? Promise.reject( res.error || res.warning ) : res
    })
}

function apiPost( params ) {
    return api( null, {
        method: 'POST',
        form: params,
    })
}

class NameSpaceSet {
    constructor ( SiteInfo ) {
        this.nameSpaces = {}
        this.nsQueue = []
        this.nsUsed = new Set
        Object.keys( SiteInfo.namespaces ).map( ns => {
            const nsInfo = SiteInfo.namespaces[ ns ]
            this.nameSpaces[ nsInfo.canonical ] = this.nameSpaces[ nsInfo[ '*' ]] = this.nameSpaces[ ns ] = nsInfo
        })
        if ( SiteInfo.namespacealiases ) {
            SiteInfo.namespacealiases.map( nsInfo => this.nameSpaces[ nsInfo[ '*' ]] = this.nameSpaces[ nsInfo.id ])
        }
    }

    check ( nsId ) {
        return this.nsUsed.has( nsId )
    }

    init ( nsList = '0' ) {
        nsList.split( ',' ).map( ns => this.request( ns ))
    }

    request ( nsIdx ) {
        const ns = this.nameSpaces[ nsIdx ]
        if ( ! ns ) {
            fatal( 'This wiki does not have name space', nsIdx )
            return
        }
        if ( ! this.check( ns.id )) {
            this.nsUsed.add( ns.id )
            this.nsQueue.push( ns.id )
        }
    }

    * [Symbol.iterator] () {
        while ( this.nsQueue.length != 0 ) {
            yield this.nsQueue.shift()
        }
    }
}

const wiki = {
    saveDir: null,
    apiUrl: null,
    metadata: {},
    nameSpaces: null,
}

class WikiItem {
    constructor ( zimNameSpace, url, title ) {
        this.encoding = null
        this.revision = 0
        this.id = null
        this.loadPriority = false
        Object.assign( this, { zimNameSpace, url, title })
    }

    async getData () {
        let data = await ( this.data !== undefined ? this.data : ( this.data = this.load( )))
        return this.preProcess( data )
    }

    preProcess ( data ) {
        return data
    }

    urlReplacements () {
        if ( typeof command.urlReplace != 'object' ) {
            return this.url
        } else {
            return command.urlReplace.reduce(
                ( acc, [ patt, repl ]) => acc.replace( patt, repl ),
                this.url
            )
        }
    }

    blackListed () {
        if ( typeof command.urlBlacklist != 'object' ) {
            return false
        }
        return command.urlBlacklist.some( patt => this.url.includes( patt ))
    }

    async load () {
        let resp
        try {
            resp = await http({
                url: this.urlReplacements(),
                encoding: null,
                priority: this.loadPriority
            })
        } catch ( error ) {
            if ( ! command.downloadErrors || error.options.external || error.statusCode == 404 || error.statusCode == 400 ) {
                throw error
            }
            fatal( 'Fatal load error' )
            //~ return Promise.reject( new Error( 'Load error' ))
        }
        let data = resp.body

        this.url = resp.request.href // possibly redirected
        this.headers = resp.headers
        if ( ! this.revision ) {
            const modified = this.headers[ 'last-modified' ] // "Tue, 27 Jun 2017 14:37:49 GMT"
            const dateBasedRevision = Math.round(( Date.parse( modified ) - Date.parse( '2000-01-01' )) / 1000 ) || 0
            this.revision = dateBasedRevision
        }

        const contentType = resp.headers[ "content-type" ]
        let csplit = contentType.split( ';' )
        this.mimeType = csplit[ 0 ]

        if ( this.mimeType.split( '/' )[ 0 ] == 'text' ) {
            this.encoding = 'utf-8'
            if ( csplit.length > 1 && csplit[ 1 ].includes( 'charset=' )) {
                this.encoding = csplit[ 1 ].split( '=' )[ 1 ]
            }
        }

        if ( this.mimeType == 'application/x-www-form-urlencoded' ) {
            return mimeFromData( data )
            .then( mimeType => {
                this.mimeType = mimeType
                return data
            })
            .catch( err => data )
        }

        if ( Buffer.isBuffer( data ) && this.encoding != null ) {
            data = iconv.decode( data, this.encoding )
        }

        return data
    }

    basePath () {
        const purl = urlconv.parse( this.url )
        const pathp = osPath.parse( purl.pathname )
        return sanitizeFN( decodeURIComponent( pathp.base ))
    }

    localPath () {
        return  this.zimNameSpace + '/' + this.basePath()
    }

    pathToTop () {
        return '../'.repeat( this.basePath().split( '/' ).length - 1 )
    }

    urlKey () {
        return this.zimNameSpace + this.basePath()
    }

    titleKey () {
        return this.title ? this.zimNameSpace + this.title : this.urlKey()
    }

    mimeId () {
        if ( this.mimeType == null )
            fatal( 'this.mimeType == null', this )
        let id = mimeIds.indexOf( this.mimeType )
        if ( id == -1 ) {
            id = mimeIds.length
            mimeIds.push( this.mimeType )
        }
        return id
    }

    storeData ( data ) {
        if ( data == null )
            return

        const savePath = osPath.join( wiki.saveDir, this.localPath())
        log( '+', savePath )

        return fs.outputFile( savePath, data )
    }

    async storeMetadata ( ) {
        const row = [
            this.urlKey(),
            this.titleKey(),
            this.revision,
            this.mimeId(),
        ]
        try {
            const res = await wiki.db.run(
                'INSERT INTO articles ( urlKey, titleKey, revision, mimeId ) VALUES ( ?,?,?,? )',
                row
            )
            //~ log( 'storeMetadata res', row, res )
            this.id = res.stmt.lastID
            ++ articleCount
            return this.id
        } catch ( err ) {
            if ( err.code == "SQLITE_CONSTRAINT" )
                return null
            fatal( 'storeMetadata error', err )
        }

    }

    async save () {
        if ( this.blackListed() )
            return ''
        try {
            const data = await this.getData()
            await this.storeData( data )
            await this.storeMetadata()
            return this.localPath()
        } catch ( err ) {
            warning( 'Save error', err.name, this.url, '->', this.localPath())
            return ''
        }
    }
}

// {
//  "pageid": 10,
//  "ns": 0,
//  "title": "Baltic Sea",
//  "touched": "2017-06-27T14:37:49Z",
//  "lastrevid": 168879,
//  "counter": 62340,
//  "length": 9324,
//  "fullurl": "http:\/\/www.cruiserswiki.org\/wiki\/Baltic_Sea",
//  "editurl": "http:\/\/www.cruiserswiki.org\/index.php?title=Baltic_Sea&action=edit"
// }
// {
//  "ns": 0,
//  "title": "Anchorages of Lesvos Island",
//  "missing": "",
//  "fullurl": "http:\/\/www.cruiserswiki.org\/wiki\/Anchorages_of_Lesvos_Island",
//  "editurl": "http:\/\/www.cruiserswiki.org\/index.php?title=Anchorages_of_Lesvos_Island&action=edit"
// }
class ArticleStub extends WikiItem {
    constructor ( pageInfo ) {
        super( 'A', urlconv.resolve( wiki.articleUriPrefix, pageInfo.fullurl ), pageInfo.title )
        this.info = pageInfo
        this.mwId = pageInfo.pageid
        this.revision = pageInfo.lastrevid
    }

    basePath () {
        if ( this.url && this.url.startsWith( wiki.articleUriPrefix )) {
            const urlParsed = urlconv.parse( this.url )
            const subPath =  urlParsed.pathname.replace( wiki.articlePath, '' )
            return sanitizeFN( decodeURIComponent( subPath )) + '.html'
        }
        return null // not a local article
    }
}

class Article extends ArticleStub {
    constructor ( pageInfo ) {
        super( pageInfo )
    }

    async preProcess( data ) {
        let src
        try {
            src = cheerio.load( data )
        } catch ( e ) {
            log( 'cheerio.load error', e, data )
            return data
        }
        try {
            let content = src( '#bodyContent' )
            if ( content.length == 0 ) {
                content = src( 'article' )
            }
            if ( content.length == 0 ) {
                fatal( "Article.preProcess -- fatal error: Can't find article's content:", this.title )
            }

            const dom = cheerio.load( wiki.pageTemplate )
            dom( 'title' ).text( this.title )

            dom( '#bodyContent' ).replaceWith( content )

            // display content inside <noscript> tags
            dom( 'noscript' ).each( (i, elem) => {
                let e = dom( elem )
                e.replaceWith( e.contents() )
            })

            // clean up
            dom( wiki.pageRemovals ).each( (i, elem) => {
                dom( elem ).remove()
            })

            // modify links
            let css = dom( '#layout-css' )
            css.attr( 'href', this.pathToTop() + css.attr( 'href' ))

            dom( 'a' ).each( (i, elem) => {
                this.transformGeoLink( elem ) ||
                this.transformLink( elem )
            })
            // map area links
            dom( 'area' ).each( (i, elem) => {
                this.transformLink( elem )
            })

            let done = dom( 'img' ).toArray().map( elem => this.saveImage( elem ))
            done = done.concat( dom( '[style*="url("]' ).toArray().map( elem => this.transformStyle( elem )))

            await Promise.all( done )

            this.mimeType = 'text/html'
            this.encoding = 'utf-8'
            const out = dom.html()
            const minified = minify( out, {
                collapseWhitespace: true,
                conservativeCollapse: true,
                decodeEntities: true,
                sortAttributes: true,
                sortClassName: true,
                removeComments: true,
            })
            return minified
        } catch ( err ) {
            log( err )
            return null
        }
    }

    transformLink( elem ) {
        const url = elem.attribs.href
        if (! url || url.startsWith( '#' ))
            return

        const link = new ArticleStub({ fullurl: url })

        const path = urlconv.parse( link.url ).pathname
        if ( ! path || path == '/' )
            return

        const basePath = link.basePath()
        if ( basePath != null ) { // local article link
            if ( path.includes( ':' )) {
                delete elem.attribs.href // block other name spaces
            } else {
                elem.attribs.href = this.pathToTop() + basePath
            }
        }
        const pathlc = path.toLowerCase()
        for ( const ext of [ '.jpg', '.jpeg', '.png', '.gif', '.svg' ]) {
            if (pathlc.endsWith( ext )) {
                delete elem.attribs.href // block links to images
            }
        }
    }

    transformGeoLink( elem ) {
        const lat = elem.attribs[ "data-lat" ]
        const lon = elem.attribs[ "data-lon" ]
        if ( lat == null || lon == null )
            return false

        elem.attribs.href = `geo:${lat},${lon}`
        return true
    }

    async transformStyle ( elem ) {
        let style = new Style( this.url, elem.attribs.style )
        return elem.attribs.style = await style.getData()
    }

    async saveImage ( elem ) {
        delete elem.attribs.srcset
        let url = elem.attribs.src
        if (! url || url.startsWith( 'data:' ))
            return
        const image = new Image( url )
        const localPath = await image.save()
        elem.attribs.src = encodeURI( this.pathToTop() + '../' + localPath )
    }
}

class Redirect extends ArticleStub {

    constructor ( info ) {
        super( info )
        this.data = null
        this.to = info.to
        this.toFragment = info.toFragment
    }

    mimeId () {
        return 0xffff
    }

    async storeMetadata ( ) {
        const id = await super.storeMetadata()
        if ( ! id )
            return
        const target = new ArticleStub( this.to )
        const row = [
            id,
            target.urlKey(),
            this.toFragment,
        ]

        log( '>', this.title || this.url, row)

        return wiki.db.run(
            'INSERT INTO redirects (id, targetKey, fragment) VALUES (?,?,?)',
            row
        )
    }
}

class Metadata extends WikiItem {
    constructor ( url, data ) {
        super( 'M', url)
        this.mimeType = 'text/plain'
        this.data = data
    }
    getData () {
        return this.data
    }
}

class MainPage extends WikiItem {
    constructor ( ) {
        super( '.', 'mainpage' )
        this.mimeType = 'text/plain'
    }
    basePath () {
        return 'mainpage'
    }
    async getData  ( ) {
        const reply = await apiPost({
            action: 'query',
            titles: wiki.mainPage,
            redirects: '',
            prop: 'info',
            inprop: 'url',
        })
        try {
            const to = Object.keys( reply.query.pages ).map( key => this.to = reply.query.pages[ key ])[0]
            const target = new ArticleStub( to )

            return target.basePath()
        } catch ( err ) {
            return null
        }
    }
    async save () {
        const data = await this.getData()
        await this.storeData( data )
        return this.localPath()
    }
}

//~ const urlCache = lru( 5000 )
const urlCache = new lru({ maxSize:500 })

class PageComponent extends WikiItem {
    basePath () {
        let name
        const purl = urlconv.parse( this.url )
        if ( purl.query && purl.query.includes( '=' ) && this.mimeType ) {
            const pathp = osPath.parse( purl.path )
            const ext = '.' + mimeTypes.extension( this.mimeType )
            name = pathp.base + ext
        } else {
            const pathp = osPath.parse( purl.pathname )
            name = pathp.name + pathp.ext.toLowerCase()
        }
        return sanitizeFN( decodeURIComponent( name ))
    }

    save () {
        let saved = urlCache.get( this.url )
        if (! saved ) {
            saved = super.save()
            urlCache.set( this.url, saved )

            saved.then( localPath => { // keep item's data in the cache
                urlCache.set( this.url, Promise.resolve( localPath ))
            })
        }
        return saved
    }
}

class Image extends PageComponent {
    constructor ( url ) {
        super( 'I', url)
        this.loadPriority = true
    }
/*
    data () {
        if (! command.images )
            return null
        return super.getData()
    }
*/
    save () {
        if (! command.images )
            return Promise.resolve( this.localPath() )
        return super.save()
    }
    basePath () {
        if ( ! this.url )
            return null

        const purl = urlconv.parse( this.url )
        let path
        if ( purl.hostname == wiki.baseParsed.hostname || purl.hostname == 'upload.wikimedia.org' ) {
            path =  super.basePath()
        } else {
            const turl = urlconv.parse( '' )
            turl.hostname = purl.hostname
            turl.pathname = purl.pathname
            const tpath = turl.format()
            path = sanitizeFN( decodeURIComponent( tpath ))
        }
        if ( osPath.extname( path ) == '' ) {
            path = path + '.' + mimeTypes.extension( this.mimeType )
        }
        return path
    }
}

//~ const layoutFileNames = new Set()

class LayoutItem extends PageComponent {
    constructor ( url ) {
        super( '-', url )
    }
/*
    checkPath (name) {
        let outname = name
        for (let i=1; layoutFileNames.has (outname); i++ ) {
            const pname = osPath.parse (name)
            outname = (pname.dir ? pname.dir + '/' : '') + `${pname.name}-${i}` + (pname.ext ? pname.ext : '')
        }
        layoutFileNames.add( name )
        return name
    }
*/
}

class FavIcon extends LayoutItem {
    constructor ( ) {
        super( wiki.info.general.logo || 'http://www.openzim.org/w/images/e/e8/OpenZIM-wiki.png' )
    }
    basePath () {
        return 'favicon'
    }
}

class Style extends LayoutItem {
    constructor ( url, data ) {
        super( url )
        this.mimeType = 'text/css'
        this.data = data
    }

    async preProcess ( data ) {
        // collect urls using dummy replacements
        const urlre = /(url\(['"]?)([^\)]*[^\)'"])(['"]?\))/g
        const requests = []
        data.replace( urlre, ( match, start, url, end ) => {
            if ( ! url.startsWith( 'data:' )) {
                const styleItem = new LayoutItem( urlconv.resolve( this.url, url ))
                requests.push( styleItem.save() )
            } else {
                requests.push( url )
            }
            return match
        })
        const resolvedUrls = await Promise.all( requests )
        const transformed = data.replace( urlre, ( match, start, url, end ) => {
            let out = match
            const rurl = resolvedUrls.shift()
            if ( rurl != null ) {
                let newUrl = this.pathToTop() + '..' + rurl
                out = start + newUrl + end
            } else {
                out = ''
            }
            return out
        })
        return transformed
    }
}

const cssDependencies = new Set()

class GlobalCss extends LayoutItem {
    constructor ( sourceDOM ) {
        super( 'zim.css' )
        this.sourceDOM = sourceDOM
        this.mimeType = 'text/css'
    }

    async load () {
        // get css stylesheets
        const cssLinks = this.sourceDOM( 'link[rel=stylesheet][media!=print]' ).toArray()
        const requests = cssLinks.map( elem => this.getCss( elem.attribs.href ))

        const stub = osPath.resolve( module.filename, '../stub.css' )
        requests.unshift( fs.readFile( stub, 'utf8' ))

        const chunks = await Promise.all( requests )
        return chunks.join( '\n' )
    }

    async getCss( cssUrl ) {
        let css = new Style( cssUrl )
        const data = await css.getData()

        const outcss = `/*
*
* from ${cssUrl}
*
*/
${data}
`
        return outcss
    }
}

async function processSamplePage ( samplePageUrl,  rmdir) {
    const resp = await requestPromise({
        url: encodeurl( samplePageUrl ),
        resolveWithFullResponse: true,
    })
    //~log(resp)

    // set base for further http requests
    const realUrl = resp.request.href
    http = pooledRequest( requestPromise, realUrl )

    // create download directory
    const purl = urlconv.parse( realUrl )
    wiki.saveDir = sanitizeFN( purl.hostname )
    if ( rmdir )
        await fs.remove( wiki.saveDir )
    await fs.mkdirs( wiki.saveDir )

    const dom = cheerio.load( resp.body )

    // find out API entry URL
    let phpUrl = dom('link[rel="EditURI"]').attr('href')
    if ( ! phpUrl ) {
        phpUrl = dom('#ca-history a').attr('href')
    }
    if ( ! phpUrl ) {
        fatal( "processSamplePage -- fatal error: API entry URL" )
    }
    //~log(resp.request.href, phpUrl, urlconv.resolve(resp.request.href, phpUrl))
    const parsedUrl = urlconv.parse(urlconv.resolve(resp.request.href, phpUrl))
    log(parsedUrl)
    parsedUrl.search = null
    parsedUrl.hash = null
    const indexPhp = urlconv.format(parsedUrl)
    parsedUrl.pathname = parsedUrl.pathname.replace('index.php', 'api.php')

    wiki.apiUrl = urlconv.format(parsedUrl)
    log(indexPhp, wiki.apiUrl)

    return dom
}

async function loadPreRequisites () {
    const stubPath = osPath.resolve( module.filename, '../stub.html' )
    wiki.pageTemplate = await fs.readFile ( stubPath, 'utf8' )
    const removalsPath = osPath.resolve( module.filename, '../removals.txt' )
    wiki.pageRemovals = await fs.readFile ( removalsPath, 'utf8' )
}

async function getSiteInfo () {
    const resp = await api ({
        action: 'query',
        meta: 'siteinfo',
        siprop: 'general|namespaces|namespacealiases',
    })

    const info = resp.query
    log( 'SiteInfo', info )
    wiki.info = info
    wiki.indexUrl = info.general.script
    wiki.mainPage = info.general.mainpage
    wiki.articlePath = info.general.articlepath.split('$')[0]
    wiki.articleUriPrefix = info.general.base.split( wiki.articlePath )[0] + wiki.articlePath
    wiki.baseParsed = urlconv.parse( wiki.articleUriPrefix )
    wiki.uri = urlconv.resolve( wiki.articleUriPrefix, wiki.info.general.server )
    wiki.nameSpaces = new NameSpaceSet( info )
}

async function saveMetadata () {

    // Name         yes     A human readable identifier for the resource. It's the same across versions (should be stable across time). MUST be prefixed by the packager name.  kiwix.wikipedia_en.nopics
    // Title        yes     title of zim file   English Wikipedia
    // Creator      yes     creator(s) of the ZIM file content  English speaking Wikipedia contributors
    // Publisher    yes     creator of the ZIM file itself  Wikipedia user Foobar
    // Date         yes     create date (ISO - YYYY-MM-DD)  2009-11-21
    // Description  yes     description of content  This ZIM file contains all articles (without images) from the english Wikipedia by 2009-11-10.
    // Language     yes     ISO639-3 language identifier (if many, comma separated)     eng
    // Tags         no      A list of tags  nopic;wikipedia
    // Relation     no      URI of external related ressources
    // Source       no      URI of the original source  http://en.wikipedia.org/
    // Counter      no      Number of non-redirect entries per mime-type    image/jpeg=5;image/gif=3;image/png=2;...
    //
    // Favicon a favicon (48x48) is also mandatory and should be located at /-/favicon

    let lang = wiki.info.general.lang.split('-')[0] // https://www.mediawiki.org/wiki/Manual:Language#Notes
    if (lang.length == 2) {
        const langObj = langs.where( '1', lang )
        lang = langObj['3']
    }

    const metadata = {
        Name: 'wikizimmer.' + wiki.info.general.wikiid,
        Title: wiki.info.general.sitename,
        Creator: '',
        Publisher: '',
        Date: new Date().toISOString().split('T')[0],
        Description: '',
        Language: lang,
        //~ Tags: '',
        //~ Relation: '',
        //~ Counter: '',
        Source: wiki.uri,
    }

    await new MainPage().save()
    await new FavIcon().save()

    for ( let i in metadata ) {
        await new Metadata( i, metadata[i] ).save()
    }
}

async function saveMimeTypes () {
    for ( let i=0, li=mimeIds.length; i < li; i++ ) {
        await wiki.db.run(
            'INSERT INTO mimeTypes (id, value) VALUES (?,?)',
            [ i + 1, mimeIds[ i ]]
        )
    }
}

function batchRedirects ( pageInfos ) {
    if ( pageInfos.length == 0 )
        return Promise.resolve()

    const titles = pageInfos.map( item => item.title ).join( '|' )

    return apiPost({
        action: 'query',
        titles,
        redirects: '',
        prop: 'info',
        inprop: 'url',
    })
    .then( reply => {
        //~ log( 'batchRedirects reply', reply )

        const redirects = reply.query.redirects
        const redirectsByFrom = {}
        redirects.map( item => ( redirectsByFrom[ item.from ] = item ))

        const targets = reply.query.pages
        const targetsByTitle = {}
        Object.keys( targets ).map( key => {
            const item = targets[ key ]
            targetsByTitle[ item.title ] = item
        })

        const done = pageInfos.map( item => {
            let target = null
            let rdr
            for ( let from = item.title; target == null; from = rdr.to ) {
                rdr = redirectsByFrom[ from ]
                if ( ! rdr || rdr.tointerwiki != null || rdr.to == item.title )
                    return null // dead end, interwiki or circular redirection
                target = targetsByTitle[ rdr.to ]
            }
            if ( target.missing != null )
                return null  // no target exists
            if ( ! wiki.nameSpaces.check( target.ns ))
                return null
            item.to = target
            item.toFragment = rdr.tofragment
            return new Redirect( item ).save()
        })
        return Promise.all( done )
    })
}

async function batchPages ( nameSpace ) {
    const queryPageLimit = 500
    const queryMaxTitles = 50

    const exclude = command.exclude ?
        new RegExp( command.exclude ) :
        { test: () => false }
    const query = {
        action: 'query',
        prop: 'info',
        inprop: 'url',
    }
    Object.assign(
        query,
        nameSpace == null ?
        {   titles: command.titles } :
        {
            generator: 'allpages',
            gapnamespace: nameSpace,
            gaplimit: queryPageLimit,
            rawcontinue: '',
        }
    )

    let continueFrom = ''
    while ( true ) {
        await wiki.db.run(
            'INSERT OR REPLACE INTO continue (id, "from") VALUES (1, ?)',
            [ continueFrom ]
        )
        if ( continueFrom == null )
            break

        await wiki.db.run( 'BEGIN' )

        const resp = await api( query )
        let pages = {}
        try {
            pages = resp.query.pages
            //~ log( '*pages', pages )
        }
        catch (e) {
            log( 'getPages', 'NO PAGES' )
        }
        let redirects = []
        const done = Object.keys( pages ).map( key => {
            if ( parseInt( key ) < 0 ) { // no such page
                return null
            }
            const pageInfo = pages[ key ]
            if ( pageInfo.redirect != null ) {
                log( '>' , pageInfo.title )
                redirects.push( pageInfo )
                if ( redirects.length == queryMaxTitles ) {
                    const res = batchRedirects( redirects )
                    redirects = []
                    return res
                }
                return null
            }
            if ( ! command.pages || exclude.test( pageInfo.title )) {
                log( 'x', pageInfo.title )
                return null
            }
            log( '#', pageInfo.title )
            return new Article( pageInfo ).save()
        })
        done.push( batchRedirects( redirects ))
        await Promise.all( done )

        await wiki.db.run( 'COMMIT' )

        continueFrom = null
        try {
            const continueKey = Object.keys( resp[ 'query-continue' ].allpages )[ 0 ]
            continueFrom = resp[ 'query-continue' ].allpages[ continueKey ]
            query[ continueKey ] = continueFrom
            log( '...', continueFrom )
        }
        catch ( e ) {
            log( 'getPages', 'No continue key' )
        }
    }
}

async function getPages () {
    if ( command.titles ) {
        log( 'Titles', command.titles )
        await batchPages()
    } else {
        wiki.nameSpaces.init( command.nameSpaces )
        for ( let nameSpace of wiki.nameSpaces ) {
            log( 'Name Space', nameSpace )
            await batchPages( nameSpace )
        }
    }
    log( '**************** done' )
}

function loadCss( dom ) {
    if (! command.css )
        return Promise.resolve()
    const css = new GlobalCss( dom )
    return css.save()
}

async function initWikiDb () {

    let dbName = osPath.join( wiki.saveDir, 'metadata.db' )

    try {
        await fs.unlink( dbName )
    } catch (err) {
    }
    wiki.db = await sqlite.open( dbName )
    return wiki.db.exec(
        'PRAGMA synchronous = OFF;' +
        //~ 'PRAGMA journal_mode = OFF;' +
        'PRAGMA journal_mode = WAL;' +

        'BEGIN;' +

        'CREATE TABLE articles (' + [
                'id INTEGER PRIMARY KEY',
                'mimeId INTEGER',
                'revision INTEGER',
                'urlKey TEXT UNIQUE',
                'titleKey TEXT',
                ].join(',') +
            ');' +
        'CREATE TABLE redirects (' +
            'id INTEGER PRIMARY KEY,' +
            'targetKey TEXT, ' +
            'fragment TEXT ' +
            ');' +
        'CREATE TABLE mimeTypes (' +
            'id INTEGER PRIMARY KEY,' +
            'value TEXT' +
            ');' +
        'CREATE TABLE continue (' +
            'id INTEGER PRIMARY KEY,' +
            '"from" TEXT' +
            ');' +

        'COMMIT;' +
        ''
    )
}

function closeMetadataStorage () {
    return wiki.db.close()
}

async function core ( samplePage ) {
    if ( command.userAgent ) {
        UserAgent = command.userAgent == 'firefox' ? UserAgentFirefox : command.userAgent
    }
    log( 'UserAgent', UserAgent )

    await loadPreRequisites( )
    const sampleDom = await processSamplePage( samplePage, command.rmdir )
    await initWikiDb()
    await loadCss( sampleDom )
    await getSiteInfo()
    await getPages()
    await saveMetadata()
    await saveMimeTypes()
    await closeMetadataStorage()
    //~ .catch( err => log( err )) // handleError
}

function main () {
    command
    .version( packageInfo.version )
    .arguments( '<wiki-page-URL>' )
    .description( `Dump a static-HTML snapshot of a MediaWiki-powered wiki.

  Where:
    wiki-page-URL \t URL of a sample page at the wiki to be dumped.
    \t\t\t This page's styling will be used as a template for all pages in the dump.` )
    .option( '-t, --titles [titles]', 'get only titles listed (separated by "|")' )
    .option( '-x, --exclude [title regexp]', 'exclude titles by a regular expression' )
    .option( '-s, --name-spaces [name-space,...]', 'name spaces to download (default: 0, i.e main)' )
    .option( '-r, --rmdir', 'delete destination directory before processing the source' )
    .option( '--no-images', "don't download images" )
    .option( '--no-css', "don't page styling" )
    .option( '--no-pages', "don't save downloaded pages" )
    .option( '-d, --no-download-errors', "ignore download errors, 404 error is ignored anyway" )
    .option( '-e, --retry-external [times]', "number of retries on external site error" )
    .option( '--user-agent [firefox or string]', "set user agent" )
    .option( '-p, --url-replace [pattern|replacement,...]', "URL replacements", ( patterns ) => {
        const repls = patterns.split( ',' )
        return repls.map( r => r.split( '|' ))
        } )
    .option( '-b, --url-blacklist [pattern|...]', "blacklisted URLs", ( patterns ) => {
        return patterns.split( '|' )
        } )
    .parse( process.argv )

    log( command.opts() )

    core( command.args[0] )
}

main ()
;
