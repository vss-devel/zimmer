#!/bin/sh
":" //# -*- mode: js -*-; exec /usr/bin/env TMPDIR=/tmp node --max-old-space-size=2000 --stack-size=42000 "$0" "$@"

// node --inspect --debug-brk

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
const Promise = require('bluebird')
const requestPromise = require('request-promise')
const sqlite = require( 'sqlite' )
const cheerio = require('cheerio')

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
let indexerDb

// https://msdn.microsoft.com/en-us/library/windows/desktop/aa365247
// just in case https://www.mediawiki.org/wiki/Manual:Page_title
const sanitizeRE = /(?:[\x00-\x1F<>:"\\\?\*]|%(?:[^0-9A-Fa-f]|[0-9A-Fa-f][^0-9A-Fa-f])|(?:[. ]$))+/g

function sanitizeFN ( name ) { // after https://github.com/pillarjs/encodeurl
    return String( name ).replace( sanitizeRE, encodeURIComponent )
    //~ return sanitizeFilename( name, { replacement: '.' })
}

function elapsedStr( from , to = Date.now()) {
    return moment.duration( to - from ).format('d[d]hh:mm:ss.SSS',{ stopTrim: "h" })
}

function log ( ...args ) {
    console.log( elapsedStr( startTime ), ... args )
}

function warn ( ...args ) {
    log( ...args )
}

function fatal ( ...args ) {
    log( ...args )
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
            if ( this.timer ) {
                clearTimeout( this.timer )
            }
            if ( this.supressTimer ) {
                clearTimeout( this.supressTimer )
            }
            this.supressTimer = setTimeout(
                () => ( this.supressTimer = false, this.reshedule()),
                query.retries * this.supressTimeout
            )
            if ( ! query.external ) {
                this.interval = this.interval * 2
            }
        }

        release () {
            this.tokenCounter --
            this.run()
        }

        retry ( query, error ) {
            this.pause( query )
            if ( ++ query.retries <= ( query.external ? retryExternal : retryLimit )) {
                this.queue.push( query )
            } else {
                query.reject( error )
            }
        }

        acquire () {
            let query
            if ( this.timer || this.supressTimer || this.tokenCounter >= maxTokens || ! ( query = this.queue.shift()))
                return false
            this.tokenCounter ++
            return query
        }

        submit ( query ) {
            return request( query )
            .catch( error => {
                const retryCause = retryStatusCodes.includes( error.statusCode ) ? error.statusCode :
                    error.cause && retryErrorCodes.includes( error.cause.code ) ? error.cause.code : false
                if ( retryCause ) {
                    log( 'retry request', interval, error.name, retryCause, query )
                    this.retry( query, error )
                    return
                }
                query.reject( error )
                return
            })
            .then( reply => {
                this.release()
                return reply ? query.resolve( reply ) : query.reject( )
            })
        }

        run () {
            let query = this.acquire()
            if ( query ) {
                this.submit( query )
                this.reshedule()
            }
        }

        append ( query, priority ) {
            return new Promise(( resolve, reject ) => {
                query.resolve = resolve
                query.reject = reject
                query.retries = 0

                if ( priority )
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

    return function ( query, priority = false ) {
        processOptions( query )
        let queue = hostQueues[ query.host ]
        if ( ! queue ) {
            queue = new Queue
            hostQueues[ query.host ] = queue
        }
        return queue.append( query , priority )
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

const wiki = {
    saveDir: null,
    apiUrl: null,
    metadata: {},
}

class WikiItem {
    constructor ( zimNameSpace, url, title ) {
        this.encoding = null
        this.revision = 0
        this.id = null
        this.loadPriority = false
        Object.assign( this, { zimNameSpace, url, title })
    }

    data () {
        return ( this.data_ !== undefined ? Promise.resolve( this.data_ ) : ( this.data_ = this.load( )))
        .then( data => ! Buffer.isBuffer( data ) || this.encoding == null
            ? data
            : iconv.decode( data, this.encoding )
            )
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

    load () {
        return http({
                url: this.urlReplacements(),
                encoding: null,
            },
            this.loadPriority
        )
        .catch( err => {
            if ( ! command.downloadErrors || err.options.external || err.statusCode == 404 ) {
                return Promise.reject( new Error( `Load error ${err.statusCode} ${err.options.uri || err.options.url}` ))
            }
            fatal( 'Load error', err.statusCode, err.options.uri || err.options.url, err.error.toString())
            return Promise.reject( new Error( 'Load error' ))
        })
        .then( resp => {
            const data = resp.body

            this.url = resp.request.href // possibly redirected
            this.headers = resp.headers
            if ( ! this.revision ) {
                const modified = this.headers[ 'last-modified' ] // "Tue, 27 Jun 2017 14:37:49 GMT"
                const dateBasedRevision = Math.round(( Date.parse( modified ) - Date.parse( '2000-01-01' )) / 1000 )
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

            return data
        })
    }

    baseName () {
        const urlp = urlconv.parse( this.url )
        const pathp = osPath.parse( urlp.pathname )
        return sanitizeFN( decodeURIComponent( pathp.base ))
    }

    localPath () {
        return  '/' + this.zimNameSpace + '/' + this.baseName()
    }

    urlKey () {
        return this.zimNameSpace + this.baseName()
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

    store ( data ) {
        if ( data == null )
            return Promise.reject( new Error( 'data == null' ))

        const savePath = wiki.saveDir + this.localPath()
        log( '+', savePath )

        return fs.outputFile( savePath, data )
        .then( () => this.localPath() )
    }

    storeMetadata ( ) {
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
            //~ log( 'storeMetadata res', res )
            this.id = res.stmt.lastID
            ++ articleCount
            return this.id
        })
    }

    process () {
        return Promise.resolve()
        .then( () => this.data())
        .then( data => this.store( data ))
        .then( () => this.storeMetadata() )
        .then( () => this.localPath() )
        .catch( err => {
            warn( 'Save error', err.name, err.message, this.url, '->', this.localPath())
            return ''
        })
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
        super( 'A', urlconv.resolve( wiki.articleBase, pageInfo.fullurl ), pageInfo.title )
        this.info = pageInfo
        this.mwId = pageInfo.pageid
        this.revision = pageInfo.lastrevid
    }

    baseName () {
        if ( this.url && this.url.startsWith( wiki.articleBase )) {
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
        this.basePath = '../'.repeat( this.baseName().split( '/' ).length - 1 )
    }

    load () {
        return super.load()
        .then( body => this.preProcess( body ))
    }

    preProcess( data, reply ) {
        let src
        try {
            src = cheerio.load( data )
        } catch ( e ) {
            log( 'cheerio.load error', e, data, reply )
            return data
        }
        const content = src( '#bodyContent' )
        const dom = cheerio.load( wiki.pageTemplate )
        dom( 'title' ).text( this.title )

        dom( '#bodyContent' ).replaceWith( content )

        // modify links
        let css = dom( '#layout-css' )
        css.attr( 'href', this.basePath + css.attr( 'href' ))

        dom( 'a' ).toArray().map( elem => {
            this.transformGeoLink( elem )
            this.transformLink( elem )
        })
        // map area links
        dom( 'area' ).toArray().map( elem => {
            this.transformLink( elem )
        })

        // remove comments
        dom( '*' ).contents().each( (i, elem) => {
            //~ log( 'comment', elem.type )
            if ( elem.type === 'comment' ) {
                dom( elem ).remove()
            }
        })

        return Promise.all( dom( 'img' ).toArray().map(
            elem => this.saveImage( elem )
        ))
        .then ( () => {
            this.mimeType = 'text/html'
            this.encoding = 'utf-8'
            const out = dom.html()
            return out
        })
        .catch( err => {
            log( err )
        })
    }

    transformLink( elem ) {
        const url = elem.attribs.href
        if (! url || url.startsWith( '#' ))
            return

        if ( url.includes( 'action=edit' )) {
            delete elem.attribs.href
            return
        }
        const link = new ArticleStub({ fullurl: url })

        const path = urlconv.parse( link.url ).pathname
        if ( ! path || path == '/' )
            return

        const baseName = link.baseName()
        if ( baseName != null ) { // local article link
            if ( path.includes( ':' )) {
                delete elem.attribs.href // block other name spaces
            } else {
                elem.attribs.href = this.basePath + baseName
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
            return

        elem.attribs.href = `geo:${lat},${lon}`
    }

    saveImage ( elem ) {
        delete elem.attribs.srcset
        let url = elem.attribs.src
        if (! url || url.startsWith( 'data:' ))
            return url
        const image = new Image( url )
        return image.process()
        .then( localPath => {
            elem.attribs.src = encodeURI( this.basePath + '..' + localPath )
        })
    }
}

class Redirect extends ArticleStub {

    constructor ( info ) {
        super( info )
        this.to = info.to
        this.toFragment = info.toFragment
    }

    data() {
        return null
    }

    mimeId () {
        return 0xffff
    }

    store () {
        return null
    }

    storeMetadata ( ) {
        return super.storeMetadata()
        .then( () => {
            const target = new ArticleStub( this.to )
            const row = [
                this.id,
                target.urlKey(),
                this.toFragment,
            ]

            log( '>', this.title || this.url, row)

            indexerDb.run(
                'INSERT INTO redirects (id, targetKey, fragment) VALUES (?,?,?)',
                row
            )
        })
    }
}

class MainPage extends Redirect {
    constructor ( ) {
        super({ fullurl: 'mainpage' })
    }
    baseName () {
        return 'mainpage'
    }
    storeMetadata ( ) {
        return apiPost({
            action: 'query',
            titles: wiki.mainPage,
            redirects: '',
            prop: 'info',
            inprop: 'url',
        })
        .then( reply => {
            Object.keys( reply.query.pages ).map( key => this.to = reply.query.pages[ key ])
            return super.storeMetadata()
        })
    }
}

class Metadata extends WikiItem {
    constructor ( url, data ) {
        super( 'M', url)
        this.mimeType = 'text/plain'
        this.data_ = data
    }
    data () {
        return this.data_
    }
}

//~ const urlCache = lru( 5000 )
const urlCache = new lru({ maxSize:500 })

class PageComponent extends WikiItem {
    baseName () {
        let name
        const urlp = urlconv.parse( this.url )
        if ( urlp.query && urlp.query.includes( '=' ) && this.mimeType ) {
            const pathp = osPath.parse( urlp.path )
            const ext = '.' + mimeTypes.extension( this.mimeType )
            name = pathp.base + ext
        } else {
            const pathp = osPath.parse( urlp.pathname )
            name = pathp.name + pathp.ext.toLowerCase()
        }
        return sanitizeFN( decodeURIComponent( name ))
    }

    process () {
        let saved = urlCache.get( this.url )
        if (! saved ) {
            saved = super.process()
            urlCache.set( this.url, saved )

            saved.then( localPath => { // do keep item's data in the cache
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
        return super.data()
    }
*/
    process () {
        if (! command.images )
            return Promise.resolve( this.localPath() )
        return super.process()
    }
}

//~ const layoutFileNames = new Set()

class StyleItem extends PageComponent {
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

class FavIcon extends StyleItem {
    constructor ( ) {
        super( wiki.info.general.logo || 'http://www.openzim.org/w/images/e/e8/OpenZIM-wiki.png' )
    }
    baseName () {
        return 'favicon'
    }
}

const cssDependencies = new Set()

class GlobalCss extends StyleItem {
    constructor ( sourceDOM ) {
        super( 'zim.css' )
        this.sourceDOM = sourceDOM
        this.mimeType = 'text/css'
    }

    load () {
        // get css stylesheets
        const cssLinks = this.sourceDOM( 'link[rel=stylesheet][media!=print]' ).toArray()
        const requests = cssLinks.map( elem => this.transformCss( elem.attribs.href ))

        const stub = osPath.resolve( module.filename, '../stub.css' )
        requests.unshift( fs.readFile( stub ))

        return Promise.all( requests )
        .then( chunks => chunks.join( '\n' ))
    }

    transformCss( cssUrl ) {
        return Promise.coroutine( function* () {
            let css = new StyleItem( cssUrl )
            const src = yield css.data()

            // collect urls using dummy replacements
            const urlre = /(url\(['"]?)([^\)]*[^\)'"])(['"]?\))/g
            const requests = []
            src.replace( urlre, ( match, start, url, end ) => {
                if ( ! url.startsWith( 'data:' )) {
                    const cssItem = new StyleItem( urlconv.resolve( cssUrl, url ))
                    requests.push( cssItem.process() )
                }
                return match
            })
            const resolvedUrls = yield Promise.all( requests )
            const transformed = src.replace( urlre, ( match, start, url, end ) => {
                const rurl = resolvedUrls.shift()
                if ( rurl == null )
                    return match
                return start + rurl.slice( 3 ) + end
            })

            const outcss = `/*
 *
 * from ${cssUrl}
 *
 */
    ${transformed}
    `
            return outcss
        }) ()
    }
}

function processSamplePage ( samplePageUrl,  rmdir) {
    return Promise.coroutine( function* () {
        const resp = yield requestPromise({
            url: encodeurl( samplePageUrl ),
            resolveWithFullResponse: true,
        })
        //~log(resp)

        // set base for further http requests
        const realUrl = resp.request.href
        http = pooledRequest( requestPromise, realUrl )

        // create download directory
        const urlp = urlconv.parse( realUrl )
        wiki.saveDir = sanitizeFN( urlp.hostname )
        if ( rmdir )
            yield fs.remove( wiki.saveDir )
        yield fs.mkdirs( wiki.saveDir )

        const dom = cheerio.load( resp.body )
        const historyLink = dom('#ca-history a').attr('href')
        //~log(resp.request.href, historyLink, urlconv.resolve(resp.request.href, historyLink))
        const parsedUrl = urlconv.parse(urlconv.resolve(resp.request.href, historyLink))
        log(parsedUrl)
        parsedUrl.search = null
        parsedUrl.hash = null
        const indexPhp = urlconv.format(parsedUrl)
        parsedUrl.pathname = parsedUrl.pathname.replace('index.php', 'api.php')

        wiki.apiUrl = urlconv.format(parsedUrl)
        log(indexPhp, wiki.apiUrl)

        return dom
    })()
}

function loadTemplate () {
    const stubPath = osPath.resolve( module.filename, '../stub.html' )
    return fs.readFile ( stubPath )
    .then( stub => (wiki.pageTemplate = stub))
}

function getSiteInfo () {
    return Promise.coroutine(function* () {
        const resp = yield api ({
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
        wiki.articleBase = info.general.base.split( wiki.articlePath )[0] + wiki.articlePath
        wiki.baseParsed = urlconv.parse( wiki.articleBase )
    }) ()
}

function saveMetadata () {

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
        Source: urlconv.resolve( wiki.articleBase, wiki.info.general.server ),
    }

    return Promise.coroutine( function * () {
        yield new MainPage().process()
        yield new FavIcon().process()

        for ( let i in metadata ) {
            yield new Metadata( i, metadata[i] ).process()
        }
    }) ()
}

function saveMimeTypes () {
    return Promise.coroutine( function * () {
        for ( let i=0, li=mimeIds.length; i < li; i++ ) {
            yield indexerDb.run(
                'INSERT INTO mimeTypes (id, value) VALUES (?,?)',
                [ i + 1, mimeIds[ i ]]
            )
        }
    }) ()
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
            if ( target.ns != 0 )
                return null
            item.to = target
            item.toFragment = rdr.tofragment
            return new Redirect( item ).process()
        })
        return Promise.all( done )
    })
}

function batchPages () {
    const pageList = command.titles
    const queryPageLimit = 500
    const queryMaxTitles = 50
    const exclude = command.exclude ?
        new RegExp( command.exclude ) :
        { test: () => false }

    return Promise.coroutine( function* () {
        const queryOpt = {
            action: 'query',
            prop: 'info',
            inprop: 'url',
        }
        if ( pageList ) {
            queryOpt.titles = pageList
        } else {
            Object.assign(
                queryOpt,
                {
                    generator: 'allpages',
                    //~ gapfilterredir: redirects ? 'redirects' : 'nonredirects' ,
                    gaplimit: queryPageLimit,
                    gapnamespace: '0',
                    rawcontinue: '',
                }
            )
        }
        let continueFrom = ''
        while ( true ) {
            yield indexerDb.run(
                'INSERT OR REPLACE INTO continue (id, "from") VALUES (1, ?)',
                [ continueFrom ]
            )
            if ( continueFrom == null )
                break

            yield indexerDb.run( 'BEGIN' )

            const resp = yield api( queryOpt )
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
                if ( parseInt( key ) < 0 ) {
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
                    log( 'x' , pageInfo.title )
                    return null
                }
                log( '#', pageInfo.title )
                return new Article( pageInfo ).process()
            })
            done.push( batchRedirects( redirects ))
            yield Promise.all( done )

            yield indexerDb.run( 'COMMIT' )

            continueFrom = null
            try {
                const continueKey = Object.keys( resp[ 'query-continue' ].allpages )[ 0 ]
                continueFrom = resp[ 'query-continue' ].allpages[ continueKey ]
                queryOpt[ continueKey ] = continueFrom
                log( '...', continueFrom )
            }
            catch ( e ) {
                log( 'getPages', 'No continue key' )
            }
        }
        log( '**************** done' )
    })()
}

function loadCss( dom ) {
    if (! command.css )
        return Promise.resolve()
    const css = new GlobalCss( dom )
    return css.process()
}

function initMetadataStorage ( samplePageDOM ) {

    let dbName = osPath.join( wiki.saveDir, 'metadata.db' )

    return fs.unlink( dbName )
    .catch( () => null )
    .then( () => sqlite.open( dbName ))
    .then( db => {
        indexerDb = db
        return indexerDb.exec(
                'PRAGMA synchronous = OFF;' +
                //~ 'PRAGMA journal_mode = OFF;' +
                'PRAGMA journal_mode = WAL;' +

                'BEGIN;' +

                'CREATE TABLE articles (' + [
                        'id INTEGER PRIMARY KEY',
                        'mimeId INTEGER',
                        'revision INTEGER',
                        'urlKey TEXT',
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
    )
    .then( () => samplePageDOM )
}

function closeMetadataStorage () {
    return indexerDb.close()
}

function core ( samplePage ) {
    if ( command.userAgent ) {
        UserAgent = command.userAgent == 'firefox' ? UserAgentFirefox : command.userAgent
    }
    log( 'UserAgent', UserAgent )

    processSamplePage( samplePage, command.rmdir )
    .then( initMetadataStorage )
    .then( loadCss )
    .then( getSiteInfo )
    .then( loadTemplate )
    .then( () => batchPages())
    .then( saveMetadata )
    .then( saveMimeTypes )
    .then( closeMetadataStorage )
    .catch( err => log( err )) // handleError
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
    .option( '-r, --rmdir', 'delete destination directory before processing the source' )
    .option( '--no-images', "don't download images" )
    .option( '--no-css', "don't page styling" )
    .option( '--no-pages', "don't save downloaded pages" )
    .option( '-d, --no-download-errors', "ignore download errors, 404 error is ignored anyway" )
    .option( '-e, --retry-external [times]', "number of retries on external site error" )
    .option( '--user-agent [firefox or string]', "set user agent" )
    .option( '-p, --url-replace [parrern|replacement,...]', "URL replacements", ( patterns ) => {
        const repls = patterns.split( ',' )
        return repls.map( r => r.split( '|' ))
        } )
    .parse( process.argv )

    log( command.opts() )

    core( command.args[0] )
}

main ()
;
