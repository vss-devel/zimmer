***zimmer*** is a package for creating [ZIM](http://www.openzim.org/wiki/OpenZIM) files from Mediawiki-powered wikis.

The package consists of 2 scripts:

- wikizimmer.js — dumps the wiki's articles into a collection of static HTML files.

- zimmer.js — builds a ZIM file from a static HTML files collection. Historically, zimmer.js is mostly a drop-in replacement for [zimwriterfs](https://github.com/wikimedia/openzim/tree/master/zimwriterfs) with a notable exception: it doesn't support *withFullTextIndex* option (index format is [not documented](http://www.openzim.org/wiki/ZIM_Index_Format)).

`wikizimmer.js` unlike to [mwoffliner](https://github.com/openzim/mwoffliner) does not depend on the [Parsoid](https://www.mediawiki.org/wiki/Parsoid) and [Redis](https://redis.io/) and `zimmer.js` unlike to [zimwriterfs](https://github.com/wikimedia/openzim/tree/master/zimwriterfs) doesn't depend on the [zimlib](http://www.openzim.org/wiki/Zimlib).

The package is relatively easy to install and it can even process some wikis running rather old versions of the Mediawiki engine.

## Installation
Requirement: `node` version >= 10.4.0

### With npm globally

```
npm i -g git+https://github.com/vadp/zimmer
```

or

### Manually

* Clone *zimmer* from Github or download ZIP
* Install dependencies: `npm install`
* Make `wikizimmer.js` and `zimmer.js` executable
* Optionally symlink both scripts into some directory available in your $PATH:

```
    ln -s wikizimmer.js <some directory in $PATH>/wikizimmer
    ln -s zimmer.js <some directory in $PATH>/zimmer
```

## Usage

The process of creating a ZIM file from a wiki consists of 2 parts.

Example:

* Dumping a wiki to a local collection of static HTML files:

`wikizimmer https://en.wikivoyage.org/wiki/Pisa`

 will dump all articles from the main name space (aka 0 or '') at the `https://en.wikivoyage.org` to the directory `en.wikivoyage.org`. The URL to a particular page is quite important in this case as this page's styling is used as a template for all other pages in the dump, so wikivoyage listings, for example, are rendered correctly at the static page of the dump.

* Building a ZIM file:

`zimmer --optimg en.wikivoyage.org`

will pack the content of the `en.wikivoyage.org` into the `en.wikivoyage.org.zim`. zimmer.js with `--optimg` option will recompress the images in the dump to save some space.


## Command line options

Run either of scripts with '--help' switch to see the list of all options available:

```
  Usage: wikizimmer [options] <wiki-page-URL>

  Dump a static-HTML snapshot of a MediaWiki-powered wiki.

  Where:
    wiki-page-URL    URL of a sample page at the wiki to be dumped.
                 This page's styling will be used as a template for all pages in the dump.

  Options:

    -V, --version                                output the version number
    -t, --titles [titles]                        get only titles listed (separated by "|")
    -x, --exclude [title regexp]                 exclude titles by a regular expression
    -s, --name-spaces [name-space,...]           name spaces to download (default: 0, i.e main)
    --content [selector]                         CSS selector for article content
    --remove [selector]                          CSS selector for removals in article content
    --template [file]                            non-standard article template
    --style [file or CSS]                        additional article CSS style
    --no-default-style                           don't use default CSS style
    --no-minify                                  don't minify articles
    --no-images                                  don't download images
    --no-css                                     don't page styling
    --no-pages                                   don't save downloaded pages
    --user-agent [firefox or string]             set user agent
    -d, --no-download-errors                     ignore download errors, 404 error is ignored anyway
    -e, --retry-external [times]                 number of retries on external site error
    -p, --url-replace [pattern|replacement,...]  URL replacements
    -b, --url-blacklist [pattern|...]            blacklisted URLs
    -r, --rmdir                                  delete destination directory before processing the source
    -h, --help                                   output usage information
```

```
$ zimmer -h

  Usage: zimmer [options] <source-directory> [zim-file...]

  Pack a directory into a zim file

  Where:
    source-directory     path to the directory with HTML pages to pack into a ZIM file
    zim-file             optional path for the output

  Options:

    -V, --version              output the version number
    -w, --welcome <page>       path of default/main HTML page. The path must be relative to HTML_DIRECTORY
    -f, --favicon <file>       path of ZIM file favicon. The path must be relative to HTML_DIRECTORY and the image a 48x48 PNG
    -l, --language <id>        language code of the content in ISO639-3
    -t, --title <title>        title of the ZIM file
    -d, --description <text>   short description of the content
    -c, --creator  <text>      creator(s) of the content
    -p, --publisher  <text>    creator of the ZIM file itself
    -v, --verbose              print processing details on STDOUT
    -m, --minChunkSize <size>  number of bytes per ZIM cluster (default: 2048)
    -x, --inflateHtml          try to inflate HTML files before packing (*.html, *.htm, ...)
    -u, --uniqueNamespace      put everything in the same namespace "A". Might be necessary to avoid problems with dynamic/javascript data loading
    -r, --redirects <path>     path to the CSV file with the list of redirects (url, title, target_url tab separated)
    --optimg                   optimise images
    --jpegquality <factor>     JPEG quality
    -h, --help                 output usage information
```

**NB:** The most options of the zimmer.js are optional as it fetches the relevant metadata from the dump created by wikizimmer.js. Perhaps only `--optimg` option is rather important if you want to save some space.
