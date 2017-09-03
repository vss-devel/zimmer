***zimmer*** package is primarily a tool for creating a [ZIM](http://www.openzim.org/wiki/OpenZIM) dump from a Mediawiki-based wiki.

The package consists of 2 scripts:

- wikizimmer.js dumps the wiki's articles (name space 0) into a collection of static HTML files.

- zimmer.js builds a ZIM file from a static HTML files collection. Historically, zimmer.js is mostly a drop-in replacement for [zimwriterfs](https://github.com/wikimedia/openzim/tree/master/zimwriterfs) with a notable exception: it doesn't support *withFullTextIndex* option (index format is [not documented](http://www.openzim.org/wiki/ZIM_Index_Format)).

The major point is that `wikizimmer.js` unlikely to [mwoffliner](https://github.com/openzim/mwoffliner) doesn't depend on the  [Parsoid](https://www.mediawiki.org/wiki/Parsoid) and [Redis](https://redis.io/) and `zimmer.js` unlikely to [zimwriterfs](https://github.com/wikimedia/openzim/tree/master/zimwriterfs) doesn't depend on the [zimlib](http://www.openzim.org/wiki/Zimlib).

The package is relatively easy to install and it can even process some wikis running rather old versions of the Mediawiki engine.

## Installation
Requirement: `node` version >= 6.x.

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

* Dumping a with to a local collection of static HTML files:

`wikizimmer https://en.wikivoyage.org/wiki/Pisa`

 will dump ***all*** `https://en.wikivoyage.org` articles to the directory `en.wikivoyage.org`. The URL to a particular page is quite important in this case as this page's styling is used as a template for all other pages in the dump, so wikivoyage listings, for example, are rendered correctly at the static page of the dump.

* Building a ZIM file:

`zimmer --optimg en.wikivoyage.org`

will pack the content of the `en.wikivoyage.org` into the `en.wikivoyage.org.zim`. zimmer.js with `--optimg` option will recompress the images in the dump to save some space.


## Command line options

Run either of scripts with '--help' switch to see the list of all options available:

```
$ wikizimmer -h

  Usage: wikizimmer [options] <wiki-page-URL>

  Dump a static-HTML snapshot of a MediaWiki-powered wiki.

  Where:
    wiki-page-URL        URL of a sample page at the wiki to be dumped.
                         This page's styling will be used as a template for all pages in the dump.

  Options:

    -V, --version  output the version number
    -t, --titles   get only titles listed (separated by "|")
    -r, --rmdir    delete destination directory before processing the source
    -noimages      don't download images
    -nocss         don't page styling
    -nopages       don't save downloaded pages
    -h, --help     output usage information
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

**NB:** The most options of the zimmer.js are really optional if it's used in combination with wikizimmer.js as the later one saves the relevant metadata into the dump directory. Perhaps only `--optimg` is quite important one if you want to save some space.
