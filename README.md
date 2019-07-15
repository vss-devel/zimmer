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

Run either of scripts with '--help' switch to see the list of all options available.

The process of creating a ZIM file from a wiki consists of 2 parts.

Example:

* Dumping a wiki to a local collection of static HTML files:

`wikizimmer https://en.wikivoyage.org/wiki/Pisa`

will dump all articles from the main name space (aka 0 or '') at the `https://en.wikivoyage.org` to the directory `en.wikivoyage.org`. The URL to a particular page is quite important in this case as this page's styling is used as a template for all other pages in the dump, so wikivoyage listings, for example, are rendered correctly at the static page of the dump.

* Building a ZIM file:

`zimmer --optimg en.wikivoyage.org`

will pack the content of the `en.wikivoyage.org` into the `en.wikivoyage.org.zim`. zimmer.js with `--optimg` option will recompress the images in the dump to save some space.

**Notes**: 
* wikizimmer.js requires a public access to the wiki's API interface.
* To dump a HTTPS server with a self-signed certificate you need to set an environment variable: `NODE_TLS_REJECT_UNAUTHORIZED=0`
* The most options of the zimmer.js are optional as it fetches the relevant metadata from the dump created by wikizimmer.js. Perhaps only `--optimg` option is rather important if you want to save some space.
