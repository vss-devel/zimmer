# zimmer

This is a nodejs [ZIM](http://www.openzim.org/wiki/OpenZIM) file creator -- mostly a drop-in replacement for [zimwriterfs](https://github.com/wikimedia/openzim/tree/master/zimwriterfs).

A notable exception: it does't support *withFullTextIndex* option.

## Installation
Requirement: `node` version >=6.x

* Clone *zimmer* from Github or download ZIP
* Install dependencies: `npm install`
* Make `zimmer.js` executable 

Optionaly to make it work as a replacement for *zimwriterfs*: 
* Symlink *zimmer* as *zimwriterfs*: `ln -s zimmer.js <some directory in $PATH>/zimwriterfs`
* Make sure genuine *zimwriterfs* is not in the $PATH

MWoffliner, for example, then should pick zimmer up instead of zimwriterfs when it creates ZIM file.

## Usage
```
zimmer.js [options]... HTML_DIRECTORY ZIM_FILE

Mandatory arguments:
 -w, --welcome           path of default/main HTML page. The path must be relative to HTML_DIRECTORY.
 -f, --favicon           path of ZIM file favicon. The path must be relative to HTML_DIRECTORY and the image a 48x48 PNG.
 -l, --language          language code of the content in ISO639-3
 -t, --title             title of the ZIM file
 -d, --description       short description of the content
 -c, --creator           creator(s) of the content
 -p, --publisher         creator of the ZIM file itself

 HTML_DIRECTORY          is the path of the directory containing the HTML pages you want to put in the ZIM file,
 ZIM_FILE                is the path of the ZIM file you want to obtain.

 Optional arguments:
 -v, --verbose           print processing details on STDOUT
 -h, --help              print this help
 -m, --minChunkSize      number of bytes per ZIM cluster (defaul: 4096)
 -x, --inflateHtml       try to inflate HTML files before packing (*.html, *.htm, ...)
 -u, --uniqueNamespace   put everything in the same namespace 'A'. Might be necessary to avoid problems with dynamic/javascript data loading.
 -r, --redirects         path to the CSV file with the list of redirects (url, title, target_url tab separated).
```

Example:

`./zimmer.js -t 'some title' <Path_to_your_directory> [name_of_your_zim.zim] `
