# zimmer

This is a nodejs [ZIM](http://www.openzim.org/wiki/OpenZIM) file creator -- mostly a drop-in replacement for [zimwriterfs](https://github.com/wikimedia/openzim/tree/master/zimwriterfs).

A notable exception: it does't support *withFullTextIndex* option.

## Installation
* Clone *zimmer* from Github or download ZIP
* Install dependencies: `npm install`
* Symlink *zimmer* as *zimwriterfs*: `ln -s zimmer.js <some directory in $PATH>/zimwriterfs`
* Make sure genuine *zimwriterfs* is not in the $PATH

MWoffliner should pick zimmer up instead of zimwriterfs when it creates ZIM file.
