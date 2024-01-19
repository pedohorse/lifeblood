#!/usr/bin/env bash

OUT=../dcc_plugins/houdini

pushd $(dirname $0)

mkdir -p $OUT/otls

# assume houdini environment is initialized
hotl -l houdini/otls/Driver-lifeblood_submitter-1.0.0.hda $OUT/otls/Driver-lifeblood_submitter-1.0.0.hda

rsync -arhv houdini/presets $OUT/presets

# copy client module
rsync -arhv --exclude=__pycache__ --exclude="*.pyc" ../src/lifeblood_client $OUT/python2.7libs
rsync -arhv --exclude=__pycache__ --exclude="*.pyc" ../src/lifeblood_client $OUT/python3.7libs
rsync -arhv --exclude=__pycache__ --exclude="*.pyc" ../src/lifeblood_client $OUT/python3.9libs
rsync -arhv --exclude=__pycache__ --exclude="*.pyc" ../src/lifeblood_client $OUT/python3.10libs

pushd $OUT
rm ../houdini.zip
zip -r ../houdini.zip . --exclude "*__pycache__/" --exclude "*.pyc" --exclude "*.md"
popd

popd