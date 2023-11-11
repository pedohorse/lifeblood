#!/bin/bash

# assume houdini environment is initialized
hotl -l houdini/otls/Driver-lifeblood_submitter-1.0.0.hda ../dcc_plugins/houdini/otls/Driver-lifeblood_submitter-1.0.0.hda

# copy client module
rsync -arhv --exclude=__pycache__ --exclude="*.pyc" ../src/lifeblood_client ../dcc_plugins/houdini/python2.7libs
rsync -arhv --exclude=__pycache__ --exclude="*.pyc" ../src/lifeblood_client ../dcc_plugins/houdini/python3.7libs
rsync -arhv --exclude=__pycache__ --exclude="*.pyc" ../src/lifeblood_client ../dcc_plugins/houdini/python3.9libs
rsync -arhv --exclude=__pycache__ --exclude="*.pyc" ../src/lifeblood_client ../dcc_plugins/houdini/python3.10libs

pushd ../dcc_plugins/houdini
rm ../houdini.zip
zip -r ../houdini.zip . --exclude "*__pycache__/" --exclude "*.pyc" --exclude "*.md"
popd