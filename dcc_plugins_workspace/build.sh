#!/bin/bash

# assume houdini environment is initialized
hotl -l houdini/otls/Driver-lifeblood_submitter-1.0.0.hda ../dcc_plugins/houdini/otls/Driver-lifeblood_submitter-1.0.0.hda

# copy client module
rsync -arhv --exclude=__pycache__ --exclude="*.pyc" ../src/lifeblood_client ../dcc_plugins/houdini/python2.7libs
rsync -arhv --exclude=__pycache__ --exclude="*.pyc" ../src/lifeblood_client ../dcc_plugins/houdini/python3.7libs
rsync -arhv --exclude=__pycache__ --exclude="*.pyc" ../src/lifeblood_client ../dcc_plugins/houdini/python3.9libs
rsync -arhv --exclude=__pycache__ --exclude="*.pyc" ../src/lifeblood_client ../dcc_plugins/blender/blender_path/addons/scripts

# post process

# zip blender addon
pushd ../dcc_plugins/blender/blender_path/addons
zip -r ../../lifeblood_addon.zip lifeblood_plugin --exclude "*__pycache__/" --exclude "*.pyc"
popd