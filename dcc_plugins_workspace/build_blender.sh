#!/usr/bin/env bash
OUT=../dcc_plugins/blender

pushd $(dirname $0)

mkdir -p $OUT

# zip blender addon
rsync -arhv --exclude=__pycache__ --exclude="*.pyc" blender/blender_path $OUT
rsync -arhv --exclude=__pycache__ --exclude="*.pyc" ../src/lifeblood_client $OUT/blender_path/addons/scripts
rsync -arhv --exclude=__pycache__ --exclude="*.pyc" ../src/lifeblood_client $OUT/blender_path/addons/lifeblood_plugin
pushd ../dcc_plugins/blender/blender_path/addons
zip -r ../../../blender_lifeblood_addon.zip lifeblood_plugin --exclude "*__pycache__/" --exclude "*.pyc"
popd

popd