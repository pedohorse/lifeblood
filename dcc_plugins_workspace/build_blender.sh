# zip blender addon
rsync -arhv --exclude=__pycache__ --exclude="*.pyc" ../src/lifeblood_client ../dcc_plugins/blender/blender_path/addons/scripts
rsync -arhv --exclude=__pycache__ --exclude="*.pyc" ../src/lifeblood_client ../dcc_plugins/blender/blender_path/addons/lifeblood_plugin
pushd ../dcc_plugins/blender/blender_path/addons
zip -r ../../lifeblood_addon.zip lifeblood_plugin --exclude "*__pycache__/" --exclude "*.pyc"
popd
