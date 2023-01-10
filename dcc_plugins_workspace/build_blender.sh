# zip blender addon
pushd ../dcc_plugins/blender/blender_path/addons
zip -r ../../lifeblood_addon.zip lifeblood_plugin --exclude "*__pycache__/" --exclude "*.pyc"
popd