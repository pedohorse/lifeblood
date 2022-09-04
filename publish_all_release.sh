#!/usr/bin/env bash

cd pkg_lifeblood
./build_pypi.sh && ./upload_pypi.sh pipkey-release
cd ..

cd pkg_lifeblood_viewer
./build_pypi.sh && ./upload_pypi.sh pipkey-release
cd ..