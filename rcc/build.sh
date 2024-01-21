#!/usr/bin/env bash

pushd $(dirname $0)

pyside2-rcc theme.rcc -o ../src/lifeblood_viewer/breeze_resources.py

popd