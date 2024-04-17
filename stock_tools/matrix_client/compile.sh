#!/bin/bash

set -e -o pipefail

pushd $(dirname "$0")

libs=("urllib3" "matrix_client" "requests" "charset_normalizer" "idna" "certifi")

for lib in ${libs[@]}; do
  if [ -d $lib ]; then
    echo "cleanup $lib"
    rm -rf $lib
  fi
done
pip download "matrix_client>=0.4,<0.5"
for lib in ${libs[@]}; do
  unzip ${lib}*.whl
  rm ${lib}*.whl
  rm -rf ${lib}*.dist-info
  rm -rf test
done

patch matrix_client/api.py matrix_client_api.py.patch

zip -r matrixclient.pyz __main__.py ${libs[@]}

# sanity check test
python matrixclient.pyz --help

mv matrixclient.pyz ../../src/lifeblood/stock_nodes/matrixclient/data/.
cp install_matrix_commander.py ../../src/lifeblood/stock_nodes/matrixclient/data/.



printf "all done\n"

popd