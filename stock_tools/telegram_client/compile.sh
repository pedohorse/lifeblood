#!/bin/bash
pushd $(dirname "$0")

if [ -d urllib3 ]; then
  echo "cleanup"
  rm -rf urllib3
fi
pip download "urllib3>=2,<3"
unzip urllib3*.whl
rm urllib3*.whl
rm -rf urllib3*.dist-info
zip -r telegram_client.pyz __main__.py telebot.py urllib3/
mv telegram_client.pyz ../../src/lifeblood/stock_nodes/telegram_client/data/.
printf "all done\n"

popd
