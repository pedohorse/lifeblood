#!/bin/bash
zip -r telegram_client.pyz __main__.py telebot.py urllib3/
mv telegram_client.pyz ../../src/lifeblood/stock_nodes/telegram_client/data/.
printf "all done\n"
