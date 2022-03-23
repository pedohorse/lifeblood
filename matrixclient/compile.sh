#!/bin/bash
zip -r matrixclient.pyz __main__.py matrix_client/ requests/ urllib3/
mv matrixclient.pyz ../src/lifeblood/stock_nodes/matrixclient/data/.
printf "all done\n"
