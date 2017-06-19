#!/bin/bash

CFG_FILE_PATH=./cfg.yaml
if [[ "$SCHEDPRICE_ENV" -eq "dev" ]]
then
    echo "[ WARN ] Will run schedprice in DEV mode."
    CFG_FILE_PATH=./cfg_dev.yaml
fi

WAIT_TIME=20
echo "[ WARN ] Waiting ${WAIT_TIME}s"
sleep $WAIT_TIME

python ./app.py $CFG_FILE_PATH
