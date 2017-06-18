#!/bin/bash

CFG_FILE_PATH=./cfg.yaml
if [ "$MKTAGG_ENV" = "dev" ]
then
    echo "[ WARN ] Will run mktagg in DEV mode."
    CFG_FILE_PATH=./cfg_dev.yaml
fi

WAIT_TIME=5
echo "[ WARN ] Waiting ${WAIT_TIME}s"
sleep $WAIT_TIME

python ./app.py $CFG_FILE_PATH
