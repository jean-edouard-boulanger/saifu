#!/bin/bash

CFG_FILE_PATH=./cfg.yaml
if [[ "$PORTPRICE_ENV" = "dev" ]]
then
    echo "[ WARN ] Will run schedprice in DEV mode."
    CFG_FILE_PATH=./cfg_dev.yaml
fi

if [[ -z "$PORTPRICE_PROCS" ]]
then
    PORTPRICE_PROCS=1
fi

WAIT_TIME=5
echo "[ WARN ] Waiting ${WAIT_TIME}s"
sleep $WAIT_TIME

echo "[ INFO ] Will start $PORTPRICE_PROCS instances of portprice"

python ./app.py $CFG_FILE_PATH
