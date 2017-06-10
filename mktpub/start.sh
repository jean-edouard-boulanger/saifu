#!/bin/bash
CFG_FILE_PATH=./cfg.yaml
if [[ "$MKTPUB_ENV" -eq "dev" ]]
then
    echo "[ WARN ] Will run mktpub in DEV mode."
    CFG_FILE_PATH=./cfg_dev.yaml
fi

sleep 5
python ./app.py $CFG_FILE_PATH
