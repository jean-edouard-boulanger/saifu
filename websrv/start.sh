#!/bin/bash
WAIT_TIME=20
echo "[ WARN ] Waiting ${WAIT_TIME}s"
sleep $WAIT_TIME

python ./app.py
