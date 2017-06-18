#!/bin/bash
CFG_FILE_PATH=./cfg.yaml
START_WAIT_TIME=5
WAIT_INTERVAL=2


if [ "$MKTPUB_ENV" = "dev" ]
then
    echo "[ WARN ] Will run mktpub in DEV mode."
    CFG_FILE_PATH=./cfg_dev.yaml
fi

export CFG_FILE_PATH

if [ -z "$MKTPUB_PROCS" ]
then
    MKTPUB_PROCS=4
fi

SOURCES=$(head -n1 tickers)
TARGETS=$(tail -n1 tickers)

for SOURCE in $SOURCES
do
    for TARGET in $TARGETS
    do
        PAIRS="${SOURCE}_${TARGET} $PAIRS"
    done
done

PAIRS=( $PAIRS )

PAIRS_COUNT=${#PAIRS[@]}
POOL_SIZE=$(($PAIRS_COUNT / $MKTPUB_PROCS))

echo "[ WARN ] Will wait ${START_WAIT_TIME}s before starting"
sleep $START_WAIT_TIME

echo "[ INFO ] Starting $MKTPUB_PROCS mktpub instances (pool size: $POOL_SIZE)"

POOLS=$(echo "${PAIRS[@]}" | xargs -n$POOL_SIZE)
while IFS= read -r POOL
do
    echo "[ INFO ] Starting pool $POOL"
    python ./app.py "$CFG_FILE_PATH" $POOL &
    PIDS="$PIDS $!"
    sleep $WAIT_INTERVAL
done <<< "$POOLS"

echo "[ INFO ] Waiting for termination of all started instances"

for PID in $PIDS
do
    wait $PID
done
