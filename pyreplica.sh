#!/bin/sh

# Standalone startup script for *nix

# verify if we are a daemon (-d argument):

if [ "x$1" != "x-d" ]; then
    # execute ourself as a "daemon" for each config file
    for CONFIG in $*;
    do
        . $CONFIG
        nohup $0 -d $CONFIG 1> $LOGFILE 2> $LOGFILE &
    done;
    exit 0
fi

# execute config file:
. $2

export DSN0="$DSN0"
export DSN1="$DSN1"

# loop forever
while :
do
    # execute pyreplica
    $SCRIPT 
    #echo wait 60 seconds if it fails
    sleep 60s
done
            
