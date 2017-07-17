#!/bin/bash

CASSG_HOME=/home/cassghub
MTODS_HOME=$CASSG_HOME/software/mtods

execute () { 
    echo -e "\n--- `date`\n"
    cd $MTODS_HOME
    . $CASSG_HOME/.bashrc
    . .env
    ./resetter.py
    ./mtods.py -v
} > $MTODS_HOME/cronjob.log 2>&1

execute

# vim: ts=4 sts=4 sw=4 et
