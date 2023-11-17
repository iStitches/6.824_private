#!/bin/bash

ps -ef | grep test-mr.sh | awk '{lines[NR]=$0} END{for(i=1;i<NR;i++) print lines[i]}' | xargs kill -9
