#!/bin/bash

rm mr-out*
rm mr-*
rm wc.so
rm -rf mr-tmp
rm -rf info.log

# terminal
#go build -buildmode=plugin ../mrapps/wc.go

# debug
go build -race -buildmode=plugin -gcflags="all=-N -l"  ../mrapps/wc.go