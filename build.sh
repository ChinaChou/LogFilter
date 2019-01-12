#!/bin/bash
cd $GOPATH/src/LogFilter/
export GOOS=linux GOARCH=amd64
go build -o ./bin/logfilter ./src/main/main.go
