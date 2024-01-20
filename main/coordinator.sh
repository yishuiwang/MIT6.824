#!/bin/bash

#export VERBOSE=$1
export VERBOSE=1

go build -race -o /Users/archer/project/6.5840/build/bin/coordinator /Users/archer/project/6.5840/src/main/mrcoordinator.go

echo "build coordinator successfully!"

nohup /Users/archer/project/6.5840/build/bin/coordinator /Users/archer/project/6.5840/src/main/pg-*.txt >/Users/archer/project/6.5840/build/logs/coordinator.log 2>&1 &

tail -200f /Users/archer/project/6.5840/build/logs/coordinator.log
