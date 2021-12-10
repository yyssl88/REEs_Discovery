#!/bin/bash
APP_HOME=`cd $(dirname $0)/..; pwd -P`

$APP_HOME/bin/common.sh stop
exit_code=$?
echo "Process finished with exit code $exit_code"
exit "$exit_code"