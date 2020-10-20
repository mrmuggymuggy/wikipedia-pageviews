#!/usr/bin/env bash
: "${PYTHON_FILE:="/workspace/jobs/wiki_pageviews_topranks.py"}"
: "${PY_FILES:="/workspace/dist/dependencies.zip"}"

export \
  PYTHON_FILE \
  PY_FILES \

export PYTHIN_BIN=$(which python)

function ensure() {
    ## Determine if a bash variable is empty or not ##
    local env_value=$(printenv $1)
    echo "$1=$(printenv $1)"
    printenv $1 > /dev/null
    if [[ -z "$env_value" || $? -ne 0 ]]; then
        echo "$1 is unset or set to the empty string";
        exit 1;
    fi
}
#ensure important env vars are set before start
ensure APP_NAME
ensure PY_FILES
ensure PYTHON_FILE
ensure SPARK_HOME
ensure SPARK_PROPERTIES

echo "${SPARK_PROPERTIES}" > /tmp/spark.properties

set -o xtrace

$SPARK_HOME/bin/spark-submit \
--name $APP_NAME \
--packages org.apache.hadoop:hadoop-aws:3.2.0 \
--conf spark.pyspark.python=$PYTHIN_BIN \
--properties-file /tmp/spark.properties \
--py-files $PY_FILES \
$PYTHON_FILE
