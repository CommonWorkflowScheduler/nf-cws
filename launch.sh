#!/bin/bash

SCRIPT_DIR=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)

export NXF_PLUGINS_DEV=$SCRIPT_DIR/plugins
$SCRIPT_DIR/../nextflow/launch.sh "$@"
