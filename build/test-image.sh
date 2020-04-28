#!/usr/bin/env sh

test $# = 1 || { echo "Need 1 parameter: image tag to test"; exit 1; }
IMAGE="bitflowstream/bitflow-pipeline-python"
TAG="$1"

# Sanity check: image starts, outputs expected line, and exits cleanly.
2>&1 docker run "$IMAGE:$TAG" 'closed://- -> python(step=echo, args={msg="hello world"})' |\
    tee /dev/stderr |\
    grep '\[python/echo\] hello world' > /dev/null
