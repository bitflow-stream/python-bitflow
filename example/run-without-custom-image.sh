#!/bin/bash
# This script mounts the custom code instead of building a custom Docker image

docker run -ti \
    -v "$PWD:/my-step" \
    -w "/my-step" \
    bitflowstream/bitflow-pipeline-python \
    'data.csv -> python(exe-args=["-p", "example_processing_step/step.py"], step=example-step, args={intArg=42}) -> csv://-'
