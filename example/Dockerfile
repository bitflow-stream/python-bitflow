# Example invokation:
# docker run -ti MY_IMAGE 'data.csv -> python(step=example-step, args={intArg=42}) -> csv://-'
FROM bitflowstream/bitflow-pipeline:python

WORKDIR /my-step
COPY . .
ENTRYPOINT ["/bitflow-pipeline", "-exe", "python;/python-bitflow/python-bitflow;-p /my-step/step.py"]
