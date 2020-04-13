# Example invokation:
# docker run -ti MY_IMAGE 'data.csv -> python(step=example-step, args={intArg=42}) -> csv://-'
FROM bitflowstream/bitflow-pipeline-python

WORKDIR /my-step
COPY . .
RUN python3 setup.py install
ENTRYPOINT ["/bitflow-pipeline", "-exe", "python;python-bitflow;-shortlog -m example_processing_step.step"]

# Alternative entrypoint that works without setup.py:
# ENTRYPOINT ["/bitflow-pipeline", "-exe", "python;python-bitflow;-shortlog -p example_processing_step/step.py"]
