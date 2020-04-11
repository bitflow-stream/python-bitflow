# bitflowstream/bitflow-pipeline:python
FROM bitflowstream/bitflow-pipeline:latest

RUN apk --no-cache add python3

# Copy and install requirements first, to take advantage of Docker image caching
WORKDIR /python-bitflow
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .
ENV PYTHONPATH=/python-bitflow
ENTRYPOINT ["/bitflow-pipeline", "-exe", "python;/python-bitflow/python-bitflow;"]
