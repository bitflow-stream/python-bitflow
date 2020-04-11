# bitflowstream/bitflow-pipeline:python-arm32v7
FROM bitflowstream/bitflow-pipeline:latest-arm32v7

RUN apk --no-cache add python3

# TODO is this line still necessary?
RUN echo -e '[global]\nextra-index-url=https://www.piwheels.org/simple\n' > /etc/pip.conf

# Copy and install requirements first, to take advantage of Docker image caching
WORKDIR /python-bitflow
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .
ENV PYTHONPATH=/python-bitflow
ENTRYPOINT ["/bitflow-pipeline", "-exe", "python;/python-bitflow/python-bitflow;"]
