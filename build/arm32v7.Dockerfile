# bitflowstream/bitflow-pipeline-python:latest-arm32v7
# Build from repository root:
# docker build -t bitflowstream/bitflow-pipeline-python:arm32v7 -f build/arm32v7.Dockerfile .
FROM bitflowstream/bitflow-pipeline:latest-arm32v7

RUN apk --no-cache add python3

# TODO is this line still necessary?
RUN echo -e '[global]\nextra-index-url=https://www.piwheels.org/simple\n' > /etc/pip.conf

# Copy and install requirements first, to take advantage of Docker image caching
WORKDIR /python-bitflow
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .
RUN python3 setup.py install
ENTRYPOINT ["/bitflow-pipeline", "-exe", "python;python-bitflow;-shortlog"]
