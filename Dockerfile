#docker build -t python-bitflow:latest .
FROM python:3

ADD ./ /opt/python-bitflow
WORKDIR /opt/python-bitflow
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirments.txt

ENTRYPOINT [ "python3.6", "./bitflow.py" ]
