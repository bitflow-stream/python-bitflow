# teambitflow/python-bitflow
FROM python:3.7-slim

WORKDIR /opt/python-bitflow
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
RUN python setup.py install
ENTRYPOINT ["python-bitflow"]

