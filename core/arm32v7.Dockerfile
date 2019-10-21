# teambitflow/python-bitflow:latest-arm32v7
FROM arm32v7/python:3.7-slim
# enable piwheels for arm builds
RUN echo '[global]\n\
    extra-index-url=https://www.piwheels.org/simple\n\
    '\
> /etc/pip.conf
WORKDIR /opt/python-bitflow
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
RUN python setup.py install
ENTRYPOINT ["python-bitflow"]