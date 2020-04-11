# bitflowstream/python-build:debian
# docker build -t bitflowstream/python-build:debian -f build.Dockerfile .
FROM python:3.8-buster

RUN apt-get update && \
    apt-get -y install apt-transport-https ca-certificates curl gnupg2 software-properties-common && \
    curl -fsSL https://download.docker.com/linux/$(. /etc/os-release; echo "$ID")/gpg > /tmp/dkey; apt-key add /tmp/dkey && \
    add-apt-repository \
       "deb [arch=amd64] https://download.docker.com/linux/$(. /etc/os-release; echo "$ID") $(lsb_release -cs) stable" && \
    apt-get update && \
    apt-get -y install docker-ce qemu-user qemu-user-static mercurial git jq

RUN pip install pytest pytest-cov

# Enable docker-cli experimental features
RUN mkdir ~/.docker && echo '{\n\t"experimental": "enabled"\n}' > ~/.docker/config.json
