[![Build Status](https://ci.bitflow.team/jenkins/buildStatus/icon?job=Bitflow%2Fpython-bitflow%2Fmaster&build=lastBuild)](http://wally144.cit.tu-berlin.de/jenkins/blue/organizations/jenkins/Bitflow%2Fpython-bitflow/activity)
[![Code Coverage](https://ci.bitflow.team/sonarqube/api/project_badges/measure?project=python-bitflow&metric=coverage)](http://wally144.cit.tu-berlin.de/sonarqube/dashboard?id=python-bitflow)
[![Maintainability](https://ci.bitflow.team/sonarqube/api/project_badges/measure?project=python-bitflow&metric=sqale_rating)](http://wally144.cit.tu-berlin.de/sonarqube/dashboard?id=python-bitflow)
[![Reliability](https://ci.bitflow.team/sonarqube/api/project_badges/measure?project=python-bitflow&metric=reliability_rating)](http://wally144.cit.tu-berlin.de/sonarqube/dashboard?id=python-bitflow)

# python-bitflow
**python-bitflow** is a plugin for [`go-bitflow`](https://github.com/bitflow-stream/go-bitflow) that allows writing and executing datastream operators in Python.
Python operators can be used inside the Bitflowscript executed by `go-bitflow`.
The main `go-bitflow` dataflow graph runs as a single Go process, while each `python-bitflow` operator is executed in a separate child process that receives input data over the standard input and produces results on the standard output.

Tested Python version: 3.8

# Installation (building Docker container)

Dockerfiles are provided for the amd64 platform (based on Alpine Linux) and arm32v7.
Select one of these two as the build TARGET and run the following in the repository root:

```
TARGET=[alpine|arm32v7]
docker build -t IMAGE_NAME -f build/$TARGET.Dockerfile .
```

# Dockerhub

Docker container images are available in the [`bitflowstream/bitflow-pipeline-python`](https://hub.docker.com/repository/docker/bitflowstream/bitflow-pipeline-python) Dockerhub repository:

```
docker pull bitflowstream/bitflow-pipeline-python
docker run bitflowstream/bitflow-pipeline-python --help
```

The Docker manifest will select the appropriate platform (amd64/arm32v7) automatically.

# Usage

For the usage of the [`bitflowstream/bitflow-pipeline-python`](https://hub.docker.com/repository/docker/bitflowstream/bitflow-pipeline-python) container, see the [`go-bitflow`](https://github.com/bitflow-stream/go-bitflow) documentation.

`python-bitflow` allows to use the `python(step=NAME, args={ a1=v1, a2=v2 }, exe-args=PYTHON_ARGS)` operator.
When starting, `python-bitflow` scans for subclasses of [`bitflow.ProcessingStep`](bitflow/runner.py).
All non-abstract classes can be used as `NAME` in the `python()` operator, by default referred to through their class name.

Example:

```
docker run -p 8888 -ti bitflowstream/bitflow-pipeline-python ':8888 -> python(step=echo, args={msg=hello}) -> text://-'
```
