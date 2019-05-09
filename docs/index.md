[![Build Status](https://ci.bitflow.team/jenkins/buildStatus/icon?job=Bitflow%2Fpython-bitflow%2Fmaster&build=lastBuild)](http://wally144.cit.tu-berlin.de/jenkins/blue/organizations/jenkins/Bitflow%2Fpython-bitflow/activity)

# python-bitflow
**python-bitflow** is a python library for sending, receiving and transforming streams of data. Read the documentation under [bitflow.rtfd.io](https://bitflow.readthedocs.io/en/latest/ "Bitflow documentation") to learn more about bitflow and the correlated projects.

Python-bitflow is current running in python3.7. It may run under other python3.x version but is not tested yet.

## Installation
You can install python-bitflow by running the following command. This will put the exection script python-bitflow into your $PATH and will make the bitflow classes availble within your current python environmnet.
```
python setup.py install
```
## Python-Bitflow script and usage examples
Bitflow script is a powerfull scripting language. It allows to define pipelines, processing steps (order), forks and more.  

#### Help!
Get help:
```
python-bitflow --help
```

#### Python-Bitflow, Capabilities
Get an overview of all available processing steps and forks
```
python-bitflow -capabilities
```

#### Script example 1. reading file into Noop processing step
```
python-bitflow -script "testing/testing_file_in.txt -> Noop()""
```

#### Script example 2. reading file into PlotLinePlot processing step
This will generate in .png file in the current path
```
python-bitflow -script "testing/testing_file_in.txt -> PlotLinePlot(metric_names='ongoing_connections')"
```

#### Script example 3. reading file into PlotLinePlot processing step
Reads testing file, if tag "filter" is set to "port_1935" the metric pkg_out_1300-1400 will be plottet, else if tag "filter" is set to "port_1936" the metric pkg_out_1400-1500. Afterwards both kind of samples got forwarded to a Noop() processing step.
```
python-bitflow -script "testing/testing_file_in.txt -> Fork_Tags(tag='filter'){ port_1935 -> PlotLinePlot(metric_names='pkg_out_1300-1400') ; port_1936 -> PlotLinePlot(metric_names='pkg_out_1400-1500') } -> Noop()" 
```

#### Script example 4. load processing step from external file (requires installation)
```
python-bitflow -script "testing/testing_file_in.txt -> my_processing()" -p my_processing.py
```
Current version does not close properly in all cases. Use strg-C to exit.

## Library Examples
**bitflow-example.py**: provides a short overview about how to setup a pipeline and initialize source,sink, and processing steps.

**download-data.py**: downloads data from a given endpoint, filters samples

**provide-data.py**: reads a file and provides this file via a listen port

## TODO
* support binary bitflow format
* intensive testing
* closing python-bitflow properly

## Known Issues:
* not all implemented processing steps are prepared to run in bitflow scripts
* sometimes a zombie process still exisits after closing python-bitflow with strg+c
* Forks are currently not listed in -capabilities 
