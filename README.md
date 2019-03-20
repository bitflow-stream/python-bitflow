# python-bitflow
Python bitflow implementation 
Supported inputs/output:
* read/write File
* download/send from Host:Port
* Listen Input/Output (currently hardcoded buffersize [unlimited])

Further:
* Currently ownly marshalls csv like bitflow format
* first implementation of bitflow script, including most of the language functionallity (python-bitflow)

## Example
bitflow-example.py: provides a short overview about how to setup a pipeline and initialize source,sink, and processing steps.
download-data.py: downloads data from a given endpoint, filters samples
provide-data.py: reads a file and provides this file via a listen port

## TODO
* support binary bitflow format
* adopt all processing steps for bitflow script
* enable forks in bitflow script
* intensive testing
* implement capabilities argument
* automatic source recognition (removes marshaller parameter from sources)


## Known Issues:
* not all implemented processing steps are prepared to run in bitflow scripts
* sometimes a zombie process still exisits after closing python-bitflow with strg+c

