import sys
import re
import logging
from antlr4 import *
from bitflow.BitflowLexer import BitflowLexer
from bitflow.BitflowParser import BitflowParser
from bitflow.source import FileSource, ListenSource, DownloadSource
from bitflow.sinksteps import FileSink, ListenSink, TerminalOut, TCPSink
from bitflow.pipeline import Pipeline
from bitflow.fork import *
from bitflow.processingstep import *
from bitflow.steps.plotprocessingsteps import *
from bitflow.fork import *
from bitflow.helper import *
from bitflow.marshaller import CsvMarshaller, BinMarshaller


# listen input regex
R_port = re.compile(r'(^:[0-9]+)')
# output seperation str
OUTPUT_SEPERATION_STRING = "://"
OUTPUT_TYPE_SEPERATION_CHAR = "+"
BINARY_DATA_FORMAT_IDENTIFIER = "bin"
CSV_DATA_FORMAT_IDENTIFIER = "csv"
DATA_FORMATS = [BINARY_DATA_FORMAT_IDENTIFIER,
                CSV_DATA_FORMAT_IDENTIFIER]
FILE_OUTPUT_TYPE = "file"
TCP_LISTEN_OUTPUT_TYPE = "listen"
TCP_SEND_OUTPUT_TYPE = "tcp"
TERMINAL_OUTPUT_TYPE = "std"
EMPTY_OUTPUT_TYPE = "empty"
OUTPUT_TYPES = [FILE_OUTPUT_TYPE,
                TCP_LISTEN_OUTPUT_TYPE,
                TCP_SEND_OUTPUT_TYPE,
                TERMINAL_OUTPUT_TYPE,
                EMPTY_OUTPUT_TYPE]

DEFAULT_FILE_DATA_FORMAT = CSV_DATA_FORMAT_IDENTIFIER
DEFAULT_TCP_DATA_FORMAT = BIN_DATA_FORMAT_IDENTIFIER
DEFAULT_STD_DATA_FORMAT = CSV_DATA_FORMAT_IDENTIFIER


def capabilities():
    ''' returns all steps as json '''

    steps_lst = []

    processing_steps = ProcessingStep.subclasses
    for step in processing_steps:
        step_dict = {}
        step_dict["Name"] = step.__name__
        if isinstance(step,Fork):
            step_dict["isFork"] = True
        else:
            step_dict["isFork"] = False
        if step.__description__:
            step_dict["Description"] = step.__description__
        required_step_args = {}
        optional_step_args = {}

        get_required_and_optional_args( step=step,
                                        required_step_args=required_step_args,
                                        optional_step_args=optional_step_args)

        step_dict["RequiredParms"] = [ parm for parm in required_step_args.keys() ]
        step_dict["OptionalParms"] = [ parm for parm in optional_step_args.keys() ]
        steps_lst.append(step_dict)

    import json
    print(json.dumps(steps_lst,sort_keys=True))


#G4:   processingStep : name parameters schedulingHints? ;
def build_processing_step(processing_step_ctx):
    parameters_dict = {}
    if processing_step_ctx.schedulingHints():
        scheduling_hints_ctx = processing_step_ctx.schedulingHints()
        parse_scheduling_hints(scheduling_hints_ctx)

    name_str = processing_step_ctx.name().getText()
    parse_parameters(processing_step_ctx.parameters(),parameters_dict)
    ps  = initialize_step(name_str,parameters_dict)
    if not ps:
        raise ProcessingStepNotKnown("{}: unsopported processing step ...".format(name_str))
    return ps

#G4:   dataInput : name+ schedulingHints? ;
def build_data_input(data_input_ctx,pipeline):
    if data_input_ctx.schedulingHints():
        scheduling_hints_ctx =  data_input_ctx.schedulingHints()
        parse_scheduling_hints(scheduling_hints_ctx)
    if len(data_input_ctx.name()) > 1:
        raise NotSupportedError("Currently only a single Data-Input is supported ...")
    data_inputs = []
    for input in data_input_ctx.name():
        input_str = input.getText()
        if ":" in input_str:
            if R_port.match(input_str):
                logging.info("Listen Source: " + input_str)
                port_str = input_str[1:]
                port = int(port_str)
                data_input = ListenSource(  port=port,
                                        pipeline=pipeline)

            else:
                logging.info("Download Source: " + input_str)
                hostname,port = input_str.split(":")
                data_input = DownloadSource(
                                host=hostname,
                                port=int(port),
                                pipeline=pipeline)

        elif input_str == "-":
            raise NotSupportedError("StdIn Input not supported yet ...")
        else:
            logging.info("File Source: " + input_str)
            data_input = FileSource(filename=input_str,
                                    pipeline=pipeline)
        data_inputs.append(data_input)
    return data_inputs


def explicit_data_output(output_type, data_format, output_url):
    output_ps = None
    data_format = None
    if output_type == FILE_OUTPUT_TYPE:
        logging.info("FileSink: " + str(FileSink.get_filepath(output_url)))
        output_ps = FileSink(   filename=output_url,
                                data_format=data_format)
    elif output_type == TCP_LISTEN_OUTPUT_TYPE:
        if output_url[0] is not ":":
            raise ParsingError("Missing requred \':\' in port ...")
        output_url = output_url[1:]
        port = int(output_url)
        logging.info("ListenSink: :" + output_url)
        output_ps = ListenSink( port=port,
                                data_format=data_format)
    elif output_type == TCP_SEND_OUTPUT_TYPE:
        try:
            hostname,port_str = output_url.split(":")
        except:
            raise ParsingError("Unable to parse {} ...".format(output_url))
        logging.info("TCPSink: " + output_url)
        port = int(port_str)
        output_ps = TCPSink(host=hostname,
                            port=port, 
                            data_format=data_format)
    elif output_type == TERMINAL_OUTPUT_TYPE:
        # currently ignores data_format
        logging.info("TerminalOut: " + output_url)
        output_ps = TerminalOut()
    elif output_type == EMPTY_OUTPUT_TYPE:
        raise NotSupportedWarning("Empty output not supported ...")
    return output_ps

def get_file_data_format(data_format,output_url):
    if data_format:
        return data_format

    if output_url.endswith(BINARY_DATA_FORMAT_IDENTIFIER):
        return BINARY_DATA_FORMAT_IDENTIFIER
    elif output_url.endswith(CSV_DATA_FORMAT_IDENTIFIER):
        return CSV_DATA_FORMAT_IDENTIFIER
    else:
        return DEFAULT_FILE_DATA_FORMAT

def implicit_data_output(output_url,data_format=None):
    output_ps = None
    
    if ":" in output_url:
        if R_port.match(output_url):
            logging.info("ListenSink: " + output_url)
            port_str = output_url[1:]
            port = int(port_str)
            df = data_format if data_format else DEFAULT_TCP_DATA_FORMAT
            output_ps = ListenSink( port=port,
                                    data_format=df) 
        else:
            logging.info("TCPSink: " + output_url)
            hostname,port_str = output_url.split(":")
            port = int(port_str)
            df = data_format if data_format else DEFAULT_TCP_DATA_FORMAT
            output_ps = TCPSink(host=hostname, 
                                port=port, 
                                data_format=df)
    elif output_url == "-":
        logging.info("TerminalOut: " + output_url)
        output_ps = TerminalOut()
    else:
        logging.info("FileSink: " + str(FileSink.get_filepath(output_url)))
        df = get_file_data_format(data_format,output_url)
        output_ps = FileSink(   filename=output_url,
                                data_format=df)
    return output_ps

# parse output string before OUTPUT_TYPE_SEPERATION_CHAR, means if present the data_format and the output_type
def parse_output_type_format_str(output_type_format_str):
    if OUTPUT_TYPE_SEPERATION_CHAR in output_type_format_str:
        values = output_type_format_str.split(OUTPUT_TYPE_SEPERATION_CHAR)
        if len(values) > 2:
            raise ParsingError("Unable to parse {}, too many values ...".format(output_type_format_str))
    else:
        values = [output_type_format_str]
    data_format = None
    output_type = None
    for v in values:
        if v in DATA_FORMATS:
            data_format = v
        elif v in OUTPUT_TYPES:
            output_type = v
        else:
             raise ParsingError("Unable to parse {}, value not known ...".format(v))
    return output_type, data_format

# parse output string like:
# :5555
# tcp://web.de:5555
# file+csv://myfile.csv
def parse_output_str(output_str):
    if len(output_str.split(OUTPUT_SEPERATION_STRING)) == 2:
        output_type_format_str, output_url = output_str.split(OUTPUT_SEPERATION_STRING)
        output_type, data_format = parse_output_type_format_str(output_type_format_str)
    else:
        output_url = output_str
        output_type = None
        data_format = None
    return output_type, data_format, output_url

#G4:   dataOutput : name schedulingHints? ;
def build_data_output(data_output_ctx):
    if data_output_ctx.schedulingHints():
        scheduling_hints_ctx =  data_output_ctx.schedulingHints()
        parse_scheduling_hints(scheduling_hints_ctx)
    output_ctx =  data_output_ctx.name()
    output_str = output_ctx.getText().lower()
    output_type, data_format, output_url = parse_output_str(output_str)

    if output_type:
        output_ps = explicit_data_output(   output_type=output_type,
                                            data_format=data_format,
                                            output_url=output_url)
    else:
        output_ps = implicit_data_output(output_url=output_url,data_format=data_format)
    return output_ps

#G4:   parameters : OPEN_PARAMS (parameterList SEP?)? CLOSE_PARAMS ;
def parse_parameters(parameters_ctx,parameters_dict):
    if parameters_ctx.parameterList():
        parameter_list_ctx = parameters_ctx.parameterList()
        parameters_dict = parse_parameter_list(parameter_list_ctx,parameters_dict)


#G4:   parameterList : parameter (SEP parameter)* ;
def parse_parameter_list(parameter_list_ctx,parameters_dict):
    for parameter_ctx in parameter_list_ctx.parameter():
        parameter_name,parameter_value = parse_parameter(parameter_ctx)
        parameters_dict[parameter_name] = parameter_value

def parse_name(name_ctx):
    if name_ctx.STRING():
        return name_ctx.STRING().getText()[1:len(name_ctx.STRING().getText())-1]
    return name_ctx.IDENTIFIER().getText()

#G4:   parameter : name EQ name ;
def parse_parameter(parameter_ctx):     
    parameter_name = parse_name(parameter_ctx.name()[0])
    parameter_value = parse_name(parameter_ctx.name()[1])
    return parameter_name,parameter_value

#G4:   schedulingHints : OPEN_HINTS (parameterList SEP?)? CLOSE_HINTS ;
def parse_scheduling_hints(scheduling_hints_ctx):
    raise NotSupportedWarning("Scheduling Hints are currently not supported and therefore ignored ...")


#G4:   namedSubPipeline : name+ NEXT subPipeline ;
def parse_named_subpipeline(named_subpipeline_ctx):
    names = []
    if named_subpipeline_ctx.name():
        for name_ctx in named_subpipeline_ctx.name():
            name_str = parse_name(name_ctx)
            names.append(name_str)
    subpipeline_processing_step_list = build_subpipeline(named_subpipeline_ctx.subPipeline())
    return subpipeline_processing_step_list,names

#G4:   subPipeline : pipelineTailElement (NEXT pipelineTailElement)* ;
def build_subpipeline(subpipeline_ctx):
    # MAYBE CHANGE IN FUTURE COMMITS
    subpipeline_processing_step_list = []
    if subpipeline_ctx.pipelineTailElement():
        for pipeline_tail_element_ctx in subpipeline_ctx.pipelineTailElement():
            pte = parse_pipeline_tail_element(pipeline_tail_element_ctx)
            subpipeline_processing_step_list.append(pte)
    return subpipeline_processing_step_list

#G4:   fork : name parameters schedulingHints? OPEN namedSubPipeline (EOP namedSubPipeline)* 
def build_fork(fork_ctx):
    parameters_dict = {}
    if fork_ctx.schedulingHints():
        scheduling_hints_ctx =  fork_ctx.schedulingHints()
        parse_scheduling_hints(scheduling_hints_ctx)
    
    name_str = parse_name(fork_ctx.name())
    parse_parameters(fork_ctx.parameters(),parameters_dict)
    fork  = initialize_fork(name_str,parameters_dict)

    for named_subpipeline_ctx in fork_ctx.namedSubPipeline():
        subpipeline_processing_step_list,names = parse_named_subpipeline(named_subpipeline_ctx)
        fork.add_processing_steps(processing_steps=subpipeline_processing_step_list,names=names)
    return fork

#G4:   window : WINDOW parameters schedulingHints? OPEN processingStep (NEXT processingStep)* CLOSE ;
def build_window(window_ctx):
    raise NotSupportedScriptError("Windows currently not supported ...")

#G4:   multiplexFork : OPEN subPipeline (EOP subPipeline)* EOP? CLOSE ;
def build_multiplex_fork(multiplex_fork_ctx):
    raise NotSupportedScriptError("Multiplex-Forks are currently not supported ...")

#G4:   pipelineTailElement : pipelineElement | multiplexFork | dataOutput ;
def parse_pipeline_tail_element(pipeline_tail_element_ctx):
    pe = None
    if pipeline_tail_element_ctx.pipelineElement():
        pipeline_element_ctx = pipeline_tail_element_ctx.pipelineElement()
        pe = parse_pipeline_element(pipeline_element_ctx)
    elif pipeline_tail_element_ctx.multiplexFork():
        multiplex_fork_ctx = pipeline_tail_element_ctx.multiplexFork()
        pe = build_multiplex_fork(multiplex_fork_ctx)
    elif pipeline_tail_element_ctx.dataOutput():
        data_output_ctx = pipeline_tail_element_ctx.dataOutput()
        pe = build_data_output(data_output_ctx)
    return pe

#G4:   pipelineElement : processingStep | fork | window ;
def parse_pipeline_element(pipeline_element_ctx):
    pipeline_element = None
    if pipeline_element_ctx.processingStep():
        processing_step_ctx = pipeline_element_ctx.processingStep()
        pipeline_element = build_processing_step(processing_step_ctx)
    elif pipeline_element_ctx.fork():
        fork_ctx = pipeline_element_ctx.fork()
        pipeline_element = build_fork(fork_ctx)
    elif pipeline_element_ctx.window():
        window_ctx = window_ctx.fork()
        pipeline_element = build_window(window_ctx)
    return pipeline_element

#G4:   pipeline : (dataInput | pipelineElement | OPEN pipelines CLOSE) (NEXT pipelineTailElement)* ;
def build_pipeline(pipeline_ctx):
    pipeline = Pipeline()
    inputs = None
    if pipeline_ctx.dataInput():
        data_input_ctx = pipeline_ctx.dataInput()
        inputs = build_data_input(data_input_ctx,pipeline)

    elif pipeline_ctx.pipelineElement():
        pipeline_element_ctx = pipeline_ctx.pipelineElement()
        pe = parse_pipeline_element(pipeline_element_ctx)
        pipeline.add_processing_step(pe)
    
    elif pipeline_ctx.pipelines():
        #pipelines_ctx = pipeline_ctx.pipelines()
        #parse_pipelines(pipelines_ctx)
        raise NotSupportedScriptError("Pipeline nesting currently not supported ...")

    if pipeline_ctx.pipelineTailElement():
        for pipeline_tail_element_ctx in pipeline_ctx.pipelineTailElement():
            pte = parse_pipeline_tail_element(pipeline_tail_element_ctx)
            pipeline.add_processing_step(pte)
    return pipeline,inputs

#G4:   pipelines : pipeline (EOP pipeline)* EOP? ;
#G4:   EOP? CLOSE ;
def parse_pipelines(pipelines_ctx):
    pipes_and_inputs = []
    for pipeline_ctx in pipelines_ctx.pipeline():
        pipeline,inputs = build_pipeline(pipeline_ctx)
        pipeline.start()
        if inputs:
            for i in inputs:
                i.start()
        pipes_and_inputs.append((pipeline,inputs))
    return pipes_and_inputs

def parse_script(script : str):
        input = InputStream(script)
        logging.info("ScriptParser: parsing:\n {} ".format(script))
        lexer = BitflowLexer(input)
        stream = CommonTokenStream(lexer)
        parser = BitflowParser(stream)
        ctx = parser.script()
        pipes_and_inputs = parse_pipelines(ctx.pipelines())
        return pipes_and_inputs
