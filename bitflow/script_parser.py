import sys
import re
import logging
from antlr4 import *
from bitflow.BitflowLexer import BitflowLexer
from bitflow.BitflowParser import BitflowParser
from bitflow.source import FileSource, ListenSource, DownloadSource
from bitflow.sinksteps import FileSink, ListenSink, TerminalOut, TCPSink
from bitflow.pipeline import Pipeline
from bitflow.processingstep import *
from bitflow.fork import *

from bitflow.marshaller import CsvMarshaller

# listen input regex
R_port = re.compile(r'(^:[0-9]+)')
# output seperation str
output_seperation = "://"
output_type_seperation = "+"
output_formats = ["csv","bin"]

DEFAULT_FILE_DATA_FORMAT = "csv"
DEFAULT_TCP_DATA_FORMAT = "csv"
DEFAULT_STD_DATA_FORMAT = "csv"


class NotSupportedError(Exception):
    pass

class NotSupportedWarning(Exception):
    pass

class ProcessingStepNotKnow(Exception):
    pass

class ParsingError(Exception):
    pass

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
                                        marshaller=CsvMarshaller(),
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
                                    pipeline=pipeline,
                                    marshaller=CsvMarshaller())
        data_inputs.append(data_input)
    return data_inputs


def explicit_data_output(output_type, output_url):
    output_ps = None
    data_format = None

    if output_type_seperation in output_type:
        a,b = output_type.split(output_type_seperation)
        if a.lower() in output_formats:
            data_format = a.lower()
            output_type = b.lower()
        elif b.lower() in output_formats:
            data_format = b.lower()
            output_type = a.lower()
        else:
            raise ParsingError("Unable to parse {} ...".format(output_formats))

    if output_type == "file":
        if not data_format:
            data_format = DEFAULT_FILE_DATA_FORMAT
        logging.info("FileSink: " + str(output_url))
        output_ps = FileSink(   filename=output_url,
                                data_format=data_format)

    elif output_type == "listen":
        if not data_format:
            data_format = DEFAULT_TCP_DATA_FORMAT
        
        if output_url[0] is not ":":
            raise ParsingError("Missing requred \':\' in port ...")
        output_url = output_url[1:]
        port = int(output_url)
        logging.info("ListenSink: :" + output_url)

        output_ps = ListenSink( port=port,
                                data_format=data_format)
    elif output_type == "tcp":
        if not data_format:
            data_format = DEFAULT_TCP_DATA_FORMAT
        try:
            hostname,port_str = output_url.split(":")
        except:
            raise ParsingError("Unable to parse {} ...".format(output_url))
        logging.info("TCPSink: " + output_url)
        port = int(port_str)
        output_ps = TCPSink(host=hostname,
                            port=port, 
                            data_format=data_format)

    elif output_type == "std":
        if not data_format:
            data_format = DEFAULT_STD_DATA_FORMAT
        # currently ignores data_format
        logging.info("TerminalOut: " + output_url)
        output_ps = TerminalOut()

    elif output_type == "empty":
        raise NotSupportedWarning("Empty output not supported ...")

    return output_ps

def implicit_data_output(output_str):
    output_ps = None
    
    if ":" in output_str:
        if R_port.match(output_str):
            logging.info("ListenSink: " + output_str)
            port_str = output_str[1:]
            port = int(port_str)
            output_ps = ListenSink( port=port,
                                    data_format=DEFAULT_TCP_DATA_FORMAT) 

        else:
            logging.info("TCPSink: " + output_str)
            hostname,port_str = output_str.split(":")
            port = int(port_str)
            output_ps = TCPSink(host=hostname, 
                                port=port, 
                                data_format=DEFAULT_TCP_DATA_FORMAT)

    elif output_str == "-":
        logging.info("TerminalOut: " + output_str)
        output_ps = TerminalOut()

    else:
        logging.info("FileSink: " + output_str)
        output_ps = FileSink(   filename=output_str,
                                data_format=DEFAULT_FILE_DATA_FORMAT)

    return output_ps

#G4:   dataOutput : name schedulingHints? ;
def build_data_output(data_output_ctx):
    if data_output_ctx.schedulingHints():
        scheduling_hints_ctx =  data_output_ctx.schedulingHints()
        parse_scheduling_hints(scheduling_hints_ctx)
    output_ctx =  data_output_ctx.name()
    output_str = output_ctx.getText()

    if len(output_str.split(output_seperation)) == 2:
        output_type, output_url = output_str.split(output_seperation)
        output_ps = explicit_data_output(output_type=output_type.lower(),output_url=output_url.lower())
    else:
        output_ps = implicit_data_output(output_str)
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
