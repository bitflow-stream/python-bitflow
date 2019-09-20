import pathlib
import re
from antlr4 import *

from bitflow.batch import *
from bitflow.batchprocessingstep import *
from bitflow.fork import *
from bitflow.helper import *
from bitflow.io.marshaller import *
from bitflow.io.sinksteps import FileSink, ListenSink, TerminalOut, TCPSink, get_filepath
from bitflow.io.sources import FileSource, ListenSource, DownloadSource
from bitflow.script.BitflowLexer import BitflowLexer
from bitflow.script.BitflowParser import BitflowParser

# listen input regex
R_port = re.compile(r'(^:[0-9]+)')
# output separation str
OUTPUT_SEPERATION_STRING = "://"
OUTPUT_TYPE_SEPERATION_CHAR = "+"
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

DEFAULT_FILE_DATA_FORMAT = CSV_DATA_FORMAT
DEFAULT_TCP_DATA_FORMAT = BIN_DATA_FORMAT
DEFAULT_STD_DATA_FORMAT = CSV_DATA_FORMAT

CAPABILITY_JSON_NAME_FIELD = "Name"
CAPABILITY_JSON_FORK_FIELD = "isFork"
CAPABILITY_JSON_BATCH_STEP_FIELD = "isBatch"
CAPABILITY_JSON_DESCRIPTION_FIELD = "Description"
CAPABILITY_JSON_OPTIONAL_PARM_FIELD = "OptionalParms"
CAPABILITY_JSON_REQUIRED_PARM_FIELD = "RequiredParms"

THREAD_PROCESS_ELEMENTS = []


# returns all steps as json
def capabilities():
    steps_lst = []
    processing_steps = ProcessingStep.get_all_subclasses(ProcessingStep)
    for step in processing_steps:
        step_dict = {CAPABILITY_JSON_NAME_FIELD: step.__name__}
        if issubclass(step, Fork):
            step_dict[CAPABILITY_JSON_FORK_FIELD] = True
        else:
            step_dict[CAPABILITY_JSON_FORK_FIELD] = False

        if issubclass(step, BatchProcessingStep):
            step_dict[CAPABILITY_JSON_BATCH_STEP_FIELD] = True
        else:
            step_dict[CAPABILITY_JSON_BATCH_STEP_FIELD] = False

        if step.__description__:
            step_dict[CAPABILITY_JSON_DESCRIPTION_FIELD] = step.__description__
        required_step_args = {}
        optional_step_args = {}

        get_required_and_optional_args(step=step,
                                       required_step_args=required_step_args,
                                       optional_step_args=optional_step_args)

        step_dict[CAPABILITY_JSON_REQUIRED_PARM_FIELD] = [parm for parm in required_step_args.keys()]
        step_dict[CAPABILITY_JSON_OPTIONAL_PARM_FIELD] = [parm for parm in optional_step_args.keys()]
        steps_lst.append(step_dict)

    import json
    return json.dumps(steps_lst, sort_keys=True)


# G4:   processingStep : name parameters schedulingHints? ;
def build_processing_step(processing_step_ctx):
    parameters_dict = {}
    if processing_step_ctx.schedulingHints():
        scheduling_hints_ctx = processing_step_ctx.schedulingHints()
        parse_scheduling_hints(scheduling_hints_ctx)

    name_str = processing_step_ctx.name().getText()
    parse_parameters(processing_step_ctx.parameters(), parameters_dict)
    ps = initialize_step(name_str, parameters_dict)
    if not ps:
        raise ProcessingStepNotKnown("{}: unsupported processing step ...".format(name_str))
    return ps


# G4:   dataInput : name+ schedulingHints? ;
def build_data_input(data_input_ctx, pipeline):
    if data_input_ctx.schedulingHints():
        scheduling_hints_ctx = data_input_ctx.schedulingHints()
        parse_scheduling_hints(scheduling_hints_ctx)
    file_input = None
    for i in data_input_ctx.name():
        input_str = i.getText()
        if ":" in input_str:
            if R_port.match(input_str):
                logging.info("Listen Source: " + input_str)
                port_str = input_str[1:]
                port = int(port_str)
                data_input = ListenSource(port=port, pipeline=pipeline)
                THREAD_PROCESS_ELEMENTS.append(data_input)
            else:
                logging.info("Download Source: " + input_str)
                hostname, port = input_str.split(":")
                data_input = DownloadSource(host=hostname, port=int(port), pipeline=pipeline)
                THREAD_PROCESS_ELEMENTS.append(data_input)
        elif input_str == "-":
            raise NotSupportedError("StdIn Input not supported yet ...")
        else:
            logging.info("File Source: Adding path " + input_str)
            if not file_input:
                file_input = FileSource(path=input_str, pipeline=pipeline)
            else:
                file_input.add_path(input_str)
    if file_input:
        THREAD_PROCESS_ELEMENTS.append(file_input)


def explicit_data_output(output_type, data_format, output_url):
    output_ps = None
    if not data_format:
        data_format = infer_data_format(output_url)
    if output_type == FILE_OUTPUT_TYPE:
        logging.info("FileSink: %s", output_url)
        if data_format:
            output_ps = FileSink(filename=output_url, data_format=data_format)
        else:
            output_ps = FileSink(filename=output_url)
    elif output_type == TCP_LISTEN_OUTPUT_TYPE:
        if output_url[0] is not ":":
            raise ParsingError("Missing required \':\' in port ...")
        output_url = output_url[1:]
        port = int(output_url)
        logging.info("ListenSink: %s", output_url)
        if data_format:
            output_ps = ListenSink(port=port, data_format=data_format)
        else:
            output_ps = ListenSink(port=port)
    elif output_type == TCP_SEND_OUTPUT_TYPE:
        try:
            hostname, port_str = output_url.split(":")
        except Exception as e:
            raise ParsingError("Unable to parse {} ...".format(output_url), e)
        logging.info("TCPSink: %s", output_url)
        port = int(port_str)
        if data_format:
            output_ps = TCPSink(host=hostname, port=port, data_format=data_format)
        else:
            output_ps = TCPSink(host=hostname, port=port)
    elif output_type == TERMINAL_OUTPUT_TYPE:
        logging.info("TerminalOut: %s", output_url)
        if data_format:
            output_ps = TerminalOut(data_format=data_format)
        else:
            output_ps = TerminalOut()
    elif output_type == EMPTY_OUTPUT_TYPE:
        raise NotSupportedWarning("Empty output not supported ...")
    return output_ps


def implicit_data_output(output_url, data_format=None):
    if ":" in output_url:
        if R_port.match(output_url):
            logging.info("ListenSink: " + output_url)
            port_str = output_url[1:]
            port = int(port_str)
            df = data_format if data_format else DEFAULT_TCP_DATA_FORMAT
            output_ps = ListenSink(port=port,
                                   data_format=df)
        else:
            logging.info("TCPSink: " + output_url)
            hostname, port_str = output_url.split(":")
            port = int(port_str)
            df = data_format if data_format else DEFAULT_TCP_DATA_FORMAT
            output_ps = TCPSink(host=hostname,
                                port=port,
                                data_format=df)
    elif output_url == "-":
        logging.info("TerminalOut: " + output_url)
        df = data_format if data_format else DEFAULT_STD_DATA_FORMAT
        output_ps = TerminalOut(data_format=df)
    else:
        df = infer_data_format(output_url)
        output_ps = FileSink(filename=output_url, data_format=df)
        logging.info("FileSink: " + str(get_filepath(output_url)))

    return output_ps


# parse output string before OUTPUT_TYPE_SEPARATION_CHAR, means if present the data_format and the output_type
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
        if v in SUPPORTED_DATA_FORMATS:
            data_format = v
        elif v in OUTPUT_TYPES:
            output_type = v
        else:
            raise ParsingError("Unable to parse {}, value not known ...".format(v))
    return output_type, data_format


def infer_data_format(url):
    data_format = None
    suffix = pathlib.Path(url).suffix
    if suffix:
        if suffix.lower() == ".txt" or suffix.lower() == ".csv":
            data_format = CSV_DATA_FORMAT
        elif suffix.lower() == ".bin":
            data_format = BIN_DATA_FORMAT
    else:
        data_format = BIN_DATA_FORMAT  # No file suffix is interpreted as binary file
    return data_format


# parse output string like:
# :5555
# tcp://web.de:5555
# file+csv://myfile.csv
def parse_output_str(output_str):
    if len(output_str.split(OUTPUT_SEPERATION_STRING)) == 2:
        output_type_format_str, output_url = output_str.split(OUTPUT_SEPERATION_STRING)
        output_type, data_format = parse_output_type_format_str(output_type_format_str)
        if not data_format:
            data_format = infer_data_format(output_url)
    else:
        output_url = output_str
        output_type = None
        data_format = None
    return output_type, data_format, output_url


# G4:   dataOutput : name schedulingHints? ;
def build_data_output(data_output_ctx):
    if data_output_ctx.schedulingHints():
        scheduling_hints_ctx = data_output_ctx.schedulingHints()
        parse_scheduling_hints(scheduling_hints_ctx)
    output_ctx = data_output_ctx.name()
    output_str = output_ctx.getText().lower()
    output_type, data_format, output_url = parse_output_str(output_str)

    if output_type:
        output_ps = explicit_data_output(output_type=output_type, data_format=data_format, output_url=output_url)
    else:
        output_ps = implicit_data_output(output_url=output_url, data_format=data_format)
    return output_ps


# G4:   parameters : OPEN_PARAMS (parameterList SEP?)? CLOSE_PARAMS ;
def parse_parameters(parameters_ctx, parameters_dict):
    if parameters_ctx.parameterList():
        parameter_list_ctx = parameters_ctx.parameterList()
        parse_parameter_list(parameter_list_ctx, parameters_dict)


# G4:   parameterList : parameter (SEP parameter)* ;
def parse_parameter_list(parameter_list_ctx, parameters_dict):
    for parameter_ctx in parameter_list_ctx.parameter():
        parameter_name, parameter_value = parse_parameter(parameter_ctx)
        parameters_dict[parameter_name] = parameter_value


def parse_name(name_ctx):
    if name_ctx.STRING():
        return name_ctx.STRING().getText()[1:len(name_ctx.STRING().getText()) - 1]
    return name_ctx.IDENTIFIER().getText()


# G4:   listValue : OPEN_HINTS (primitiveValue (SEP primitiveValue)*)? CLOSE_HINTS ;
def parse_list_value(list_value_ctx):
    lst = []
    for primitive_value_ctx in list_value_ctx.primitiveValue():
        lst.append(parse_name(primitive_value_ctx.name()))
    return lst


# G4:   mapValueElement : name EQ primitiveValue ;
def parse_map_value_element(map_value_element_ctx):
    key = parse_name(map_value_element_ctx.name())
    value = parse_name(map_value_element_ctx.primitiveValue().name())
    return key, value


# G4:   mapValue : OPEN (mapValueElement (SEP mapValueElement)*)? CLOSE ;
def parse_map_value(map_value_ctx):
    d = {}
    for map_value_element_ctx in map_value_ctx.mapValueElement():
        k, v = parse_map_value_element(map_value_element_ctx)
        d[k] = v
    return d


# G4:   parameter : name EQ parameterValue ;
def parse_parameter(parameter_ctx):
    parameter_name = parse_name(parameter_ctx.name())
    parameter_value = None
    if parameter_ctx.parameterValue().primitiveValue():
        parameter_value = parse_name(parameter_ctx.parameterValue().primitiveValue().name())
    elif parameter_ctx.parameterValue().listValue():
        parameter_value = parse_list_value(parameter_ctx.parameterValue().listValue())
    elif parameter_ctx.parameterValue().mapValue():
        parameter_value = parse_map_value(parameter_ctx.parameterValue().mapValue())
    return parameter_name, parameter_value


# G4:   schedulingHints : OPEN_HINTS (parameterList SEP?)? CLOSE_HINTS ;
def parse_scheduling_hints(scheduling_hints_ctx):
    raise NotSupportedWarning("Scheduling Hints are currently not supported and therefore ignored ...")


# G4:   namedSubPipeline : name+ NEXT subPipeline ;
def parse_named_subpipeline(named_subpipeline_ctx):
    names = []
    if named_subpipeline_ctx.name():
        for name_ctx in named_subpipeline_ctx.name():
            name_str = parse_name(name_ctx)
            names.append(name_str)
    subpipeline_processing_step_list = build_subpipeline(named_subpipeline_ctx.subPipeline())
    return subpipeline_processing_step_list, names


# G4:   subPipeline : pipelineTailElement (NEXT pipelineTailElement)* ;
def build_subpipeline(subpipeline_ctx):
    subpipeline_processing_step_list = []
    if subpipeline_ctx.pipelineTailElement():
        for pipeline_tail_element_ctx in subpipeline_ctx.pipelineTailElement():
            pte = parse_pipeline_tail_element(pipeline_tail_element_ctx, None)  # maybe a pipeline is required here too
            if pte:
                subpipeline_processing_step_list.append(pte)
    return subpipeline_processing_step_list


# G4:   fork : name parameters schedulingHints? OPEN namedSubPipeline (EOP namedSubPipeline)*
def build_fork(fork_ctx, pipeline):
    parameters_dict = {}
    if fork_ctx.schedulingHints():
        scheduling_hints_ctx = fork_ctx.schedulingHints()
        parse_scheduling_hints(scheduling_hints_ctx)

    name_str = parse_name(fork_ctx.name())
    parse_parameters(fork_ctx.parameters(), parameters_dict)
    fork = initialize_fork(name_str, parameters_dict)

    for named_subpipeline_ctx in fork_ctx.namedSubPipeline():
        subpipeline_processing_step_list, names = parse_named_subpipeline(named_subpipeline_ctx)
        fork.add_processing_steps(processing_steps=subpipeline_processing_step_list,
                                  names=names)
        # gives outer pipeline to the fork ps. so that after trivising fork pipeline samples go back
        # into outer pipeline. each pipeline runs in their own thread, future thread or process.
        # To keep this pipeline has must be passed
        # fork.set_root_pipeline(pipeline)
    return fork


def build_batch_processing_step(processing_step_ctx):
    parameters_dict = {}
    if processing_step_ctx.schedulingHints():
        scheduling_hints_ctx = processing_step_ctx.schedulingHints()
        parse_scheduling_hints(scheduling_hints_ctx)

    name_str = processing_step_ctx.name().getText()
    parse_parameters(processing_step_ctx.parameters(), parameters_dict)
    ps = initialize_batch_step(name_str, parameters_dict)
    if not ps:
        raise ProcessingStepNotKnown("{}: unsupported processing step ...".format(name_str))
    return ps


# G4:   batchPipeline : processingStep (NEXT processingStep)* ;
def build_batch_pipeline(batch_step, batch_step_ctx):
    if batch_step_ctx.processingStep():
        for processing_step_ctx in batch_step_ctx.processingStep():
            ps = build_batch_processing_step(processing_step_ctx)
            batch_step.add_processing_step(ps)
    return batch_step


# G4:   batch : name parameters schedulingHints? OPEN batchPipeline CLOSE ;
def build_batch(batch_ctx, pipeline):
    parameters_dict = {}
    if batch_ctx.schedulingHints():
        scheduling_hints_ctx = batch_ctx.schedulingHints()
        parse_scheduling_hints(scheduling_hints_ctx)

    parse_parameters(batch_ctx.parameters(), parameters_dict)
    name_str = parse_name(batch_ctx.name())
    batch_step = initialize_step(name_str, parameters_dict)
    batch_step_ctx = batch_ctx.batchPipeline()
    batch_step = build_batch_pipeline(batch_step, batch_step_ctx)

    THREAD_PROCESS_ELEMENTS.append(pipeline)
    return batch_step


# G4:   multiplexFork : OPEN subPipeline (EOP subPipeline)* EOP? CLOSE ;
def build_multiplex_fork(multiplex_fork_ctx):
    raise NotSupportedWarning("Multiplex-Forks are currently not supported ...")


# G4:   pipelineTailElement : pipelineElement | multiplexFork | dataOutput ;
def parse_pipeline_tail_element(pipeline_tail_element_ctx, pipeline):
    pe = None
    if pipeline_tail_element_ctx.pipelineElement():
        pipeline_element_ctx = pipeline_tail_element_ctx.pipelineElement()
        pe = parse_pipeline_element(pipeline_element_ctx, pipeline)
    elif pipeline_tail_element_ctx.multiplexFork():
        multiplex_fork_ctx = pipeline_tail_element_ctx.multiplexFork()
        pe = build_multiplex_fork(multiplex_fork_ctx)
    elif pipeline_tail_element_ctx.dataOutput():
        data_output_ctx = pipeline_tail_element_ctx.dataOutput()
        pe = build_data_output(data_output_ctx)
    return pe


# G4:   pipelineElement : processingStep | fork | batch ;
def parse_pipeline_element(pipeline_element_ctx, pipeline):
    pipeline_element = None
    if pipeline_element_ctx.processingStep():
        processing_step_ctx = pipeline_element_ctx.processingStep()
        pipeline_element = build_processing_step(processing_step_ctx)
    elif pipeline_element_ctx.fork():
        fork_ctx = pipeline_element_ctx.fork()
        pipeline_element = build_fork(fork_ctx, pipeline)
    elif pipeline_element_ctx.batch():
        batch_ctx = pipeline_element_ctx.batch()
        pipeline_element = build_batch(batch_ctx, pipeline)
    return pipeline_element


# G4:   pipeline : (dataInput | pipelineElement | OPEN pipelines CLOSE) (NEXT pipelineTailElement)* ;
def build_pipeline(pipeline_ctx):
    pipeline = Pipeline()
    if pipeline_ctx.dataInput():
        data_input_ctx = pipeline_ctx.dataInput()
        build_data_input(data_input_ctx, pipeline)

    elif pipeline_ctx.pipelineElement():
        pipeline_element_ctx = pipeline_ctx.pipelineElement()
        pe = parse_pipeline_element(pipeline_element_ctx, pipeline)
        pipeline.add_processing_step(pe)

    elif pipeline_ctx.pipelines():
        pipelines_ctx = pipeline_ctx.pipelines()
        parse_pipelines(pipelines_ctx)

    if pipeline_ctx.pipelineTailElement():
        for pipeline_tail_element_ctx in pipeline_ctx.pipelineTailElement():
            pte = parse_pipeline_tail_element(pipeline_tail_element_ctx, pipeline)
            pipeline.add_processing_step(pte)
    THREAD_PROCESS_ELEMENTS.append(pipeline)


# G4:   pipelines : pipeline (EOP pipeline)* EOP? ;
# G4:   EOP? CLOSE ;
def parse_pipelines(pipelines_ctx):
    for pipeline_ctx in pipelines_ctx.pipeline():
        build_pipeline(pipeline_ctx)


# reset global variable to so parse_script can be called more than once in a single process without confusion
def priviatize_tp_elements():
    global THREAD_PROCESS_ELEMENTS
    tp = THREAD_PROCESS_ELEMENTS
    THREAD_PROCESS_ELEMENTS = []
    return tp


def parse_script(script: str):
    input = InputStream(script)
    logging.info("ScriptParser: parsing:\n {} ".format(script))
    lexer = BitflowLexer(input)
    stream = CommonTokenStream(lexer)
    parser = BitflowParser(stream)
    ctx = parser.script()
    parse_pipelines(ctx.pipelines())

    return priviatize_tp_elements()
