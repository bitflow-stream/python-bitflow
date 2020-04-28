import inspect
import logging

BOOL_TRUE_STRINGS = ["true", "yes", "1", "ja", "y", "j"]
BOOL_FALSE_STRINGS = ["false", "no", "0", "nein", "n"]


class UnknownProcessingStep(Exception):
    pass


class ParameterParseException(Exception):
    pass


def instantiate_step(name, root_class, args_list):
    return instantiate_step_class(find_step_class(name, root_class), args_list)


def instantiate_step_class(step_class, args_list):
    args_dict = parse_string_dict(args_list)
    parsed_args = parse_args(step_class, args_dict)
    logging.info("Instantiating class {} with args: {}".format(step_class, args_dict))
    return step_class(**parsed_args)


def find_step_class(name, root_class):
    step_classes = collect_subclasses(root_class)
    logging.info("Found {} subclass(es) of {}: {}".format(len(step_classes), root_class,
                                                          [s.get_step_name() for s in step_classes]))
    for ps in step_classes:
        if ps.get_step_name().lower() == name.lower():
            return ps
    raise UnknownProcessingStep("Unknown processing step '{}'".format(name))


def collect_subclasses(cls):
    all_subclasses = []
    for subclass in cls.__subclasses__():
        all_subclasses.append(subclass)
        all_subclasses.extend(collect_subclasses(subclass))
    return all_subclasses


def parse_string_dict(string_list):
    # Format:[ "a=b", "c=d" ]
    result = {}
    for part in string_list:
        part = part.strip()
        if len(part) == 0:
            continue
        keyVal = part.split("=")
        if len(keyVal) != 2:
            raise ParameterParseException("Failed to parse as list of key-value pairs: {}".format(string_list))
        result[keyVal[0]] = keyVal[1]
    return result


def parse_args(step, raw_args):
    provided_args = set(raw_args.keys())

    constructor = inspect.signature(step.__init__)
    parsed_args = {}
    for i, (name, param) in enumerate(constructor.parameters.items()):
        if i == 0 and param.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
            # Skip the 'self' parameter
            continue

        typ = param.annotation if param.annotation is not inspect.Parameter.empty else str
        if name in raw_args:
            # Try to parse the string to the required type
            value = raw_args[name]
            if typ == bool:
                typ = parse_bool  # Use custom boolean parsing
            try:
                parsed_args[name] = typ(value)
            except Exception as e:
                raise ParameterParseException("Failed to parse argument {}={} to {}: {}".format(name, value, typ, e))
        elif param.default is not inspect.Parameter.empty:
            # There is a default value, therefore ignore the missing parameter
            pass
        else:
            raise ParameterParseException("Missing required parameter '{}' for processing step {}".format(name, step))
        if name in provided_args:
            provided_args.remove(name)
    if len(provided_args) > 0:
        raise ParameterParseException(
            "Unexpected parameter(s) for processing step {}: {}. Known parameters: {}".format(
                step, provided_args, constructor.parameters.keys()))
    return parsed_args


def parse_bool(string):
    string = string.lower()
    if string in BOOL_TRUE_STRINGS:
        return True
    elif string in BOOL_FALSE_STRINGS:
        return False
    raise ParameterParseException("Failed to parse '{}' to bool".format(string))
