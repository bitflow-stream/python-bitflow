import logging
import types


class NotSupportedError(Exception):
    pass


class NotSupportedWarning(Exception):
    pass


class ProcessingStepNotKnown(Exception):
    pass


class ParsingError(Exception):
    pass


# Decorator class for logging method calling (before and after)
class OnCloseDeco(type):
    def __new__(mcs, name, bases, attrs):
        for attr_name, attr_value in attrs.items():
            if isinstance(attr_value, types.FunctionType) and attr_name == "on_close":
                attrs[attr_name] = mcs.deco(attr_value, bases[0].__name__)
        return super(OnCloseDeco, mcs).__new__(mcs, name, bases, attrs)

    @classmethod
    def deco(mcs, func, name):
        def wrapper(*args, **kwargs):
            logging.info("%s: closing ...", name)
            result = func(*args, **kwargs)
            logging.info("%s: closed", name)
            return result
        return wrapper
