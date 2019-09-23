import inspect
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
class CtrlMethodDecorator(type):
    def __new__(mcs, name, bases, attrs):
        for attr_name, attr_value in attrs.items():
            if isinstance(attr_value, types.FunctionType):
                if attr_name == "start":
                    attrs[attr_name] = mcs.deco(attr_value, logging.info, "starting ...", "started")
                elif attr_name == "stop":
                    attrs[attr_name] = mcs.deco(attr_value, logging.info, "stopping ...", "stopped")
                elif attr_name == "on_close":
                    attrs[attr_name] = mcs.deco(attr_value, logging.info, "closing ...", "closed")
        return super(CtrlMethodDecorator, mcs).__new__(mcs, name, bases, attrs)

    @classmethod
    def deco(mcs, func, logging_func, text_before, text_after):
        def wrapper(*args, **kwargs):
            try:
                is_method = inspect.getargspec(func)[0][0] == 'self'
            except:
                is_method = False

            if is_method:
                name = args[0].__class__.__name__
            else:
                name = "Some class"
            if text_before:
                logging_func("%s: %s", name, text_before)
            result = func(*args, **kwargs)
            if text_after:
                logging_func("%s: %s", name, text_after)
            return result
        return wrapper
