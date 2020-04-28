import unittest
import bitflow.steps # Make sure step classes are loaded
from bitflow import parameters
from bitflow.runner import ProcessingStep
from tests.helpers import configure_logging

class TestParameterParsing(unittest.TestCase):

    def setUp(self):
        configure_logging()

    def instantiate_step(self, name, *args):
        return parameters.instantiate_step(name, ProcessingStep, args)

    def test_empty_parameters(self):
        self.instantiate_step("debug", "")

    def test_empty_parameters_noop(self):
        self.instantiate_step("noop", "")

    def test_parse_int_to_int(self):
        step = self.instantiate_step("debug", "int=4321")
        self.assertTrue(isinstance(step.int, int))

    def test_parse_int_to_float(self):
        step = self.instantiate_step("debug", "float=4321")
        self.assertTrue(isinstance(step.float, float))

    def test_parse_int_to_str(self):
        step = self.instantiate_step("debug", "str=4321")
        self.assertTrue(isinstance(step.str, str))

    def test_parse_int_to_bool(self):
        with self.assertRaises(parameters.ParameterParseException):
            self.instantiate_step("debug", "bool=4321")

    def test_parse_float_to_int(self):
        with self.assertRaises(parameters.ParameterParseException):
            self.instantiate_step("debug", "int=0.5")

    def test_parse_float_to_float(self):
        step = self.instantiate_step("debug", "float=0.5")
        self.assertTrue(isinstance(step.float, float))

    def test_parse_float_to_str(self):
        step = self.instantiate_step("debug", "str=0.5")
        self.assertTrue(isinstance(step.str, str))

    def test_parse_float_to_bool(self):
        with self.assertRaises(parameters.ParameterParseException):
            self.instantiate_step("debug", "boolean=0.5")

    def test_parse_str_to_int(self):
        with self.assertRaises(parameters.ParameterParseException):
            self.instantiate_step("debug", "int=str")

    def test_parse_str_to_float(self):
        with self.assertRaises(parameters.ParameterParseException):
            self.instantiate_step("debug", "float=str")

    def test_parse_str_to_str(self):
        step = self.instantiate_step("debug", "str=str")
        self.assertTrue(isinstance(step.str, str))

    def test_parse_str_to_bool(self):
        with self.assertRaises(parameters.ParameterParseException):
            self.instantiate_step("debug", "bool=str")

    def test_parse_bool_to_int(self):
        with self.assertRaises(parameters.ParameterParseException):
            self.instantiate_step("debug", "int=False")

    def test_parse_bool_to_float(self):
        with self.assertRaises(parameters.ParameterParseException):
            self.instantiate_step("debug", "float=False")

    def test_parse_bool_to_str(self):
        step = self.instantiate_step("debug", "str=False")
        self.assertTrue(isinstance(step.str, str))

    def test_parse_bool_to_bool_False(self):
        step = self.instantiate_step("debug", "bool=False")
        self.assertTrue(step.bool is False)
        self.assertTrue(isinstance(step.bool, bool))

    def test_parse_bool_to_bool_True(self):
        step = self.instantiate_step("debug", "bool=True")
        self.assertTrue(step.bool is True)
        self.assertTrue(isinstance(step.bool, bool))

    def test_parse_bool_to_bool_Yes(self):
        step = self.instantiate_step("debug", "bool=Yes")
        self.assertTrue(step.bool is True)
        self.assertTrue(isinstance(step.bool, bool))

    def test_parse_bool_to_bool_0(self):
        step = self.instantiate_step("debug", "bool=0")
        self.assertTrue(step.bool is False)
        self.assertTrue(isinstance(step.bool, bool))

    def test_unknown_processing_step(self):
        with self.assertRaises(parameters.UnknownProcessingStep):
            self.instantiate_step("abc", "bla=blub")

    class TestStep(ProcessingStep):
        step_name = "weird-step"
        def __init__(self, b:str, c, a:int = 1):
            self.a = a
            self.b = b
            self.c = c

    def test_inconsistent_params_1(self):
        step = self.instantiate_step("weird-step", "b=hello", "c=world")
        self.assertEqual(step.a, 1)
        self.assertEqual(step.b, "hello")
        self.assertEqual(step.c, "world")
    
    def test_inconsistent_params_2(self):
        with self.assertRaises(parameters.ParameterParseException):
            self.instantiate_step("weird-step", "b=hello")
        with self.assertRaises(parameters.ParameterParseException):
            self.instantiate_step("weird-step", "c=hello")

    def test_inconsistent_params_3(self):
        step = self.instantiate_step("weird-step", "a=3", "b=hello", "c=world")
        self.assertEqual(step.a, 3)
        self.assertEqual(step.b, "hello")
        self.assertEqual(step.c, "world")

if __name__ == '__main__':
    unittest.main()
