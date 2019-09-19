import unittest

from bitflow import helper
from bitflow.script import script_parser
from bitflow.io.marshaller import *
from tests.support import *


class TestBitflowScriptParser(unittest.TestCase):

    def test_simple_pipeline(self):
        script_parser.parse_script("DebugGenerationStep()")

    def test_ps_parse_int_to_int(self):
        tp = script_parser.parse_script("noop(float=4321)")
        self.assertTrue(isinstance(tp[0].get_processing_steps()[0].int, int))

    def test_ps_parse_int_to_float(self):
        tp = script_parser.parse_script("noop(float=4321)")
        self.assertTrue(isinstance(tp[0].get_processing_steps()[0].float, float))

    def test_ps_parse_int_to_str(self):
        tp = script_parser.parse_script("noop(str=4321)")
        self.assertTrue(isinstance(tp[0].get_processing_steps()[0].str, str))

    def test_ps_parse_int_to_bool(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("noop(bool=4321)")

    def test_ps_parse_int_to_list(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("noop(list=4321)")

    def test_ps_parse_int_to_dict(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("noop(dict=4321)")

    def test_ps_parse_float_to_int(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("noop(int=0.5)")

    def test_ps_parse_float_to_float(self):
        tp = script_parser.parse_script("noop(float=0.5)")
        self.assertTrue(isinstance(tp[0].get_processing_steps()[0].float, float))

    def test_ps_parse_float_to_str(self):
        tp = script_parser.parse_script("noop(str=0.5)")
        self.assertTrue(isinstance(tp[0].get_processing_steps()[0].str, str))

    def test_ps_parse_float_to_bool(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("noop(boolean=0.5)")

    def test_ps_parse_float_to_list(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("noop(list=0.5)")

    def test_ps_parse_float_to_dict(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("noop(dict=0.5)")

    def test_ps_parse_str_to_int(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("noop(int=str)")

    def test_ps_parse_str_to_float(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("noop(float=str)")

    def test_ps_parse_str_to_str(self):
        tp = script_parser.parse_script("noop(str=str)")
        self.assertTrue(isinstance(tp[0].get_processing_steps()[0].str, str))

    def test_ps_parse_str_to_bool(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("noop(bool=str)")

    def test_ps_parse_str_to_list(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("noop(list=str)")

    def test_ps_parse_str_to_dict(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("noop(dict=str)")

    def test_ps_parse_bool_to_int(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("noop(int=False)")

    def test_ps_parse_bool_to_float(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("noop(float=False)")

    def test_ps_parse_bool_to_str(self):
        tp = script_parser.parse_script("noop(str=False)")
        self.assertTrue(isinstance(tp[0].get_processing_steps()[0].str, str))

    def test_ps_parse_bool_to_bool_False(self):
        tp = script_parser.parse_script("noop(bool=False)")
        self.assertTrue(tp[0].get_processing_steps()[0].bool is False)
        self.assertTrue(isinstance(tp[0].get_processing_steps()[0].bool, bool))

    def test_ps_parse_bool_to_bool_True(self):
        tp = script_parser.parse_script("noop(bool=True)")
        self.assertTrue(tp[0].get_processing_steps()[0].bool is True)
        self.assertTrue(isinstance(tp[0].get_processing_steps()[0].bool, bool))

    def test_ps_parse_bool_to_bool_Yes(self):
        tp = script_parser.parse_script("noop(bool=Yes)")
        self.assertTrue(tp[0].get_processing_steps()[0].bool is True)
        self.assertTrue(isinstance(tp[0].get_processing_steps()[0].bool, bool))

    def test_ps_parse_bool_to_bool_0(self):
        tp = script_parser.parse_script("noop(bool=0)")
        self.assertTrue(tp[0].get_processing_steps()[0].bool is False)
        self.assertTrue(isinstance(tp[0].get_processing_steps()[0].bool, bool))

    def test_ps_parse_bool_to_list(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("noop(list=False)")

    def test_ps_parse_bool_to_dict(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("noop(dict=False)")

    def test_ps_parse_list_to_int(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("noop(int=[list,list])")

    def test_ps_parse_list_to_float(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("noop(float=[list,list])")

    def test_ps_parse_list_to_str(self):
        script_parser.parse_script("noop(str=[list,list])")

    def test_ps_parse_list_to_bool(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("noop(bool=[list,list])")

    def test_ps_parse_list_to_list(self):
        tp = script_parser.parse_script("noop(list=[list,list])")
        self.assertTrue(isinstance(tp[0].get_processing_steps()[0].list, list))

    def test_ps_parse_list_to_dict(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("noop(dict=[list,list])")

    def test_ps_parse_dict_to_int(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("noop(int={k=v,dict=dict})")

    def test_ps_parse_dict_to_float(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("noop(float={k=v,dict=dict})")

    def test_ps_parse_dict_to_str(self):
        script_parser.parse_script("noop(str={k=v,dict=dict})")

    def test_ps_parse_dict_to_bool(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("noop(bool={k=v,dict=dict})")

    def test_ps_parse_dict_to_list(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("noop(list={k=v,dict=dict})")

    def test_ps_parse_dict_to_dict(self):
        tp = script_parser.parse_script("noop(dict={k=v,dict=dict})")
        self.assertTrue(isinstance(tp[0].get_processing_steps()[0].dict, dict))

    def test_unknown_processing_step(self):
        with self.assertRaises(helper.ProcessingStepNotKnown):
            script_parser.parse_script("abc(bla=blub)")

    def setUp(self):
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

    def tearDown(self):
        pass


class TestBitflowScriptFormatParser(unittest.TestCase):

    def test_get_file_data_format_BIN(self):
        data_format = script_parser.infer_data_format("blacsv.bin")
        self.assertEqual(script_parser.BIN_DATA_FORMAT, data_format)

    def test_get_file_data_format_CSV(self):
        data_format = script_parser.infer_data_format("blabin.csv")
        self.assertEqual(CSV_DATA_FORMAT, data_format)

    def test_parse_output_str__data_format__filename(self):
        output_str = "csv://test.csv"
        output_type, data_format, output_url = script_parser.parse_output_str(output_str)
        self.assertEqual(output_type, None)
        self.assertEqual(data_format, CSV_DATA_FORMAT)
        self.assertEqual(output_url, "test.csv")

    def test_parse_output_str__data_type__filename(self):
        output_str = "file://test.txt"
        output_type, data_format, output_url = script_parser.parse_output_str(output_str)
        self.assertEqual(output_type, script_parser.FILE_OUTPUT_TYPE)
        # For robustness .txt is interpreted as csv data type. Complies with go-bitflow and bitflow4j.
        self.assertEqual(data_format, CSV_DATA_FORMAT)
        self.assertEqual(output_url, "test.txt")

    def test_parse_output_str__data_type__data_format__filename(self):
        output_str = "file+csv://test.csv"
        output_type, data_format, output_url = script_parser.parse_output_str(output_str)
        self.assertEqual(output_type, script_parser.FILE_OUTPUT_TYPE)
        self.assertEqual(data_format, CSV_DATA_FORMAT)
        self.assertEqual(output_url, "test.csv")

    def test_parse_output_str__data_type__host_port_url(self):
        output_str = "tcp://test:5555"
        output_type, data_format, output_url = script_parser.parse_output_str(output_str)
        self.assertEqual(output_type, script_parser.TCP_SEND_OUTPUT_TYPE)
        # For robustness, missing extensions are interpreted as binary data type. Complies with go-bitflow.
        self.assertEqual(data_format, BIN_DATA_FORMAT)
        self.assertEqual(output_url, "test:5555")

    def test_parse_output_str__data_type__data_format__host_port_url(self):
        output_str = "tcp+csv://test:5555"
        output_type, data_format, output_url = script_parser.parse_output_str(output_str)
        self.assertEqual(output_type, script_parser.TCP_SEND_OUTPUT_TYPE)
        self.assertEqual(data_format, CSV_DATA_FORMAT)
        self.assertEqual(output_url, "test:5555")

    def test_parse_output_str__to_many_identifiers(self):
        output_str = "listen+bin+csv://:5555"
        with self.assertRaises(helper.ParsingError):
            script_parser.parse_output_str(output_str)

    def test_parse_output_str__unknown_data_format(self):
        output_str = "listen+bid://:5555"
        with self.assertRaises(helper.ParsingError):
            script_parser.parse_output_str(output_str)

    def test_parse_output_str__data_format__download(self):
        output_str = "tcp+bin://web.de:5555"
        output_type, data_format, output_url = script_parser.parse_output_str(output_str)
        self.assertEqual(output_type, script_parser.TCP_SEND_OUTPUT_TYPE)
        self.assertEqual(data_format, script_parser.BIN_DATA_FORMAT)
        self.assertEqual(output_url, "web.de:5555")

    def setUp(self):
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
