import unittest

from bitflow import fork


class WildcardCompare(unittest.TestCase):

    def test_string_to_string(self):
        string = "test"
        wildcard = "test"
        self.assertTrue(fork.wildcard_compare(wildcard, string))

    def test_string_to_upper_string(self):
        string = "test"
        wildcard = "TesT"
        self.assertTrue(fork.wildcard_compare(wildcard, string))

    def test_string_to_star_string(self):
        string = "test"
        wildcard = "t*t"
        self.assertTrue(fork.wildcard_compare(wildcard, string))

    def test_upper_string_to_string(self):
        string = "TEST"
        wildcard = "test"
        self.assertTrue(fork.wildcard_compare(wildcard, string))

    def test_numbers_to_star_string(self):
        string = "1111"
        wildcard = "1*1"
        self.assertTrue(fork.wildcard_compare(wildcard, string))


class ExactCompare(unittest.TestCase):

    def test_string_to_string(self):
        string = "test"
        expression = "test"
        self.assertTrue(fork.exact_compare(expression, string))

    def test_string_to_upper_string(self):
        string = "test"
        expression = "TEST"
        self.assertTrue(fork.exact_compare(expression, string))

    def test_upper_string_to_string(self):
        string = "TEST"
        expression = "test"
        self.assertTrue(fork.exact_compare(expression, string))

    def test_number_to_number(self):
        string = "34234"
        expression = "34234"
        self.assertTrue(fork.exact_compare(expression, string))

    def test_number_string_to_number_string(self):
        string = "test23"
        expression = "test23"
        self.assertTrue(fork.exact_compare(expression, string))

    def test_string_char_to_string_char(self):
        string = "?tes_23!"
        expression = "?tes_23!"
        self.assertTrue(fork.exact_compare(expression, string))


if __name__ == '__main__':
    unittest.main()
