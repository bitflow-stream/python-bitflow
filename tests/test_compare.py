import unittest

from bitflow import fork


class TestForkCompare(unittest.TestCase):

    # First element of tuple is the string and second is the wildcard to compare with
    wildcard_compare = [("test", "test"), ("test", "TesT"), ("test", "t*t"), ("TEST", "test"), ("1111", "1*1")]

    # First element is the string value. Second is the string to compare with
    exact_compare = [("test", "test"), ("test", "TEST"), ("TEST", "test"), ("34234", "34234"), ("test23", "test23"),
                     ("?tes_23!", "?tes_23!")]

    def test_wildcard_compare(self):
        for pair in self.wildcard_compare:
            wildcard = pair[1]
            input_string = pair[0]
            with self.subTest(msg="Checking wildcard compare of fork.", wildcard=wildcard, input_string=input_string):
                self.assertTrue(fork.wildcard_compare(wildcard, input_string))

    def test_exact_compare(self):
        for pair in self.exact_compare:
            expression = pair[1]
            input_string = pair[0]
            with self.subTest(msg="Checking exact string compare of fork.", expression=expression, input_string=input_string):
                self.assertTrue(fork.wildcard_compare(expression, input_string))


if __name__ == '__main__':
    unittest.main()
