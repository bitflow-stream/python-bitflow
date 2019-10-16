import logging
import os

LOGGING_LEVEL = logging.ERROR

dir_path = os.path.dirname(os.path.realpath(__file__))
TESTING_IN_FILE_CSV = dir_path + "/test_data/in.csv"
TESTING_IN_FILE_BIN = dir_path + "/test_data/in.bin"
TESTING_IN_FILE_CSV_SMALL = dir_path + "/test_data/in_small.csv"
TESTING_IN_FILE_BIN_SMALL = dir_path + "/test_data/in_small.bin"

TESTING_IN_FILE_CSV_EMPTY = dir_path + "/test_data/empty.csv"
TESTING_IN_FILE_BIN_EMPTY = dir_path + "/test_data/empty.bin"

TESTING_IN_FILE_CSV_BROKEN_1 = dir_path + "/test_data/broken1.csv"
TESTING_IN_FILE_BIN_BROKEN_1 = dir_path + "/test_data/broken1.bin"
TESTING_IN_FILE_CSV_BROKEN_2 = dir_path + "/test_data/broken2.csv"
TESTING_IN_FILE_BIN_BROKEN_2 = dir_path + "/test_data/broken2.bin"
TESTING_IN_FILE_CSV_BROKEN_3 = dir_path + "/test_data/broken3.csv"
TESTING_IN_FILE_BIN_BROKEN_3 = dir_path + "/test_data/broken3.bin"

TESTING_EXPECTED_OUT_FILE_CSV_BROKEN_2_3 = dir_path + "/test_data/expected_broken23_out.csv"
TESTING_EXPECTED_BATCH_OUT_SMALL = dir_path + "/test_data/expected_batch_out_small.csv"
TESTING_EXPECTED_OUT_FILE_CSV_DOUBLE = dir_path + "/test_data/expected_double_out.csv"
TESTING_EXPECTED_OUT_FILE_BIN_DOUBLE = dir_path + "/test_data/expected_double_out.bin"

TESTING_IN_FILE_CSV_ONLY_HEADER = dir_path + "/test_data/only_header.csv"
TESTING_IN_FILE_BIN_ONLY_HEADER = dir_path + "/test_data/only_header.bin"

OUT_FILE_DIR = "/tmp"
PREFIX = "/python-bitflow-testing-"
TESTING_OUT_FILE_CSV = OUT_FILE_DIR + PREFIX + "out.csv"
TESTING_OUT_FILE_CSV_2 = OUT_FILE_DIR + PREFIX + "out2.csv"
TESTING_OUT_FILE_BIN = OUT_FILE_DIR + PREFIX + "out.bin"
TESTING_OUT_FILE_BIN_2 = OUT_FILE_DIR + PREFIX + "out2.bin"

TEST_OUT_FILES = [TESTING_OUT_FILE_CSV,
                  TESTING_OUT_FILE_CSV_2,
                  TESTING_OUT_FILE_BIN,
                  TESTING_OUT_FILE_BIN_2]


def remove_files(l):
    for f in l:
        remove_file(f)


def remove_file(f):
    if os.path.isfile(f):
        os.remove(f)
        logging.info("deleted file {} ...".format(f))


def remove_folder(d):
    if os.path.isdir(d):
        os.rmdir(d)


def create_folder(d):
    if not os.path.isdir(d):
        os.mkdir(d)


def read_file(fn):
    with open(fn, 'rb') as f:
        s = f.read()
    return s


def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1
