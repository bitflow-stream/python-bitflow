import logging
import os

LOGGING_LEVEL=logging.ERROR

TESTING_IN_FILE_CSV = "in.csv"
TESTING_IN_FILE_BIN = "in.bin"

OUT_FILE_DIR="/tmp"
PREFIX = "/python-bitflow-testing-"
TESTING_OUT_FILE_CSV = OUT_FILE_DIR  + PREFIX + "out.csv"
TESTING_OUT_FILE_CSV_2 = OUT_FILE_DIR + PREFIX + "out2.csv"
TESTING_OUT_FILE_BIN = OUT_FILE_DIR + PREFIX + "out.bin"
TESTING_OUT_FILE_BIN_2 = OUT_FILE_DIR + PREFIX + "out2.bin"

TEST_OUT_FILES = [  TESTING_OUT_FILE_CSV,
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
    with open(fn,'rb') as f:
        s = f.read()
    return s

def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

