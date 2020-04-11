import logging

def configure_logging():
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.ERROR)
