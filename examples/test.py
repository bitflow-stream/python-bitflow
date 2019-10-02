#!/usr/bin/env python3

import logging

from bitflow.io.sinksteps import TerminalOut
from bitflow.io.sources import FileSource
from bitflow.pipeline import Pipeline
from bitflow.processingstep import PARALLEL_MODE_PROCESS
from bitflow.steps.plotprocessingsteps import PlotLinePlot, multiprocessing

class W:

    def __init__(self, queue):
        self.queue = queue

    def write(self, data):
        self.queue.put(data)

    def read(self):
        data = self.queue.get()
        self.queue.task_done()
        return data

class A(multiprocessing.Process):

    def __init__(self, w):
        super().__init__()
        self.w = w

    def run(self):
        while self.w.queue.qsize() > 0:
            print(self.w.read())
        print("A")


class B(multiprocessing.Process):

    def __init__(self):
        super().__init__()
        self.param = None
        self.w = W(multiprocessing.JoinableQueue())

    def on_start(self):
        self.w.write("test")
        self.param = A(self.w)

    def run(self):
        self.on_start()
        self.param.start()
        print("B")


class C:
    def __init__(self):
        self.b = B()

    def start(self):
        self.b.start()
        print("C")

    def join(self):
        self.b.join()


if __name__ == '__main__':
    c = C()
    c.start()
    c.join()
