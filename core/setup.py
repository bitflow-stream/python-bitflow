#!/usr/bin/env python

from distutils.core import setup

setup(name='python-bitflow',
      version='0.1',
      description='bitflow is a stream processing framework that can be controlled through a script language.',
      author='mwallschlaeger&aacker',
      author_email='marcel.wallschlaeger@tu-berlin.de',
      url='https://github.com/bitflow-stream/python-bitflow',
      scripts=['python-bitflow', 'python-bitflow'],
      packages=['bitflow', 'bitflow/io', 'bitflow/script', 'bitflow/steps']
      )
