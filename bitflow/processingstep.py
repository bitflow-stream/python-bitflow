import logging
import threading
import datetime
import sys
import typing
import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import numpy as np

from bitflow.sample import Sample, Header
from bitflow.helper import *

STRING_LIST_SEPERATOR=","

StringList = typing.List[str]


def get_required_and_optional_args(step,required_step_args,optional_step_args):
	step_args = typing.get_type_hints(step.__init__)

	if step.__init__.__defaults__:
		optional_step_args_len = len(step.__init__.__defaults__)
	else:
		optional_step_args_len = 0

	for i in range(0,len(step_args) - optional_step_args_len):
		required_step_args[list(step_args.keys())[i]] = step_args[list(step_args.keys())[i]]

	for i in range(len(step_args) - optional_step_args_len,len(step_args)):
		optional_step_args[list(step_args.keys())[i]] = step_args[list(step_args.keys())[i]]

	return required_step_args,optional_step_args

def type_compare(required_type, script_value):
	if required_type is int:
		try:
			script_value = int(script_value)
		except:
			return False
	if required_type is float:
		try:
			script_value = float(script_value)
		except:
			return False
	if required_type is bool:
		try:
			script_value = bool(script_value)
		except:
			return False
	return True

def compare_args(step,script_args):
	if len(script_args) == 0:
		return  True 
	# list not tested
	required_step_args = {}
	optional_step_args = {}
	get_required_and_optional_args(	step=step,
									required_step_args=required_step_args,
									optional_step_args=optional_step_args)
	# if less than required arguments passed
	if len(script_args) < len(required_step_args):
		return False

	# compare required aruguments
	found_required_args = 0
	found_optional_args = 0 
	for i in range(len(script_args)):
		script_key_name = list(script_args.keys())[i]
		script_value = script_args[script_key_name]
		#script_value_type = type(script_value)

		if script_key_name in required_step_args.keys():
			if type_compare(required_step_args[script_key_name],script_value):
				found_required_args += 1
				logging.debug("Found required " + script_key_name + "...")


		if script_key_name in optional_step_args.keys():
			if type_compare(optional_step_args[script_key_name],script_value):
				found_optional_args += 1
				logging.debug("Found optional " + script_key_name + "...")

	# no optinal arguments passed
	if found_required_args == len(script_args):
		return True
	
	if (found_required_args + found_optional_args) == len(script_args):
		return True  

	return False

def initialize_step(name,script_args):
	processing_steps = ProcessingStep.subclasses

	for ps in processing_steps:

		if ps.__name__.lower() == name.lower():
			if compare_args(ps,script_args):
				logging.info("{} with args: {}  ok ...".format(name,script_args))				
				try:
					ps_obj = ps(**script_args)
				except Exception  as e:
					logging.error(str(e))
				return ps_obj
	logging.info("{} with args: {}  failed ...".format(name,script_args))				
	return None


def string_lst_to_lst(str_lst):
	values = str_lst.split(STRING_LIST_SEPERATOR)
	for value in values:
		value = value.strip()
	return values

SUBLCALSSES_TO_IGNORE=[	"AsyncProcessingStep",
						"Fork"]

class ProcessingStep():
	''' Abstract ProcessingStep Class'''
	subclasses = []
	__description__ = "No description written yet."


	def __init__(self):
		self.next_step = None

	def __init_subclass__(cls, **kwargs):
		super().__init_subclass__(**kwargs)
		if cls.__name__ not in SUBLCALSSES_TO_IGNORE:
			cls.subclasses.append(cls)

	def set_next_step(self,next_step):
		self.next_step = next_step

	def write(self,sample):
		if sample:
			if self.next_step:
				self.next_step.execute(sample)

	def execute(self,sample):
		raise NotImplementedError

	def stop(self):
		self.on_close()

	def on_close(self):
		logging.info("{}: closing ...".format(self.__name__))


class AsyncProcessingStep(ProcessingStep,threading.Thread):

	def __init__(self):
		ProcessingStep.__init__(self)
		threading.Thread.__init__(self)

	def stop(self):
		self.is_running = False

class DebugGenerationStep(AsyncProcessingStep):
	"""example generativ processing step"""
	__name__ = "debug-generation-step"
	__description__ = "DEBUG. Generates random samples with different tages."

	def __init__(self):
		super().__init__()

		self.is_running = True

	def execute(self,sample):
		self.write(sample)

	def run(self):
		import time
		while self.is_running:
			time.sleep(1)
			import random
			v1 = random.random()
			metrics = [str(v1)]
			header = ["random_value"]
			sample = Sample(header=Header(header=header),metrics=metrics)
			r_tag = random.randint(0,1)
			if r_tag == 0:
				sample.add_tag("blub","bla")
			else:
				sample.add_tag("blub","blub")
				sample.add_tag("test","rudolph")
			self.write(sample)

	def stop(self):
		self.is_running = False
		self.on_close()

class Noop(ProcessingStep):

	__description__ = "DEBUG. Noop."
	__name__ = "noop"

	def __init__(self):
		super().__init__()

	def execute(self,sample):
		self.write(sample)


class ModifyTimestamp(ProcessingStep):
	""" Modifies Timestamp of traversing samples
	start_time: in %Y-%m-%d %H:%M:%S.%f' like '2018-04-06 14:51:15.157232' 
	interval: in seconds
	"""
	__description__ = "Modifies Timestamp so that the first sample will get timestamp of start_time parameter"
	__name__ = "modify-timestamp"

	def __init__(self,interval : int ,start_time : str = "now"):
		try:
			self.start_time =datetime.datetime.strptime(start_time,'%Y-%m-%d %H:%M:%S.%f')
		except:
			if self.start_time is not "now":
				logging.error("{}: no correct datetime str, using now ...".format(self.__class__.__name__))
			self.start_time = datetime.datetime.now()

		try:
			self.interval = datetime.timedelta(seconds=interval)
		except Exception as  mtf:
			logging.error("Could not parse interval value to float in " + str(self) + "!\n" + str(mtf))
			self.on_close()
		super().__init__()

	def execute(self,sample):
		self.start_time = self.start_time + self.interval 
		sample.set_timestamp(self.start_time)

		self.write(sample)

class ListenForTags(ProcessingStep):
	""" Open Rest API to add or delete tags to samples

	port: port to listen on
	"""
	__description__ = "Opens a rest-interface to add tags to traversing samples"
	__name__ = "listen-for-tags"

	def __init__(self,port : int = 7777):
		from bitflow.rest import RestServer

		self.lock_all_tags = threading.Lock()
		self.all_tags = {}
		self.rest_server = RestServer(self.lock_all_tags,self.all_tags,port)
		self.rest_server.start()

	def __str__(self):
		return "ListenForTagsProcessingStep"

	def execute(self,sample):
		logging.debug("tags: " + str(self.all_tags))
		self.lock_all_tags.acquire()
		if len(self.all_tags) != 0:
			for k,v in self.all_tags.items():
				sample.add_tag(k,v)
		self.lock_all_tags.release()
		self.write(sample)


class AddTag(ProcessingStep):
	""" Adds a give tag and value to the samples

	tag: tag name
	value: value string 
	"""
	__description__ = "Adds a give tag and value to the samples"
	__name__ = "add-tag"

	def __init__(self, tag : str, value : str):	
		super().__init__()
		self.tag = tag
		self.value = value

	def execute(self, sample):
		sample.add_tag(self.tag,self.value)
		self.write(sample)
