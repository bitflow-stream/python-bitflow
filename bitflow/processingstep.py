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


StringList = typing.List[str]

def capabilities():
	''' returns all steps as json '''
	processing_steps = ProcessingStep.subclasses
	

def get_required_and_optional_args(step):
	step_args = typing.get_type_hints(step.__init__)

	if step.__init__.__defaults__:
		optional_step_args_len = len(step.__init__.__defaults__)
	else:
		optional_step_args_len = 0
	
	required_step_args = {}
	optional_step_args = {}
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
	required_step_args, optional_step_args = get_required_and_optional_args(step)
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

class ProcessingStep():
	''' Abstract ProcessingStep Class'''
	subclasses = []

	def __init__(self):
		self.__name__ = "ProcessingStep"
		self.next_step = None

	def __init_subclass__(cls, **kwargs):
		super().__init_subclass__(**kwargs)
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
		self.__name__  = "AbstractAsyncProcessingStep"

	def stop(self):
		self.is_running = False

class DebugGenerationStep(AsyncProcessingStep):
	"""example generativ processing step"""

	def __init__(self):
		super().__init__()

		self.__name__ = "Debug_ProcessingStep"
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

	def __init__(self):
		super().__init__()

	def execute(self,sample):
		self.write(sample)


class ModifyTimestamp(ProcessingStep):
	""" Modifies Timestamp of traversing samples

	start_time: in %Y-%m-%d %H:%M:%S.%f' like '2018-04-06 14:51:15.157232' 
	interval: in seconds
	
	"""
	def __init__(self,interval : int ,start_time : str = "now"):
		logging.info("ModifyTimestampProcessingStep")
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
	def __init__(self, tag : str, value : str):	
		super().__init__()
		self.tag = tag
		self.value = value

	def execute(self, sample):
		sample.add_tag(self.tag,self.value)
		self.write(sample)

class NValueSumLinePlotProcessingStep(ProcessingStep):

	def __init__(self,metric_names,tag,xlabel):
		'''
		ProcessingStep to generate sum() plot of N tags and N values
		metric_names: list of values
		tag: highlights values by tag
		xlabel: x-axes label 
		'''
		self.tag = tag
		self.metric_names = metric_names
		self.values = {}

	def __str__(self):
		return "NValueLinePlotProcessingStep"

	def execute(self,sample):
		''' executed on each sample '''
		tag = sample.get_tag(self.tag)
		
		if tag not in self.values:
			self.values[tag] = [0 for x in range(len(self.metric_names))]

		tag_element = self.values[tag]	
		for i in range(0,len(self.metric_names)):
			index = sample.header.header.index(self.metric_names[i]) # find index of metric
			tag_element[i] += float(sample.metrics[index])		
			
		self.write(sample)

	def on_close(self):
		for key,value in self.values.items():
			bp = plt.plot(value)
		
		plt.subplots_adjust(bottom=0.3)
		xticks_pos = [0.65*patch.get_width() + patch.get_xy()[0] for patch in bp]
		plt.xticks(xticks_pos, self.metric_names,  ha='right', rotation=45)
		plt.xlabel(self.xlabel)
		plt.ylabel("abs()")

		plt.savefig(self.__str__() + '.png')		
		super().on_close()

class NValueAvgLinePlotProcessingStep(ProcessingStep):

	def __init__(self,metric_names,tag,xlabel,font_size=12):
		'''
		ProcessingStep to generate avg() plot of N tags and N values
		metric_names: list of values
		tag: highlights values by tag
		xlabel: x-axes label
		fontsize: font size of axis
		'''
		self.font_size=font_size
		self.tag = tag
		self.metric_names = metric_names
		self.values = {}

	def __str__(self):
		return "NValueLinePlotProcessingStep"

	def execute(self,sample):
		''' executed on each sample '''
		tag = sample.get_tag(self.tag)
		
		if tag not in self.values:
			self.values[tag] = [[] for x in range(len(self.metric_names))]

		tag_element = self.values[tag]	
		for i in range(0,len(self.metric_names)):
			index = sample.header.header.index(self.metric_names[i]) # find index of metric
			tag_element[i].append( float(sample.metrics[index]))		
			
		self.write(sample)

	def on_close(self):
		for key,value in self.values.items():
			avg_list = []
			for v in value:
				avg_list.append(sum(v) / float(len(v)))
			bp = plt.plot(avg_list)
		

		plt.subplots_adjust(bottom=0.3)
		plt.xticks(self.metric_names,  ha='right', rotation='vertical')
		plt.xlabel(self.xlabel,fontsize=self.font_size - 4)
		plt.ylabel("average()")
		plt.savefig(self.__str__() + '.png')		
		super().on_close()


class TwoValueLinePlotProcessingStep(ProcessingStep):

	def __init__(self,x_metric,y_matric):
		'''
		ProcessingStep to generate plot of one metric against another one
		metric_name: value to plot
		tag: highlights values by tag
		'''
		self.x_metric = x_metric
		self.y_matric = y_matric

		self.values_a = []
		self.values_b = []

	def __str__(self):
		return "TwoValueLinePlotProcessingStep"

	def execute(self,sample):
		''' executed on each sample '''
		index_a = sample.header.header.index(self.x_metric) # find index of metric
		self.values_a.append(float(sample.metrics[index_a]))

		index_b = sample.header.header.index(self.y_matric) # find index of metric
		self.values_b.append(float(sample.metrics[index_b]))
		self.write(sample)

	def on_close(self):
		plt.plot(self.values_a,self.values_b)
		plt.xlabel(self.x_metric)
		plt.ylabel(self.y_matric)
		plt.ioff()
		plt.savefig(self.__str__() + "-" + self.x_metric + "-" + self.y_matric + '.png')	
		plt.close()
		super().on_close()


class SimpleLinePlot(ProcessingStep):

	def __init__(self,metric_name):
		'''
		ProcessingStep to generate plot of one metric against time
		metric_name: value to plot
		'''
		super().__init__()
		self.metric_name = metric_name
		self.values = []
		self.time = []
		self.__name__ = "SimpleLinePlot"

	def execute(self,sample):
		''' executed on each sample '''
		index = sample.header.header.index(self.metric_name	) # find index of metricq
		self.values.append(float(sample.metrics[index]))
		self.time.append(sample.get_timestamp())
		self.write(sample)

	def on_close(self):
		plt.plot(self.time,self.values)
		plt.xlabel("Time")
		plt.ylabel(self.metric_name)
		plt.ioff()
		plt.savefig("{}-{}.png".format(self.__name__,self.metric_name))	
		plt.close()
		super().on_close()



class SumBarPlotForValuesProcessingStep(ProcessingStep):

	def __init__(self,metric_names,xlabel):
		'''
		ProcessingStep to generate barplot of multiple metrics
		metric_names: list of values to abs() and barplot
		'''
		self.metric_names = metric_names
		self.values = [0 for x in range(len(self.metric_names))]
		self.xlabel = xlabel

	def __str__(self):
		return "SumBarplotForValuesProcessingStep"

	def execute(self,sample):
		''' executed on each sample '''
		for i in range(0,len(self.metric_names)):
			index = sample.header.header.index(self.metric_names[i]) # find index of metric
			self.values[i] += float(sample.metrics[index])		
		self.write(sample)

	def on_close(self):
		plt.figure(1)
		bp = plt.bar(range(0,len(self.metric_names)),self.values,align="center")
		plt.subplots_adjust(bottom=0.3)
		xticks_pos = [0.65*patch.get_width() + patch.get_xy()[0] for patch in bp]
		plt.xticks(xticks_pos, self.metric_names,  ha='right', rotation=45)
		plt.xlabel(self.xlabel)
		plt.ylabel("abs()")
		plt.savefig(self.__str__() + '.png')		
		super().on_close()


class BoxPlotSeperateByTagProcessingStep(ProcessingStep):

	DEFAULT_FORMAT = "png"

	def __init__(self,metric_name,tag,ylabel,show_xlabel=True,format="png",right_adjustment=0.95,left_adjustment=0.15,bottom_adjustment=0.3,top_adjustment=0.0,font_size=12,showfliers=True):
		'''
		ProcessingStep to generate plot of one metric
		metric_name: value to plot
		tag: highlights values by tag
		bottom_adjustment: extra size of plot on the bottom
		left_adjustment: extra size of plot on the left
		fontsize: fontsize of axis and ticks
		showfliers: ignores outliers of False 
		'''
		if format in ["png", "svg","pdf"]:
			self.format = format
		else:
			self.format = self.DEFAULT_FORMAT
		self.ylabel = ylabel
		self.show_xlabel =show_xlabel 
		self.showfliers = showfliers
		self.font_size = font_size
		
		self.right_adjustment = right_adjustment
		self.left_adjustment = left_adjustment
		self.bottom_adjustment = bottom_adjustment
		self.right_adjustment = top_adjustment

		self.metric_name = metric_name
		self.tag = tag
		self.values = {}

	def __str__(self):
		return "BoxPlotSeperateByTagProcessingStep"

	def execute(self,sample):
		''' executed on each sample '''
		index = sample.header.header.index(self.metric_name	) # find index of metricq
		
		tv = sample.get_tag(self.tag)
		if tv not in self.values:
			self.values[tv] = []
		self.values[tv].append(float(sample.metrics[index]))
		return sample

	def on_close(self):
		v_list=[]
		x_labels = []
		
		for t in tmp:
			v_list.append(self.values[t])
		x_labels = tmp
		for key,value in self.values.items():
			v_list.append(value)
			x_labels.append(key)
		matplotlib.rc('pdf', fonttype=42)
		
		plt.figure(1)
		bp = plt.boxplot(v_list,notch=0, sym='+', whis=1.5,showfliers=self.showfliers,whiskerprops={'color':'black'}, boxprops={'color':'black'} )
		plt.subplots_adjust(bottom=self.bottom_adjustment,left=self.left_adjustment,right=self.right_adjustment,top=self.top_adjustment)

		plt.xticks(range(1,len(v_list)+1),x_labels,rotation='vertical',fontsize=self.font_size)
		if self.show_xlabel:
			plt.xlabel("Tag="+self.tag,fontsize=self.font_size - 4)
		if self.ylabel == None:
			plt.ylabel(self.metric_name,fontsize=self.font_size - 4)
		else:
			try:
				plt.ylabel(self.ylabel,fontsize=self.font_size - 4)
			except:
				logging.warning("alternative y label could not be set, parameter ha to be str")
				plt.ylabel(self.metric_name,fontsize=self.font_size - 4)
		plt.savefig(self.__str__() + "-" + self.metric_name + "." + self.format,format=self.format)		
		plt.close()
		super().on_close()

class LinePlot(ProcessingStep):
	
	def __init__(self,metric_name : str ,tag : str):
		self.metric_name = metric_name
		self.tag = tag
		self.values = {}
		self.begin_time=-1

	def __str__(self):
		return "LinePlotProcessingStep"

	def execute(self,sample):
		index = sample.header.header.index(self.metric_name	)
		tag_value = sample.get_tag(self.tag)
		tv = tag_value[self.tag]
		if self.begin_time == -1:
			self.begin_time = sample.get_timestamp()
		if tv not in self.values:
			self.values[tv] = [[],[]]
		self.values[tv][0].append(sample.metrics[index])
		self.values[tv][1].append(sample.get_timestamp() - self.begin_time)
		self.write(sample)

	def on_close(self):
		for key,value in self.values.items():
			plt.plot(value[1],value[0],label=key, linewidth=2.0)
		plt.ylabel(self.metric_name)
		plt.xlabel("time")  
		legend = plt.legend(loc='upper right', fontsize='x-large')
		p1 = Process(target=plt.show())
		p1.start()
		super().on_close()
