import logging
from bitflow.rest import RestServer
import threading, datetime,sys
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import tensorflow as tf

class ProcessingStep():
	''' Abstract ProcessingStep Class'''

	def __init__(self):
		logging.info("{}: initialized ...".format(str(self)))

	def __str__(self):
		return "ProcessingStep"

	def execute(self,sample):
		raise NotImplementedError()

	def on_close(self):
		logging.info("closing ProcessingStep " + str(self))

class NoopProcessingStep(ProcessingStep):

	def execute(self,sample):
		return sample


class ModifyTimestampProcessingStep(ProcessingStep):
	''' expect 
	start_time in %Y-%m-%d %H:%M:%S.%f' like '2018-04-06 14:51:15.157232' 
	interval in seconds
	'''
	def __init__(self,interval,start_time=None):
		logging.info("ModifyTimestampProcessingStep")
		if start_time == None:
			self.start_time = datetime.datetime.now()
		else:
			self.start_time =	datetime.datetime.strptime(start_time,'%Y-%m-%d %H:%M:%S.%f')
		try:
			self.interval = datetime.timedelta(seconds=interval)
		except Exception as  mtf:
			logging.error("Could not parse interval value to float in " + str(self) + "!\n" + str(mtf))
			self.on_close()
		super().__init__()

	def __str__(self):
		return "ModifyTimestampProcessingStep"

	def execute(self,sample):
		self.start_time = self.start_time + self.interval 
		sample.set_timestamp(self.start_time)

		return sample

class ExcludeMetricByNameProcessingStep(ProcessingStep):

	def __init__(self,exclude_list):
		self.exclude_list = exclude_list
		logging.info("ProcessingStep Metrics "+ str(self.exclude_list))

	def __str__(self):
		return "ExcludeMetricByNameProcessingStep"

	def execute(self,sample):

		for to_exclude in self.exclude_list:
			try:
				index = sample.get_metricsindex_by_name(to_exclude)
				if index != None:
					sample.remove_metrics(index)
			except:
				continue
		return sample

class ListenForTagsProcessingStep(ProcessingStep):
	
	def __init__(self,port):
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

		return sample

class AddTagProcessingStep(ProcessingStep):
	
	def __init__(self,tag,value):	
		self.tag = tag
		self.value = value

	def __str__(self):
		return "AddTagilter"

	def execute(self, sample):
		sample.add_tag(self.tag,self.value)
		return sample

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
			
		return sample

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
			
		return sample

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
		return sample

	def on_close(self):
		plt.plot(self.values_a,self.values_b)
		plt.xlabel(self.x_metric)
		plt.ylabel(self.y_matric)
		plt.ioff()
		plt.savefig(self.__str__() + "-" + self.x_metric + "-" + self.y_matric + '.png')	
		plt.close()
		super().on_close()


class SimpleLinePlotProcessingStep(ProcessingStep):

	def __init__(self,metric_name):
		'''
		ProcessingStep to generate plot of one metric against time
		metric_name: value to plot
		'''
		self.metric_name = metric_name
		self.values = []
		self.time = []

	def __str__(self):
		return "SimpleLinePlotProcessingStep"

	def execute(self,sample):
		''' executed on each sample '''
		index = sample.header.header.index(self.metric_name	) # find index of metricq
		self.values.append(float(sample.metrics[index]))
		self.time.append(sample.get_timestamp())
		return sample

	def on_close(self):
		plt.plot(self.time,self.values)
		plt.xlabel("Time")
		plt.ylabel(self.metric_name)
		plt.ioff()
		plt.savefig(self.__str__() + "-" + self.metric_name + '.png')	
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
		return sample

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

class LinePlotProcessingStep(ProcessingStep):
	
	def __init__(self,metric_name,tag):
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
		return sample

	def on_close(self):
		for key,value in self.values.items():
			plt.plot(value[1],value[0],label=key, linewidth=2.0)
		plt.ylabel(self.metric_name)
		plt.xlabel("time")  
		legend = plt.legend(loc='upper right', fontsize='x-large')
		p1 = Process(target=plt.show())
		p1.start()
		super().on_close()