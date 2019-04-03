from bitflow.processingstep import *

class PlotTagBoxplot(ProcessingStep):

	DEFAULT_FORMAT = "png"

	__description__ = "Boxplot processing step. Plots given metric and distinguashes by given tag name. One box per unique tag_value. file_formats: png|pdf|svg(default=png)"
	__name__ = "PlotTagBoxplot"

	SUPPORTED_FILE_FORMATS = ["png", "svg","pdf"]

	def __init__(self,
				metric_name : str,
				tag : str,
				filename : str = "",
				ylabel : str = "",
				xlabel : str = "",
				title : str = "",
				show_xlabel : bool = True,
				show_ylabel : bool = True,
				show_title : bool = True,
				file_format : str ="png",
				font_size : float = 12,
				showfliers: bool = True):

		super().__init__()
		self.metric_name = metric_name
		self.tag = tag
		self.values = {}

		if file_format.lower() in self.SUPPORTED_FILE_FORMATS:
			self.file_format = file_format
		else:
			raise NotSupportedError("{}: file format, {}, not supported ...".format(file_format))


		if filename == "":
			self.filename = self.__name__ + "-" + metric_name + "." + file_format
		else: 
			self.filename = filename
		if ylabel == "":
			self.ylabel = None
		else: 
			self.ylabel = ylabel
		if xlabel == "":
			self.xlabel = None
		else:
			self.xlabel = xlabel
		if title == "":
			self.title = self.__name__ + ": " + metric_name 
		else:
			self.title = title

		self.show_ylabel = show_ylabel 
		self.show_xlabel = show_xlabel 
		self.show_title = show_title
		self.showfliers = showfliers
		self.font_size = font_size
		
	def execute(self,sample):
		''' executed on each sample '''
		try:
			index = sample.header.header.index(self.metric_name	) # find index of metricq
		except ValueError:
			self.write(sample)

		tv = sample.get_tag(self.tag)
		if tv not in self.values:
			self.values[tv] = []
		self.values[tv].append(float(sample.metrics[index]))

		self.write(sample)

	def on_close(self):
		v_list=[]
		x_labels = []	

		for key,value in self.values.items():
			v_list.append(value)
			x_labels.append(key)

		plt.boxplot(v_list,showfliers=self.showfliers,whiskerprops={'color':'black'}, boxprops={'color':'black'} )
		plt.xticks(range(1,len(v_list)+1),x_labels,rotation='vertical',fontsize=self.font_size)
		
		if self.show_xlabel:
			if self.xlabel:
				plt.xlabel(self.xlabel,fontsize=self.font_size - 4)
			else:
				plt.xlabel("Tag="+self.tag,fontsize=self.font_size - 4)

		if self.show_ylabel:
			if self.ylabel:
				plt.ylabel(self.ylabel,fontsize=self.font_size - 4)
			else:
				plt.ylabel(self.metric_name,fontsize=self.font_size - 4)

		if self.show_title:
			plt.title(self.title)

		plt.savefig(self.filename,format=self.file_format)		
		plt.close()
		super().on_close()


class PlotLinePlot(ProcessingStep):

	DEFAULT_FORMAT = "png"

	__description__ = "Lineplot processing step. Plots given metrics E.g. one line per given metric over time. file_format: png|pdf|svg(default=png)"
	__name__ = "PlotLineplot"

	SUPPORTED_FILE_FORMATS = ["png", "svg","pdf"]

	def __init__(self,
				metric_names : str,
				filename : str = "",
				ylabel : str = "",
				xlabel : str = "",
				title : str = "",
				show_xlabel : bool = True,
				show_ylabel : bool = True,
				show_title : bool = True,
				show_legend : bool = True,
				file_format : str ="png",
				font_size : float = 12):

		super().__init__()
		self.metric_names = string_lst_to_lst(metric_names)
		self.values = {}

		if file_format.lower() in self.SUPPORTED_FILE_FORMATS:
			self.file_format = file_format
		else:
			raise NotSupportedError("{}: file format, {}, not supported ...".format(file_format))

		if filename == "":
			self.filename = self.__name__ + "-" + metric_name + "." + file_format
		else: 
			self.filename = filename
		if ylabel == "":
			self.ylabel = None
		else: 
			self.ylabel = ylabel
		if xlabel == "":
			self.xlabel = None
		else:
			self.xlabel = xlabel
		if title == "":
			self.title = self.__name__ + ": " + metric_names
		else:
			self.title = title

		self.show_legend = show_legend
		self.show_title = show_title
		self.show_ylabel = show_ylabel 
		self.show_xlabel = show_xlabel 
		self.font_size = font_size

	def execute(self,sample):
		''' executed on each sample '''
		for metric in self.metric_names:
			try:
				index = sample.header.header.index(metric) # find index of metricq
			except ValueError:
				self.write(sample)

			if metric not in self.values:
				self.values[metric] = []
			self.values[metric].append(float(sample.metrics[index]))
			self.write(sample)

	def on_close(self):
		for key,values in self.values.items():
			plt.plot(values,label=key,linewidth=0.6)
		
		if self.show_xlabel:
			if self.xlabel:
				plt.xlabel(self.xlabel,fontsize=self.font_size - 4)
			else:
				plt.xlabel("time",fontsize=self.font_size - 4)
		
		if self.show_ylabel:
			if self.ylabel:
				plt.ylabel(self.ylabel,fontsize=self.font_size - 4)
			else:
				plt.ylabel("values",fontsize=self.font_size - 4)

		if self.show_title:
			plt.title(self.title)
		if self.show_legend:
			plt.legend()

		plt.savefig(self.filename,format=self.file_format)		
		plt.close()
		super().on_close()


class PlotBarPlot(ProcessingStep):

	DEFAULT_FORMAT = "png"

	__description__ = "Barplot processing step. Plots given metrics as bar. using aggregation: sum|avg(default=sum), file_format: png|pdf|svg(default=png)"
	__name__ = "PlotBarplot"

	SUPPORTED_FILE_FORMATS = ["png", "svg","pdf"]
	SUPPORTED_AGGREGATION_FORMAT = ["sum", "avg"]

	def __init__(self,
				metric_names : str,
				aggregation: str = "sum",
				filename : str = "",
				ylabel : str = "",
				xlabel : str = "",
				title : str = "",
				show_xlabel : bool = True,
				show_ylabel : bool = True,
				show_title : bool = True,
				file_format : str ="png",
				font_size : float = 12):

		super().__init__()
		self.metric_names = string_lst_to_lst(metric_names)
		self.values = {}

		if file_format.lower() in self.SUPPORTED_FILE_FORMATS:
			self.file_format = file_format
		else:
			raise NotSupportedError("{}: file format, {}, not supported ...".format(file_format))
		
		if aggregation.lower() in self.SUPPORTED_AGGREGATION_FORMAT:
			self.aggregation = aggregation
		else:
			raise NotSupportedError("{}: aggregation format, {}, not supported ...".format(aggregation))

		if filename == "":
			self.filename = self.__name__ + "-" + metric_name + "." + file_format
		else: 
			self.filename = filename
		if ylabel == "":
			self.ylabel = None
		else: 
			self.ylabel = ylabel
		if xlabel == "":
			self.xlabel = None
		else:
			self.xlabel = xlabel
		if title == "":
			self.title = self.__name__ + ": " + metric_names
		else:
			self.title = title

		self.show_title = show_title
		self.show_ylabel = show_ylabel 
		self.show_xlabel = show_xlabel 
		self.font_size = font_size

	def execute(self,sample):
		''' executed on each sample '''
		for metric in self.metric_names:
			try:
				index = sample.header.header.index(metric) # find index of metricq
			except ValueError:
				self.write(sample)

			if metric not in self.values:
				self.values[metric] = []
			self.values[metric].append(float(sample.metrics[index]))
			self.write(sample)

	def on_close(self):
		v_list = []

		for key,values in self.values.items():
			if self.aggregation == self.SUPPORTED_AGGREGATION_FORMAT[0]: #sum
				v = sum(values)
			if self.aggregation == self.SUPPORTED_AGGREGATION_FORMAT[1]: #avg
				v = sum(values) / len(values) 
			v_list.append(v)

		bp = plt.bar(range(0,len(self.metric_names)),v_list,align="center")
		plt.subplots_adjust(bottom=0.3)
		xticks_pos = [0.65*patch.get_width() + patch.get_xy()[0] for patch in bp]
		plt.xticks(xticks_pos, self.metric_names,  ha='right', rotation=45)
		
		if self.show_xlabel:
			if self.xlabel:
				plt.xlabel(self.xlabel,fontsize=self.font_size - 4)
			else:
				plt.xlabel("time",fontsize=self.font_size - 4)
		
		if self.show_ylabel:
			if self.ylabel:
				plt.ylabel(self.ylabel,fontsize=self.font_size - 4)
			else:
				plt.ylabel("values",fontsize=self.font_size - 4)

		if self.show_title:
			plt.title(self.title)

		plt.savefig(self.filename,format=self.file_format)		
		plt.close()
		super().on_close()
