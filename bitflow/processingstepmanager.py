from bitflow.processingstep import *

class ProcessingStepManager():
	
	steps = [{
		"name": "Noop",
		"description": " No Operation ProcessingStep",
		"args": {},
		"class": NoopProcessingStep
		},{
		"name": "ModifyTimestamp",
		"description": "change timestamp of samples, with defined starting time and interval between samples",
		"args":[{
				"name": "interval",
				"ptype": float,
				"desc": "defined interval (seconds) between each sample",
				"required": True,
			},{
				"name": "start_time",
				"ptype": str,
				"desc": "New starting Time like '2018-04-06 14:51:15.157232'",
				"required": True
			}],
		"class": ModifyTimestampProcessingStep
	},{
		"name": "ExcludeMetricByName",
		"description": "Excludes Metrics from Sample by given name in header",
		"args":[{
				"name": "exclude_list",
				"ptype": list,
				"desc": "List of header fields to exclude from sample",
				"required": True,
				}],
		"class": ExcludeMetricByNameProcessingStep
	}]
		
	def get_console_size(self):
		import os
		rows, columns = os.popen('stty size', 'r').read().split()
		return rows,columns

	def print_processing_steps(self):
		args_indent = "\t"
		seperator_char = "-"
		__,columns = self.get_console_size()
		seperator = "" 
		for i in range(int(columns)):
			seperator+=seperator_char

		print(seperator)
		for ps in self.steps:
			print("{}:".format(ps["name"]))
			print("description: {}".format(ps["description"]))
			for argument in ps["args"]:
				if argument["required"] == True:
					print("{}{} [{}]: {}".format(args_indent,argument["name"],str(argument["ptype"]),argument["desc"]))
				else:
					print("{}{} [{}]: {} (optional)".format(args_indent,argument["name"],str(argument["ptype"]),argument["desc"]))
			print(seperator)

	