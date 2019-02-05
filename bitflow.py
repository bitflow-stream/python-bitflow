#!/usr/bin/python3

import argparse, logging, configparser, re, queue, signal, sys
#from sip_source import qos_sipp_stress
#from dns_source import qos_dns
#from pcap_source import qos_pcap
#from pcap_source import pcap_file
#from pcap_source.protocol_handler import PacketCount,SipHandler
#from pcap_source.tcp_handler import TCPHandler
from bitflow.sink import StdSink, FileSink, SendSink, ListenSink, GraphiteSink, NoopSink
from bitflow.marshaller import CsvMarshaller
from bitflow.pipeline import Pipeline
from bitflow.processingstep import * 
from bitflow.processingstepmanager import ProcessingStepManager
from bitflow.source import *

CI_DEFAULT = 500 * 0.001
L_PORT_DEFAULT = 5010
#PCAP_AUTO_RECOGNISE_DEFAULT= 15
MAX_LISTEN_PORT_RECEIVERS = 20

#PCAP_TIME_NORMAL = "normal"
#PCAP_TIME_HIGH = "high"

SOURCE = []

def __init__():
	pass

#def list_protocol_handlers():
#	for ph in AVAILABLE_PROTOCOL_HANLDERS:
#		print(ph.__name__)

#def get_protocol_handler_by_name(protocol_handler_name):
#	for h in AVAILABLE_PROTOCOL_HANLDERS:
#		if h.__name__ == protocol_handler_name:
#			return h 
#	return AVAILABLE_PROTOCOL_HANLDERS[0] # default hander



def configure_logging(debug,filename=None):
	if filename is None:
		if debug:
			logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
		else:
			logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
	else:
		if debug:
			logging.basicConfig(filename=filename,format='%(asctime)s %(message)s', level=logging.DEBUG)
		else:
			logging.basicConfig(filename=filename,format='%(asctime)s %(message)s', level=logging.INFO)

def safe_exit(pipeline):
	for i in SOURCE:
		try:
			i.on_close()
		except:
			sys.exit(1)
	
	try:
		pipeline.on_close()
	except:
		sys.exit(1)

	sys.exit(0)

def strg_c_exit():
	pipeline = SOURCE[0].pipeline
	safe_exit(pipeline)

def sig_int_handler(signal, frame):
	#print('You pressed Ctrl+C!')
	strg_c_exit()


def main():
	signal.signal(signal.SIGINT, sig_int_handler)
	parser = argparse.ArgumentParser()
	# network
	parser.add_argument("-l",help="value -> Data sink: accept TCP connections for sending out data (default [])",metavar=':PORT')
	parser.add_argument("-s",help="value -> Data sink: send data to specified TCP endpoint",metavar='HOST:PORT')
	parser.add_argument("-g",help="value -> Data sink: send data via graphite protocol to an endpoint",metavar='PREFIX@HOST:PORT')
	parser.add_argument("-n",action='store_true',help="value -> Data sink: Noop Sink is doing nothing")


	parser.add_argument("-D",help="value -> Data source: download data from specified host:port",metavar='HOST:PORT')
	parser.add_argument("-L",help="value -> Data source: accept TCP connections for receiving data (default [])",metavar=':PORT')

	# file
	parser.add_argument("-f",help="String -> Data sink: write data to file",metavar='Sink_FILE')
	parser.add_argument("-io_buf",help="int -> Size (byte) of buffered IO when writing files. (default 4096)",metavar='4096')
	parser.add_argument("-F",help="String -> Data source: read data from file",metavar='SOURCE_FILE')

	# generic
	#parser.add_argument("-ci",help="duration -> Interval for collecting local samples (default 500ms)",metavar='500')
	#parser.add_argument("-si",help="duration -> Interval for sinking (sending/printing/...) data when collecting local samples (default 500ms)",metavar='500')

	# ...
	parser.add_argument("-ff",help="String -> Data format for file Sink, one of (t=text, c=CSV, b=binary) (only supports csv for now)",metavar='c')
	parser.add_argument("-metrics",help="Print all available metrics and exit.",metavar='')
	parser.add_argument("-p",action='store_true',help="Data sink: print to stdout")
	parser.add_argument("-tag",help="All collected samples will have the given tags (key=value) attached. (default {[] []})",metavar='TAG=VALUE')

	# PRocessing Steps
	#parser.add_argument("-processing_step","-ps",action='append',nargs='*',help="inserts a processing_step into the internal processing pipeline. List of processing steps: ")
	parser.add_argument("-capabilities",action='store_true',help="list all available processing steps")
	# listen 
	parser.add_argument("-listen-tags",help="listen on defined Port to set Tags",metavar='PORT')

	# sources specific
	#parser.add_argument("-sipp",help="Sipp log file, enables collection of sipp stress specific metrics",metavar='sip-stress.1.xml')
	#parser.add_argument("-dns",help="collect data about dns server and domain.",metavar='zone@dnsserver')
	#parser.add_argument("-pcap",help="collect data through pcap based on provided dependency model or endpoints,\n \
	#	- expects interface@hostname \n \
	#	- and -dependency or -endpoints or -auto_recognition")
	#parser.add_argument("-pcap_file",help="collecting metrics through a previously stored pcap file")
	#parser.add_argument("-pcap_time_accuracy",help="defining time accaracy within the pcap file",metavar='normal|high')
	#parser.add_argument("-protocol_handler", help="define the used protocol handler by name, if not set default \"packet_count\" is used")
	#parser.add_argument("-lph","-list_protocol_handlers",action='store_true', help="list available protocol handlers")

	#parser.add_argument("-dependency",help="provide dependency model including hostname provided to pcap",metavar='dependency_model')
	#parser.add_argument("-endpoints",help="list of endpoints seperated by : e.g. 22:80:443")
	#parser.add_argument("-auto_recognition",help="run auto recognition for passed time (default 60s)",metavar='time_in_secounds')

	# logging
	parser.add_argument("-log",help="Redirect logs to a given file in addition to the console.",metavar='')
	parser.add_argument("-v",action='store_true',help="Enable verbose logging Sink")
	args = parser.parse_args()

	sink = []
	pipeline = Pipeline()
	ps_manager = ProcessingStepManager()

	#################
	# MANAGE DEBUG  #
	#################

	debug = False
	if args.v:
		debug = True

	if args.log:
		logfile = args.log
		configure_logging(debug,logfile)
	else:
		configure_logging(debug)
		logging.debug("debug mode enabled")

	################
	# LIST SECTION #
	################
	#if args.lph:
	#	list_protocol_handlers()
	#	sys.exit(0)

	if args.lps:
		ps_manager.print_processing_steps()
		sys.exit(0)

	###################
	#  MANAGE TIMING  #
	###################

	#ci = CI_DEFAULT
	#if args.ci:
	#	try:
	#		ci = float(args.ci) * 0.001
	#	except:
	#		logging.warning("not a valid number: " + args.ci + "\n using default value: " + CI_DEFAULT)
	#		ci = CI_DEFAULT

	####################
	#  MANAGE Sinks  #
	####################

	if args.f:
		io_buf = 4096
		if args.io_buf:
			io_buf = args.io_buf
		file_name = args.f
		file_out = FileSink(CsvMarshaller(),file_name,io_buf)
		logging.debug("Writing Sink to file: " + file_name)
		file_out.start()
		sink.append(file_out)

	if args.s:
		hostname,port = args.s.split(":")
		tcp_out = SendSink(CsvMarshaller(), hostname, int(port))
		logging.debug("Writing Sink to network: " + hostname+":"+str(port))
		tcp_out.start()
		sink.append(tcp_out)

	if args.n:
		noop_out = NoopSink()
		logging.debug("Writing Sink noop... ")
		noop_out.start()
		sink.append(noop_out)
	if args.g:
		if "@" in args.g:
			prefix,host_port = args.g.split("@")
			hostname,port = host_port.split(":")
			graphite_out = GraphiteSink(CsvMarshaller(), hostname, int(port),prefix=prefix)
		else:
			hostname,port = args.g.split(":")
			graphite_out = GraphiteSink(CsvMarshaller(), hostname, int(port))


		logging.debug("Writing Sink to Graphite: " + hostname + ":" + str(port))
		graphite_out.start()
		sink.append(graphite_out)

	if args.l:
		try:
			#match = re.findall(r'[0-9]+(?:\.[0-9]+){3}:[0-9]+', args.l) including hostip
			R = re.compile(r"(?P<host>.*):(?P<port>[0-9]+)")
			match = R.match(args.l)
			host = match.groupdict()['host']
			port = match.groupdict()['port']
		except Exception as e:
			logging.warning("not a valid port identifier: " + str(args.l) + "\n using default value: " + str(L_PORT_DEFAULT))
			port = L_PORT_DEFAULT
			logging.warning("Exception: " + str(e))
		logging.debug("Writing Sink to connected peers "+str(port))
		ltcp_out = ListenSink(CsvMarshaller(),MAX_LISTEN_PORT_RECEIVERS,host,port)
		ltcp_out.start()
		sink.append(ltcp_out)

	if args.p:
		std_out = StdSink(CsvMarshaller())
		logging.debug("Writing Sink to stdout")
		std_out.start()
		sink.append(std_out)

	if len(sink) == 0:
		logging.error("no Sink method defined")
		parser.print_help(sys.stderr)
		safe_exit(pipeline)
			
	#####################
	#  MANAGE PIPELINE  #
	#####################

	if args.tag:
		try:
			tags_string = str(args.tag)
			tag = {}
			if "," in tags_string:
				tags_list = tags_string.split(',')
				for tag_string in tags_list:
					k,v = tag_string.split('=')
					tag[k] = v
			else: # only one tag
				k,v = tags_string.split('=')
				tag[k] = v 
		except Exception as tag1:
			logging.error(args.tag + " has not a valid tag syntax. Has to look like: source=web.de " + str(tag1))
		else:	
			logging.debug("adding processing Step AddTag to processing pipeline")
			pipeline.add_processing_step(AddTagProcessingStep(tag))

	if args.listen_tags:
		try:
			port = int(args.listen_tags)
			logging.debug("adding processing Step ListenForTagsProcessingStep to processing pipeline")
			pipeline.add_processing_step(ListenForTagsProcessingStep(port))
		except Exception as listen_tags1:
			logging.warning(str(port) + " is not a valid port, using default port 7777.")
			pipeline.add_processing_step(ListenForTagsProcessingStep())
		
	#if args.sipp:
	#	pipeline.add_filter(SippStressQosMetricsFilter())

	
	pipeline.set_sink(sink)
	pipeline.start()


	#######################
	#  MANAGE DATASOURCES #
	#######################	
	# SIPP COLLECTOR
	# if args.sipp:
	# 	sipp = qos_sipp_stress.QosSippStress(args.sipp,pipeline,ci)
	# 	logging.debug("reading data from QosSippStress-Collector")
	# 	SOURCE.append(sipp)
	# 	sipp.start()
	# # DNS COLLECTOR
	# elif args.dns:
	# 	try:
	# 		zone,dnsserver = args.dns.split("@") 
	# 		dns = qos_dns.QosDns(zone,dnsserver,pipeline,ci)
	# 		logging.debug("reading data from QosDNS-Collector")
	# 		SOURCE.append(dns)
	# 		dns.start()
	# 	except Exception as dns1:
	# 		logging.error("Not a valid zone@dnsserver server Anotation! " + str(dns1))
	# 		exit(1)
	
	# # PCAP COLLECTOR
	# elif args.pcap:
	# 	# always wants iface@hostname
	# 	try:
	# 		iface,hostname = args.pcap.split("@")
	# 	except:
	# 		logging.error("expecting -pcap INTERFACE@HOSTNAME")
	# 		exit(1)
	# 	# TODO instert protocol handler choosing
	# 	pcapsource = qos_pcap.QosPcap(hostname,iface,pipeline,ci)
	# 	logging.debug("reading data from PCAP-Collector")

	# 	optional_args=0
		
	# 	if args.dependency:
	# 		dependency_model = args.dependency
	# 		pcapsource.build_by_dependency(args.dependency)
	# 		optional_args+=1
	# 	elif args.endpoints:
	# 		ports = args.endpoints.split(":")
	# 		pcapsource.build_by_endpoints(ports)
	# 		optional_args+=1
	# 	elif args.auto_recognition:
	# 		try:
	# 			pcapsource.build_by_auto_recognition(args.auto_recognition)
	# 		except:
	# 			pcapsource.build_by_auto_recognition(PCAP_AUTO_RECOGNISE_DEFAULT)
	# 		optional_args+=1

	# 	if optional_args > 1:
	# 		logging.error("to many optional arguments choose one out of dependency,endpoint and auto_recognition")
	# 		exit(1)
	# 	if optional_args == 0:
	# 		logging.error("to less optional arguments choose one out of dependency,endpoint and auto_recognition")
	# 		exit(1)

	# 	try:
	# 		SOURCE.append(pcapsource)
	# 		pcapsource.start()
	# 	except Exception as pcap1:
	# 		logging.error("something went wrong while capturing packets on network, " + str(pcap1))
	
	# # PCAP FILE
	# elif args.pcap_file:
	# 	try: 
	# 		pcapfile = args.pcap_file
	# 		acc = 1
	# 		if args.pcap_time_accuracy:
	# 			if args.pcap_time_accuracy == PCAP_TIME_HIGH:
	# 				acc = 100
	# 		if args.protocol_handler:
	# 			protocol_handler = args.protocol_handler
	# 			ph_class = get_protocol_handler_by_name(protocol_handler)
	# 			pf = pcap_file.PcapFile(pcapfile,acc,"port 1935",pipeline,ci,ph_class)
	# 		else:
	# 			logging.error("protocol_handler parameter missing!")
	# 			exit(1)

	# 		SOURCE.append(pf)
	# 		pf.start()
	# 	except Exception as pcap_file1:
	# 		logging.error("Sonething went wrong by initializing pcap-file input "  + str(pcap_file1))
	# 		exit(1)
	# FILE SOURCE
	
	#elif args.F:
	if args.F:
		try:
			filename = args.F
			filesource = FileSource(filename,pipeline,CsvMarshaller())
			logging.debug("reading data from File: " + filename)
			SOURCE.append(filesource)
			filesource.start()
		except Exception as filesource1:
			logging.error("something went wrong while reading file " + filename + ". " + str(filesource1))
		exit(1)
	# DOWNLOAD SOURCE
	elif args.D:
		try:
			host,port = args.D.split(':')
			downloadsource = DownloadSource(host,int(port),pipeline,CsvMarshaller())
			logging.debug("downloading data from Endpoint: " + host+":"+port)
			SOURCE.append(downloadsource)
			downloadsource.start()
		except Exception as downloadsource1:
			logging.error("something went wrong while connecting to host:port " + args.D + ". " + str(downloadsource1))
			exit(1)
	# LISTEN ON SOCKET SOURCE
	elif args.L:
			try:
				port = int(args.L[1:])
			except Exception as listensource1:
				port = 5010
				logging.error("Somethhing went wrong while parsing argument -L " + str(listensource1))
			listensource = ListenSource(CsvMarshaller(),pipeline,None,int(port))
			SOURCE.append(listensource)
			listensource.start()
	else:
		logging.error("No data source defined")
		parser.print_help(sys.stderr)
		safe_exit(pipeline)
		

if __name__ == '__main__':
	main()

