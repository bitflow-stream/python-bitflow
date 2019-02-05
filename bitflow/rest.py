from flask import Flask,request,abort
import threading,logging,time

class RestServer(threading.Thread):
			
	def __init__(self,lock_all_tags,all_tags,port=7777):
		self.lock_all_tags = lock_all_tags
		self.all_tags = all_tags
		self.port = port

		self.timer_list = []

		self.app = Flask(__name__)
		self.app.route('/tag', methods=['POST'])(self.get_tags)

		super().__init__()

	def __str(self):
		return "RestServerFilter"

	def run(self):
		logging.info("service started")
		self.app.run(host="0.0.0.0", port=self.port)
	
	def get_tags(self):
		if request.method == 'POST':
			tags_request = [-1,{}]
			for key,value in request.args.items():
				if key == "timeout":
					try:
						tags_request[0] = int(value)
					except Exception as e:
						logging.err("passed Timeout is not an integer Value"+ str(e))
						abort(403)
				else:
					tags_request[1][key]=value
					self.all_tags[key]=value
				
			self.lock_all_tags.acquire()

			for timer in self.timer_list:
				for tag in tags_request[1]:
					try:
						del timer.tags_request[1][tag]
					except: pass
				if len(timer.tags_request[1]) == 0:
					pass #maybe kill useless threads here
			self.lock_all_tags.release()

			if tags_request[0] != -1:
				timer = WaitAndRemove(self.lock_all_tags,self.all_tags,tags_request)
				self.timer_list.append(timer)
				timer.start()
			return "200"
		else:
			abort(415)

			
class WaitAndRemove(threading.Thread):

	def __init__(self,lock_all_tags,all_tags,tags_request):
		self.lock_all_tags = lock_all_tags
		self.all_tags = all_tags
		self.tags_request = tags_request
		super().__init__()

	def run(self):
		try:
			time.sleep(int(self.tags_request[0]))
			for tag in self.tags_request[1]:
				logging.info("removing tag: " + tag + " from tags")
				self.remove_tag(tag)
		except Exception as e1:
			logging.warning("tag not in list: " + str(e1))

	def remove_tag(self,key):
		self.lock_all_tags.acquire()
		del self.all_tags[key]
		self.lock_all_tags.release()
