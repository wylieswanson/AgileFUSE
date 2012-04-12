#!/usr/bin/python

import socket, sys
import json

class	AgileReader(object):

	def	__init__(self, sockpath=None):
		print "init"
		self.sockpath = sockpath

	def	setup(self):
		try:
			self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
			print dir(self.socket)
		except:
			raise

	def	connect(self):
		try:
			self.socket.connect(self.sockpath)
		except:
			raise

	def	create_payload(self, url=None, offset=None, size=None):
		try:
			payload_dict = {}
			payload_dict['url'] = url
			payload_dict['offset'] = offset
			payload_dict['size'] = size
			payload_bin = json.dumps(payload_dict)
			return payload_bin
		except:
			raise

	def	send_request(self, payload):
		try:
			self.socket.send(payload)
		except:
			raise
if __name__  == "__main__":

	url = 'http://mapper.dev.lldns.net/llnw/staff/bblack/lamaupload.mp3'
	offset = 0
	size = 2055764
	MAX_READ = 4096
	try:
		obj = AgileReader(sockpath=sys.argv[1])
		obj.setup()
		obj.connect()
		payload_bin = obj.create_payload(url=url, offset=offset, size=size)
		if len(payload_bin):
			obj.send_request(payload_bin)
		f = open('payload.mp3', 'w')
		while True:
			data = obj.socket.recv(MAX_READ)
			if not data: break
			print "recv %d len packet" % (len(data))
			f.write(data)
		f.close()
		obj.socket.close()
	except:
		raise
