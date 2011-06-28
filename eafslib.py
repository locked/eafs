import time
import xmlrpclib
import socket

class EAFSChunkServerRpc:
	def __init__(self, uuid, address):
		self.uuid = uuid
		self.address = address
		self.size_total = 0
		self.size_available = 0
		self.available = 1
		self.last_seen = time.time()
		self.rpc = xmlrpclib.ServerProxy(address)
		socket.setdefaulttimeout(5)
