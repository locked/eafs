# -*- coding: utf-8 -*-
'''
 * EAFS
 * Copyright (C) 2009-2011 Adam Etienne <eadam@lunasys.fr>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation version 3.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
'''

import math,uuid,os,time,operator,sys,argparse,zlib,threading,statvfs,hashlib
import xmlrpclib
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler

from eafslib import EAFSChunkServerRpc


uuid_filename = "chunkserver.uuid"



class EAFSChunkserver:
	def __init__(self, master_host, host, port, rootfs):
		# Create root fs
		self.rootfs = rootfs
		if not os.access(self.rootfs, os.W_OK):
			os.makedirs(self.rootfs)
		self.uuid_filename_full = os.path.join( self.rootfs, str(port)+"-"+uuid_filename )
		uuid = ""
		try:
			f = open(self.uuid_filename_full, 'r+')
			lines = f.readlines()
			if lines is not None and len(lines)>0:
				uuid = str(lines[0])
		except:
			print "No uuid file. Will create it."
	
		self.address = "http://%s:%d" % (host, port)
		self.master = xmlrpclib.ServerProxy(master_host)
		self.uuid = self.master.connect_chunkserver( self.address, uuid )
		if self.uuid is None:
			return False
		if uuid is None or uuid=="":
			print "Create UUID file %s" % self.uuid_filename_full,
			f = open(self.uuid_filename_full, 'w+')
			f.write(self.uuid)
			f.close()
		#self.chunktable = {}
		self.local_filesystem_root = os.path.join( rootfs, "chunks", str(self.uuid) ) #repr
		#print "FS ROOT: %s" % self.local_filesystem_root
		if not os.access(self.local_filesystem_root, os.W_OK):
			os.makedirs(self.local_filesystem_root)
	
	
	def write(self, chunk_uuid, chunk):
		local_filename = self.chunk_filename(chunk_uuid)
		with open(local_filename, "w") as f:
			f.write(chunk.data)
			#f.write(zlib.decompress(chunk.data))
		chunk_md5 = hashlib.md5(zlib.decompress(chunk.data)).hexdigest()
		local_filename_md5 = self.chunk_md5_filename(chunk_uuid)
		with open(local_filename_md5, "w") as f:
			f.write(chunk_md5)
		#print "chunkserver_has_chunk: ", self.uuid, chunk_uuid, chunk_md5
		self.master.chunkserver_has_chunk( self.uuid, chunk_uuid, chunk_md5 )
		return len(chunk.data)
	
	
	def read(self, chunkuuid):
		data = None
		local_filename = self.chunk_filename(chunkuuid)
		with open(local_filename, "r") as f:
			data = f.read()
		return xmlrpclib.Binary(data)
		#return xmlrpclib.Binary(zlib.compress(data))
	
	
	def replicate(self, chunkuuid, chunkserver_uuid, chunkserver_address):
		chunkserver = EAFSChunkServerRpc( chunkserver_uuid, chunkserver_address )
		chunk = chunkserver.rpc.read( chunkuuid )
		self.write( chunkuuid, chunk )
	
	
	def stat(self):
		f = os.statvfs(self.rootfs)
		size_total = int( f[statvfs.F_BLOCKS] * f[statvfs.F_FRSIZE] / 1024 )
		size_available = int( f[statvfs.F_BAVAIL] * f[statvfs.F_FRSIZE] / 1024 )
		return (size_total, size_available)
	
	
	def chunk_filename(self, chunkuuid):
		return os.path.join( self.local_filesystem_root, str(chunkuuid) ) + '.eafs'
	
	
	def chunk_md5_filename(self, chunkuuid):
		return os.path.join( self.local_filesystem_root, str(chunkuuid) ) + '.md5'



class RequestHandler(SimpleXMLRPCRequestHandler):
	rpc_paths = ('/RPC2',)


def main():
	parser = argparse.ArgumentParser(description='EAFS Chunk Server')
	parser.add_argument('--host', dest='host', default='localhost', help='Bind to address')
	parser.add_argument('--port', dest='port', default=6800, type=int, help='Bind to port')
	parser.add_argument('--master', dest='master', default='localhost:6799', help='Master server address')
	parser.add_argument('--rootfs', dest='rootfs', default='/tmp/eafs/', help='Save chunk to')
	args = parser.parse_args()
	
	master_host = "http://" + args.master
	
	# Create server
	server = SimpleXMLRPCServer((args.host, args.port), requestHandler=RequestHandler, allow_none=True, logRequests=False)
	server.register_introspection_functions()
	server.register_instance(EAFSChunkserver(master_host, args.host, args.port, args.rootfs))
	server.serve_forever()

if __name__ == "__main__":
	main()

