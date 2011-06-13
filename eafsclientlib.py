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

import math,uuid,os,time,operator,random,xmlrpclib,argparse,zlib


class EAFSChunkserver:
	def __init__(self, uuid, address):
		self.uuid = uuid
		self.address = address
		self.rpc = xmlrpclib.ServerProxy(address)


class EAFSClientLib():
	def __init__(self, master_host):
		#print "host: ", master_host
		self.master = xmlrpclib.ServerProxy(master_host)
		self.chunk_size = self.master.get_chunksize()
		self.chunkservers = {}
		self.fd = 0
	
	def update_chunkservers(self):
		chunkservers = self.master.get_chunkservers()
		for chunkserver in chunkservers:
			if chunkserver['uuid'] not in self.chunkservers:
				print "ADD CHUNKSERVER: ", chunkserver['uuid'], chunkserver['address']
				self.chunkservers[chunkserver['uuid']] = EAFSChunkserver( chunkserver['uuid'], chunkserver['address'] )
	
	def write_chunks(self, chunkuuids, data):
		chunks = [ data[x:x+self.chunk_size] \
			for x in range(0, len(data), self.chunk_size) ]
		self.update_chunkservers()
		#print "CHUNKSERVERS: ", len(self.chunkservers)
		for i in range(0, len(chunkuuids)): # write to each chunkserver
			chunkuuid = chunkuuids[i]
			chunklocs = self.master.get_chunklocs(chunkuuid)
			chunkserver_writes = 0
			for chunkloc in chunklocs:
				#print "Chunk size: ", i, len(chunks[i])
				try:
					self.chunkservers[chunkloc].rpc.write(chunkuuid, xmlrpclib.Binary(zlib.compress(chunks[i])))
					chunkserver_writes += 1
				except:
					print "Chunkserver %s failed" % chunkloc
					if chunkloc in self.chunkservers:
						del self.chunkservers[chunkloc]
			if chunkserver_writes==0:
				raise Exception("write_chunks error, not enough chunkserver available")
	
	
	def num_chunks(self, size):
		return (size // self.chunk_size) \
			+ (1 if size % self.chunk_size > 0 else 0)
	
	
	def eafs_write_append(self, filename, data):
		if not self.exists(filename):
			raise Exception("append error, file does not exist: " + filename)
		num_append_chunks = self.num_chunks(len(data))
		#print "[eafs_write_append] DATA SIZE, NUM CHUNKS:", len(data), num_append_chunks
		append_chunkuuids = self.master.alloc_append(filename, num_append_chunks)
		self.write_chunks(append_chunkuuids, data) 
		return len(data)
	
	
	def eafs_write(self, path, data, attributes):
		if self.exists(path):
			self.delete(path)
		num_chunks = self.num_chunks(len(data))
		chunkuuids = self.master.alloc(path, num_chunks, attributes)
		self.write_chunks(chunkuuids, data)
		return len(data)
	
	
	def eafs_read(self, path, size, offset):
		filename = path
		if not self.exists(filename):
			raise Exception("read error, file does not exist: " + filename)
		chunks = []
		chunkuuids = self.master.get_chunkuuids(filename)
		self.update_chunkservers()
		for chunkuuid in chunkuuids:
			chunklocs = self.master.get_chunklocs(chunkuuid)
			done_chunkserver = []
			chunk = None
			chunk_read = False
			while not (chunk_read or len(done_chunkserver)==len(chunklocs)):
				chunkidrnd = random.randint(0, len(chunklocs)-1)
				while chunkidrnd not in done_chunkserver and len(done_chunkserver)>0:
					chunkidrnd = random.randint(0, len(chunklocs)-1)
				chunkloc = chunklocs[chunkidrnd]
				#print "Select chunkloc %s from %d choices" % (chunkloc, len(chunklocs))
				try:
					chunk_raw = self.chunkservers[chunkloc].rpc.read(chunkuuid)
					chunk = chunk_raw.data
					chunk_read = True
				except:
					print "Chunkserver %d failed %d remaining" % (chunkidrnd, len(chunklocs)-len(done_chunkserver))
				done_chunkserver.append(chunkidrnd)
			if not chunk_read:
				raise Exception("read error, chunkserver unavailable: " + filename)
			chunks.append(chunk)
		data = reduce(lambda x, y: x + y, chunks) # reassemble in order
		return data[offset:offset+size]

