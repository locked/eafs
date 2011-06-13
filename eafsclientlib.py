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


class EAFSChunkServerRpc:
	def __init__(self, uuid, address):
		self.uuid = uuid
		self.address = address
		self.rpc = xmlrpclib.ServerProxy(address)


class EAFSClientLib():
	def __init__(self, master_host, debug=0):
		#print "host: ", master_host
		self.debug = debug
		self.master = xmlrpclib.ServerProxy(master_host)
		self.chunk_size = self.master.get_chunksize()
		self.chunkservers = {}
		self.fd = 0
		self.cache_counter = 0
	
	def update_chunkservers(self):
		self.cache_counter -= 1
		if self.cache_counter<=0:
			self.cache_counter = 100
			chunkservers = self.master.get_chunkservers()
			for chunkserver in chunkservers:
				if chunkserver['uuid'] not in self.chunkservers:
					print "ADD CHUNKSERVER: ", chunkserver['uuid'], chunkserver['address']
					self.chunkservers[chunkserver['uuid']] = EAFSChunkServerRpc( chunkserver['uuid'], chunkserver['address'] )
	
	def write_chunks(self, chunkuuids, data):
		chunks = [ data[x:x+self.chunk_size] \
			for x in range(0, len(data), self.chunk_size) ]
		#start = time.time()
		self.update_chunkservers()
		#if self.debug>0: print "[write_chunks] update_chunkservers: ", (time.time()-start)
		#start_total = time.time()
		for i in range(0, len(chunkuuids)): # write to each chunkserver
			chunkuuid = chunkuuids[i]
			chunklocs = self.master.choose_chunkserver_uuids()
			#self.master.get_chunklocs(chunkuuid)
			chunkserver_writes = 0
			for chunkloc in chunklocs:
				if chunkloc in self.chunkservers:
					#if self.debug>3: print "Chunk size: ", i, len(chunks[i])
					try:
						#start = time.time()
						self.chunkservers[chunkloc].rpc.write(chunkuuid, xmlrpclib.Binary(zlib.compress(chunks[i])))
						#if self.debug>1: print "[write_chunks] rpc.write: ", (time.time()-start)
						chunkserver_writes += 1
					except:
						print "Chunkserver %s failed" % chunkloc
						if chunkloc in self.chunkservers:
							del self.chunkservers[chunkloc]
			if chunkserver_writes==0:
				raise Exception("write_chunks error, not enough chunkserver available")
		#if self.debug>0: print "[write_chunks] total writes: ", (time.time()-start_total)
	
	
	def num_chunks(self, size):
		return (size // self.chunk_size) \
			+ (1 if size % self.chunk_size > 0 else 0)
	
	
	def eafs_write_append(self, filename, data):
		if not self.exists(filename):
			raise Exception("append error, file does not exist: " + filename)
		num_append_chunks = self.num_chunks(len(data))
		#print "[eafs_write_append] DATA SIZE, NUM CHUNKS:", len(data), num_append_chunks
		start = time.time()
		append_chunkuuids = self.master.alloc_append(filename, num_append_chunks)
		#if self.debug>0: print "[eafs_write_append] master.alloc_append: ", (time.time()-start)
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
		#if self.debug>1: print "eafs_read filename: [%s] size:%d offset:%d" % (filename, size, offset)
		(chunkuuids, offset, chunkserver_uuids) = self.master.get_chunkuuids_offset(filename,size,offset)
		#if self.debug>2: print "eafs_read chunkuuids: ", chunkuuids
		self.update_chunkservers()
		for chunkuuid in chunkuuids:
			#if self.debug>3: print "eafs_read chunkuuid: ", chunkuuid
			#chunklocs = self.master.get_chunklocs(chunkuuid)
			chunklocs = chunkserver_uuids[chunkuuid]
			done_chunkserver = []
			chunk = None
			chunk_read = False
			while not (chunk_read or len(done_chunkserver)==len(chunklocs)):
				chunkidrnd = random.randint(0, len(chunklocs)-1)
				#print "Random: ", chunkidrnd, done_chunkserver, chunklocs
				if len(done_chunkserver)>0:
					while chunkidrnd in done_chunkserver:
						chunkidrnd = random.randint(0, len(chunklocs)-1)
						#print "Random2: ", chunkidrnd, done_chunkserver, chunklocs
				chunkloc = chunklocs[chunkidrnd]
				done_chunkserver.append(chunkidrnd)
				#if self.debug>2: print "Select chunkloc %d::%s from %d choices" % (chunkidrnd, chunkloc, len(chunklocs))
				try:
					chunk_raw = self.chunkservers[chunkloc].rpc.read(chunkuuid)
					chunk = zlib.decompress(chunk_raw.data)
					chunk_read = True
				except:
					print "Chunkserver %d failed %d remaining" % (chunkidrnd, len(chunklocs)-len(done_chunkserver))
			if not chunk_read:
				raise Exception("read error, chunkserver unavailable: " + filename)
			chunks.append(chunk)
		if len(chunks)==0:
			return None
		data = reduce(lambda x, y: x + y, chunks) # reassemble in order
		return data[offset:offset+size]

