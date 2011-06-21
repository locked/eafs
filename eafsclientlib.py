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

import math,uuid,os,time,operator,random,xmlrpclib,argparse,zlib,threading,hashlib

from eafslib import EAFSChunkServerRpc



class EAFSClientLib():
	def __init__(self, master_host, debug=0):
		self.debug = debug
		self.master = xmlrpclib.ServerProxy(master_host)
		self.chunk_size = self.master.get_chunksize()
		self.chunkservers = {}
		self.chunk_cache = {}
		self.chunk_cache_read = {}
		self.chunk_cache_read_wait = {}
		self.fd = 0
		#self.cache_counter = 0
	
	
	def update_chunkservers(self):
		#self.cache_counter -= 1
		#if self.cache_counter<=0:
		#	self.cache_counter = 100
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
			chunkserver_uuids = self.master.choose_chunkserver_uuids()
			chunkserver_writes = 0
			for chunkserver_uuid in chunkserver_uuids:
				if chunkserver_uuid in self.chunkservers:
					#print "chunkserver_uuid: ", chunkserver_uuid
					#if self.debug>3: print "Chunk size: ", i, len(chunks[i])
					try:
						#if True:
						#start = time.time()
						write_data = zlib.compress(chunks[i])
						write_data_len = int(self.chunkservers[chunkserver_uuid].rpc.write(chunkuuid, xmlrpclib.Binary(write_data)))
						#if self.debug>1: print "[write_chunks] rpc.write: ", (time.time()-start)
						if write_data_len==len(write_data):
							chunkserver_writes += 1
						else:
							print "Write on chunkserver %s failed (%d/%d)" % (chunkserver_uuid, write_data_len, len(write_data))
					except:
						print "Chunkserver %s failed" % chunkserver_uuid
						if chunkserver_uuid in self.chunkservers:
							del self.chunkservers[chunkserver_uuid]
			if chunkserver_writes==0:
				raise Exception("write_chunks error, not enough chunkserver available")
		#if self.debug>0: print "[write_chunks] total writes: ", (time.time()-start_total)
	
	
	def num_chunks(self, size):
		return (size // self.chunk_size) \
			+ (1 if size % self.chunk_size > 0 else 0)
	
	
	def accumulate(self, path, data, fh):
		if fh not in self.chunk_cache:
			self.chunk_cache[fh] = ""
		self.chunk_cache[fh] += data
		if len(self.chunk_cache[fh])>=self.chunk_size:
			self.flush_low( path, fh )
		return len(data)
	
	
	def eafs_write_append(self, path, data, fh):
		#if not self.exists(path):
		#	raise Exception("append error, file does not exist: " + path)
		#num_append_chunks = self.num_chunks(len(data))
		#print "[eafs_write_append] DATA SIZE, NUM CHUNKS:", len(data), num_append_chunks
		#start = time.time()
		#append_chunkuuids = self.master.alloc_append(path, num_append_chunks)
		#if self.debug>0: print "[eafs_write_append] master.alloc_append: ", (time.time()-start)
		#self.write_chunks(append_chunkuuids, data)
		#print "eafs_write: fh:%d path:%s data:%d chunk_cache:%d" % (fh, path, len(data), len(self.chunk_cache[fh]))
		return self.accumulate( path, data, fh )
	
	
	def flush_low(self, path, fh):
		if fh not in self.chunk_cache:
			return False
		if len(self.chunk_cache[fh])>self.chunk_size:
			data = self.chunk_cache[fh][0:self.chunk_size]
		else:
			data = self.chunk_cache[fh]
		#print "data:", data
		#print "cache:", self.chunk_cache[path]
		#print "Flush Low: fh:%d path:%s data:%d chunk_cache:%d chunk_size:%d" % (fh, path, len(data), len(self.chunk_cache[fh]), self.chunk_size)
		if len(data)>0:
			num_append_chunks = self.num_chunks(len(data))
			data_md5 = hashlib.md5(data).hexdigest()
			#print "Flush Low: chunks:%d" % (num_append_chunks)
			if not self.exists(path):
				attributes = {"type":"f", "atime":time.time(), "ctime":time.time(), "mtime":time.time(), "size":0, "links":1, "attrs":""}
				chunkuuids = self.master.alloc(path, num_append_chunks, attributes, data_md5)
			else:
				chunkuuids = self.master.alloc_append(path, num_append_chunks, data_md5)
			#print "Flush Low: append_chunkuuids:%d" % (len(data))
			self.master.file_set_attr(path, 'size', int(len(data)), 'add')
			
			self.write_chunks(chunkuuids, data)
			
			if fh in self.chunk_cache:
				self.chunk_cache[fh] = self.chunk_cache[fh][self.chunk_size:]
			else:
				return 0
		return True
	
	
	def exists(self, path):
		return self.master.exists(path)
	
	
	def eafs_flush(self, path, fh):
		#print "** FUSE called flush( %s ) **" % path
		self.flush_low( path, fh )
		if fh in self.chunk_cache:
			del self.chunk_cache[fh]
		return True
	
	
	def eafs_write(self, path, data, fh): #, attributes
		if self.exists(path):
			self.master.delete(path)
		#num_chunks = self.num_chunks(len(data))
		#chunkuuids = self.master.alloc(path, num_chunks, attributes)
		#print "eafs_write: fh:%d path:%s data:%d" % (fh, path, len(data))
		self.accumulate( path, data, fh )
		#self.write_chunks(chunkuuids, data)
		return len(data)
	
	
	def eafs_read(self, path, size, offset):
		if not self.exists(path):
			raise Exception("read error, file does not exist: " + path)
		#if self.debug>1: print "eafs_read path: [%s] size:%d offset:%d" % (path, size, offset)
		#print "eafs_read: ", path, size, offset
		(chunkuuids, offset, chunkserver_uuids, chunkmd5s, next_chunkuuid) = self.master.get_chunkuuids_offset(path,size,offset)
		#print "eafs_read chunkserver_uuids: ", chunkserver_uuids
		#if self.debug>2:
		#print "eafs_read chunkuuids: ", chunkuuids
		self.update_chunkservers()
		chunks = []
		for chunkuuid in chunkuuids:
			#if self.debug>3: 
			#print "eafs_read chunkuuid: ", chunkuuid
			if chunkuuid in self.chunk_cache_read or chunkuuid in self.chunk_cache_read_wait:
				if chunkuuid in self.chunk_cache_read_wait:
					while self.chunk_cache_read_wait[chunkuuid]:
						time.sleep(1.0/1000.0)
				chunk = self.chunk_cache_read[chunkuuid]
				chunk_md5 = hashlib.md5(chunk).hexdigest()
				if chunkmd5s[chunkuuid]<>chunk_md5:
					print "eafs_read MD5 ERROR chunkuuid:%s chunk:%d master_md5:%s chunk_md5:%s" % (chunkuuid, len(chunk), chunkmd5s[chunkuuid], chunk_md5)
					chunk = self.get_chunk( chunkuuid, chunkserver_uuids )
				#print "eafs_read chunkuuid:%s chunk:%d master_md5:%s chunk_md5:%s" % (chunkuuid, len(chunk), chunkmd5s[chunkuuid], chunk_md5)
			else:
				chunk = self.get_chunk( chunkuuid, chunkserver_uuids )
			if chunk is None:
				raise Exception("read error, chunkserver unavailable: " + path)
			chunks.append(chunk)
		if len(chunks)==0:
			return None
		data = reduce(lambda x, y: x + y, chunks) # reassemble in order
		data = data[offset:offset+size]
		
		
		# Threading works but only for relatively slow media like video,
		# for copy it is not that good
		if next_chunkuuid is not None and next_chunkuuid not in self.chunk_cache_read and next_chunkuuid not in self.chunk_cache_read_wait:
			self.chunk_cache_read_wait[next_chunkuuid] = True
			get_chunk_thread = threading.Thread(None, self.get_chunk_thread, args=(next_chunkuuid, ))
			get_chunk_thread.daemon = True
			get_chunk_thread.start()
		
		
		#print "eafs_read size:%d offset:%d data:%d chunk:%d " % (size, offset, len(data), len(chunk))
		return data
	
	
	def get_chunk(self, chunkuuid, chunkserver_uuids):
		chunklocs = chunkserver_uuids[chunkuuid]
		done_chunkserver = []
		chunk = None
		#chunk_read = False
		while not ((chunk is not None) or (len(done_chunkserver)==len(chunklocs))):
			chunkidrnd = random.randint(0, len(chunklocs)-1)
			#print "Random: ", chunkidrnd, done_chunkserver, chunklocs
			if len(done_chunkserver)>0:
				while chunkidrnd in done_chunkserver:
					chunkidrnd = random.randint(0, len(chunklocs)-1)
					#print "Random2: ", chunkidrnd, done_chunkserver, chunklocs
			chunkloc = chunklocs[chunkidrnd]
			done_chunkserver.append(chunkidrnd)
			#if self.debug>2: 
			#print "Select chunkloc %d::%s from %d choices" % (chunkidrnd, chunkloc, len(chunklocs))
			try:
				# Read from chunkserver
				chunk_raw = self.chunkservers[chunkloc].rpc.read(chunkuuid)
				chunk = zlib.decompress(chunk_raw.data)
				#print "Read: ", chunkuuid, len(chunk)
				
				# Add to read cache
				self.chunk_cache_read[chunkuuid] = chunk
				
				#chunk_read = True
			except:
				print "Chunkserver %d failed %d remaining" % (chunkidrnd, len(chunklocs)-len(done_chunkserver))
		return chunk
	
	
	def get_chunk_thread(self, chunkuuid):
		#print "get_chunk_thread: %s" % chunkuuid
		chunkserver_uuids = {chunkuuid: self.master.get_chunklocs(chunkuuid)}
		self.get_chunk( chunkuuid, chunkserver_uuids )
		self.chunk_cache_read_wait[chunkuuid] = False
