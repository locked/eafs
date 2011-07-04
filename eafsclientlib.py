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
	
	
	def write_chunk(self, chunk_uuid, data):
		#start = time.time()
		write_data = data #zlib.compress(chunks[i])
		#print "[write_chunks] compress: ", (time.time()-start)
		write_data_xmlrpc = xmlrpclib.Binary(write_data)
		chunkserver_uuids = self.master.choose_chunkserver_uuids()
		chunkserver_writes = 0
		#print "[write_chunk] chunkserver_uuids: ", chunkserver_uuids
		for chunkserver_uuid in chunkserver_uuids:
			if chunkserver_uuid in self.chunkservers:
				#print "chunkserver_uuid: ", chunkserver_uuid
				#if self.debug>3: print "Chunk size: ", i, len(chunks[i])
				#start = time.time()
				#print "writing..."
				data_md5 = hashlib.md5(data).hexdigest()
				print "[write_chunk] set_md5: %s" % data_md5
				self.master.set_chunk_attrs( chunk_uuid, { 'md5':data_md5, 'size':len(data) } )
				try:
					write_data_len = int( self.chunkservers[chunkserver_uuid].rpc.write(chunk_uuid, write_data_xmlrpc) )
					#print "done"
					#print "Wrote on chunkserver %s: %d" % (chunkserver_uuid, write_data_len)
					#print "[write_chunks] rpc.write: ", (time.time()-start)
					if write_data_len==len(write_data):
						chunkserver_writes += 1
					else:
						print "Write on chunkserver %s failed (%d/%d)" % (chunkserver_uuid, write_data_len, len(write_data))
				except:
					#else:
					print "Chunkserver %s failed" % chunkserver_uuid
					if chunkserver_uuid in self.chunkservers:
						del self.chunkservers[chunkserver_uuid]
		if chunkserver_writes==0:
			raise Exception("write_chunks error, not enough chunkserver available")
		return True
	
	
	def write_chunks(self, chunkuuids, data):
		chunks = [ data[x:x+self.chunk_size] \
			for x in range(0, len(data), self.chunk_size) ]
		
		#start = time.time()
		self.update_chunkservers()
		#print "[write_chunks] update_chunkservers: ", (time.time()-start)
		start_total = time.time()
		for i in range(0, len(chunkuuids)): # write to each chunkserver
			chunkuuid = chunkuuids[i]
			self.write_chunk( chunkuuid, chunks[i] )
		#print "[write_chunks] total writes: ", (time.time()-start_total)
		return True
	
	
	def num_chunks(self, size):
		return (size // self.chunk_size) \
			+ (1 if size % self.chunk_size > 0 else 0)
	
	
	def exists(self, path):
		return self.master.exists(path)
	
	
	def flush_low(self, path, fh, chunk_uuid=0):
		if fh not in self.chunk_cache:
			return False
		if chunk_uuid<>0:
			if chunk_uuid not in self.chunk_cache[fh]:
				return False
			chunk_uuids = [ chunk_uuid ]
		else:
			chunk_uuids = [ uuid for uuid in self.chunk_cache[fh] ]
		
		self.update_chunkservers()
		
		for chunk_uuid in chunk_uuids:
			data = self.chunk_cache[fh][chunk_uuid]
			self.write_chunk(chunk_uuid, data)
			#del self.chunk_cache[fh][chunk_uuid]
		return True
	"""
	def flush_low(self, path, fh, chunk_offset=0):
		if fh not in self.chunk_cache:
			return False
		if chunk_offset not in self.chunk_cache[fh]:
			return False
		#if len(self.chunk_cache[fh][chunk_offset])>self.chunk_size:
		#	data = self.chunk_cache[fh][0:self.chunk_size]
		#else:
		data = "".join( self.chunk_cache[fh][chunk_offset] )
		#print "data:", data
		#print "cache:", self.chunk_cache[path]
		#print "Flush Low: fh:%d path:%s data:%d chunk_cache:%d chunk_size:%d" % (fh, path, len(data), len(self.chunk_cache[fh]), self.chunk_size)
		if len(data)>0:
			num_append_chunks = self.num_chunks(len(data))
			data_md5 = hashlib.md5(data).hexdigest()
			#print "Flush Low: chunks:%d" % (num_append_chunks)
			if not self.exists(path):
				attributes = {"type":"f", "atime":int(time.time()), "ctime":int(time.time()), "mtime":int(time.time()), "size":0, "links":1, "attrs":""}
				chunkuuids = self.master.alloc(path, num_append_chunks, attributes, data_md5)
			else:
				chunkuuids = self.master.alloc_append(path, num_append_chunks, data_md5)
			#print "Flush Low: append_chunkuuids:%d" % (len(data))
			
			if self.write_chunks(chunkuuids, data):
				#print "Set size %s: %d" % (path, len(data))
				self.master.file_set_attr(path, 'size', int(len(data)), 'add')
			
			del self.chunk_cache[fh][chunk_offset]
			#if fh in self.chunk_cache:
			#	self.chunk_cache[fh] = self.chunk_cache[fh][self.chunk_size:]
			#else:
			#	return 0
		return True
	"""
	
	
	def eafs_flush(self, path, fh):
		print "** FUSE called flush( %s ) **" % path
		self.flush_low( path, fh )
		if fh in self.chunk_cache:
			del self.chunk_cache[fh]
		return True
	
	
	def accumulate(self, path, data, fh):
		self.accumulate_append( path, data, 0, fh )
		"""
		chunk_offset = 0
		if fh not in self.chunk_cache:
			self.chunk_cache[fh] = {}
			self.chunk_cache[fh][chunk_offset] = ""
		self.chunk_cache[fh][chunk_offset] += data
		if len(self.chunk_cache[fh][chunk_offset])>=self.chunk_size:
			self.flush_low( path, fh )
		return len(data)
		"""
	
	
	def alloc_chunk( self, fh, path, data, attributes, chunkserver_uuids ):
		num_chunks = self.num_chunks(len(data))
		print "[accumulate_append] Alloc: %s num_chunks:%d size:%d" % (path, num_chunks, len(data))
		chunkuuids = self.master.alloc(path, num_chunks, attributes)
		print "[accumulate_append] Alloc Done"
		for chunkuuid in chunkuuids:
			self.chunk_cache[fh][chunkuuid] = None
			chunkserver_uuids[chunkuuid] = []
			for chunkserver_uuid in self.chunkservers:
				chunkserver_uuids[chunkuuid].append( chunkserver_uuid )
		return (chunkuuids, chunkserver_uuids)
	
	
	def accumulate_append(self, path, data, offset, fh):
		size = len(data)
		chunk_data_offset = offset
		
		chunkuuids = None
		if self.exists( path ):
			#print "[accumulate_append] Get chunks/offset: path:%s size:%d offset:%d" % (path, size, offset)
			(chunkuuids, chunk_data_offset, chunkserver_uuids, chunkmd5s, next_chunkuuid) = self.master.get_chunkuuids_offset(path,size,offset)
			attributes = 0
		else:
			#print "[accumulate_append] Set attributes for path:%s" % path
			attributes = {"type":"f", "atime":int(time.time()), "ctime":int(time.time()), "mtime":int(time.time()), "size":0, "links":1, "attrs":""}
		
		if fh not in self.chunk_cache:
			self.chunk_cache[fh] = {}
		
		offset_end = chunk_data_offset+size
		
		if chunkuuids is None or chunkuuids==[]:
			(chunkuuids, chunkserver_uuids) = self.alloc_chunk( fh, path, data, attributes, chunkserver_uuids )
		
		print "[accumulate_append] chunkuuids: ", chunkuuids
		
		for chunkuuid in chunkuuids:
			print "[accumulate_append] rewrite chunk %s" % (chunkuuid)
			if chunkuuid not in self.chunk_cache[fh]:
				chunk = self.get_chunk( chunkuuid, chunkserver_uuids )
				#print "[accumulate_append] chunk (GET): ", chunk
			else:
				chunk = self.chunk_cache[fh][chunkuuid]
				#print "[accumulate_append] chunk (CACHE): ", chunk
			
			initial_size = 0
			if chunk is not None:
				initial_size = len(chunk)
			
			initial_md5 = ''
			if chunk is not None:
				initial_md5 = hashlib.md5(chunk).hexdigest()
			
			# Start
			a = ""
			if chunk_data_offset>0 and chunk is not None:
				a = chunk[0:chunk_data_offset]
				#print "[accumulate_append] a: 0:%d len:%d" % (offset, len(a))
			
			# Middle (data)
			#print "[accumulate_append] offset:%d chunk_data_offset:%d offset_end:%d self.chunk_size:%d -- size:%d" % (offset, chunk_data_offset, offset_end, self.chunk_size, len(data))
			new_offset = size
			new_data = ""
			if offset_end>self.chunk_size:
				new_offset = size - (offset_end - self.chunk_size)
				print "  new offset:", new_offset
				new_data = data[new_offset:]
				#offset_end = self.chunk_size
				print "  Alloc new chunkuuids:", len(new_data)
				(new_chunkuuids, chunkserver_uuids) = self.alloc_chunk( fh, path, new_data, attributes, chunkserver_uuids )
				for new_chunkuuid in new_chunkuuids:
					print "  Append chunkuuid:", new_chunkuuid
					chunkuuids.append( new_chunkuuid )
			
			# End
			c = ""
			if offset_end<self.chunk_size and chunk is not None:
				c = chunk[offset_end+1:self.chunk_size]
				#print "[accumulate_append] c: %d:%d len:%d" % (offset_end, self.chunk_size, len(c))
			
			print "  [accumulate_append] before // a:%d data:%d c:%d" % ( len(a), len(data[0:new_offset]), len(c))
			self.chunk_cache[fh][chunkuuid] = a + data[0:new_offset] + c
			#print "a / data / c", a, data, c
			
			print "  [accumulate_append] offset:%d chunk_data_offset:%d offset_end:%d new_offset:%d -- size:%d chunk_size:%d a:%d c:%d" % (offset, chunk_data_offset, offset_end, new_offset, len(data), len(self.chunk_cache[fh][chunkuuid]), len(a), len(c))
			
			#new_size = len( self.chunk_cache[fh][chunkuuid] )
			#print "new_size:%d initial_size:%d" % (new_size, initial_size)
			#if new_size>initial_size:
			#	self.master.file_set_attr(path, 'size', new_size-initial_size, 'add')
			
			chunk_data_offset = 0
			data = new_data
			offset_end = chunk_data_offset+len(data)
			
			#new_md5 = hashlib.md5(self.chunk_cache[fh][chunkuuid]).hexdigest()
			#if initial_md5<>new_md5:
			self.flush_low( path, fh, chunkuuid )
		#print "[accumulate_append] all done"
		return size
		"""
		if chunk_offset not in self.chunk_cache[fh]:
			current_data = [] #"" for i in range(0,self.chunk_size)]
			self.chunk_cache[fh][chunk_offset] = current_data
			if self.exists( path ):
				read_data = self.eafs_read( path, size, offset )
				if read_data is not None:
					current_data = [i for i in read_data]
			for d in current_data:
				self.chunk_cache[fh][chunk_offset].append( d )
			print "[accumulate_append] Load chunk data: path:%s size:%d offset:%d currentdata:%d alldata:%d" % (path, size, offset, len(current_data), len(self.chunk_cache[fh][chunk_offset]))
		print "[accumulate_append] chunk_offset: %d" % chunk_data_offset
		offset_local = chunk_data_offset;
		for d in data:
			#print "[accumulate_append] set data: %d / %d / %d" % (fh, chunk_offset, offset_local)
			if len(self.chunk_cache[fh][chunk_offset])>offset_local:
				self.chunk_cache[fh][chunk_offset][offset_local] = d
			else:
				self.chunk_cache[fh][chunk_offset].append( d )
			if len(self.chunk_cache[fh][chunk_offset])>=self.chunk_size:
				print "[accumulate_append] flush: %d / %d / %d" % (chunk_offset, len(self.chunk_cache[fh][chunk_offset]), self.chunk_size)
				self.flush_low( path, fh, chunk_offset )
				break;
				#return len(data)+self.accumulate_append(path, data, offset+1, fh))
			else:
				offset_local += 1
		return len(data)
		"""
	
	
	def eafs_write(self, path, data, fh): #, attributes
		#if self.exists(path):
		#	self.master.delete(path)
		#num_chunks = self.num_chunks(len(data))
		#chunkuuids = self.master.alloc(path, num_chunks, attributes)
		#print "eafs_write: fh:%d path:%s data:%d" % (fh, path, len(data))
		self.accumulate_append( path, data, 0, fh )
		#self.write_chunks(chunkuuids, data)
		return len(data)
	
	
	def eafs_write_append(self, path, data, offset, fh):
		#if not self.exists(path):
		#	raise Exception("append error, file does not exist: " + path)
		#num_append_chunks = self.num_chunks(len(data))
		#print "[eafs_write_append] DATA SIZE, NUM CHUNKS:", len(data), num_append_chunks
		#start = time.time()
		#append_chunkuuids = self.master.alloc_append(path, num_append_chunks)
		#if self.debug>0: print "[eafs_write_append] master.alloc_append: ", (time.time()-start)
		#self.write_chunks(append_chunkuuids, data)
		#print "[eafs_write_append] fh:%d path:%s data:%d offset:%d" % (fh, path, len(data), offset)
		return self.accumulate_append( path, data, offset, fh )
	
	
	def eafs_read(self, path, size, offset):
		self.update_chunkservers()
		if not self.exists(path):
			raise Exception("read error, file does not exist: " + path)
		#if self.debug>1: print "eafs_read path: [%s] size:%d offset:%d" % (path, size, offset)
		#print "eafs_read: ", path, size, offset
		(chunkuuids, offset, chunkserver_uuids, chunkmd5s, next_chunkuuid) = self.master.get_chunkuuids_offset(path,size,offset)
		#print "eafs_read chunkserver_uuids: ", chunkserver_uuids
		#if self.debug>2:
		#print "eafs_read chunkuuids: ", chunkuuids
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
		print "[get_chunk] chunkuuid:%s chunklocs:%s" % (chunkuuid, "".join(chunklocs))
		done_chunkserver = []
		chunk = None
		#chunk_read = False
		self.update_chunkservers()
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
			#try:
			if True:
				# Read from chunkserver
				#print "reading...",
				chunk_raw = self.chunkservers[chunkloc].rpc.read(chunkuuid)
				#print "done"
				chunk = chunk_raw.data #zlib.decompress(chunk_raw.data)
				#print "Read: ", chunkuuid, len(chunk)
				
				# Add to read cache
				self.chunk_cache_read[chunkuuid] = chunk
				
				#chunk_read = True
			#except:
			else:
				print "Chunkserver %s:%d failed %d remaining" % (chunkloc, chunkidrnd, len(chunklocs)-len(done_chunkserver))
				#try:
				#	del self.chunkservers[chunkloc]
				#except:
				#	pass
		return chunk
	
	
	def get_chunk_thread(self, chunkuuid):
		#print "get_chunk_thread: %s" % chunkuuid
		chunkserver_uuids = {chunkuuid: self.master.get_chunklocs(chunkuuid)}
		self.get_chunk( chunkuuid, chunkserver_uuids )
		self.chunk_cache_read_wait[chunkuuid] = False
