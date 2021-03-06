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

import math,uuid,sys,os,time,operator,xmlrpclib,random,argparse
from eafslib import EAFSChunkServerRpc


class EAFSClient:
	def __init__(self, master_host):
		self.master = xmlrpclib.ServerProxy(master_host)
		self.chunkservers = {}

	def write(self, filename, data):
		if self.exists(filename):
			self.delete(filename)
		num_chunks = self.num_chunks(len(data))
		attributes = {"mode":"file", "atime":"", "ctime":"", "mtime":"", "attrs":""}
		chunkuuids = self.master.alloc(filename, num_chunks, attributes)
		self.write_chunks(chunkuuids, data)
	
	def update_chunkservers(self):
		chunkservers = self.master.get_chunkservers()
		#print "CHUNKSERVERS[RAW]: ", chunkservers
		for chunkserver in chunkservers:
			#print chunkserver
			if chunkserver['uuid'] not in self.chunkservers:
				self.chunkservers[chunkserver['uuid']] = EAFSChunkServerRpc( chunkserver['uuid'], chunkserver['address'] )
		
	def write_chunks(self, chunkuuids, data):
		chunks = [ data[x:x+self.master.get_chunksize()] \
			for x in range(0, len(data), self.master.get_chunksize()) ]
		#chunkservers = self.master.get_chunkservers()
		self.update_chunkservers()
		#print "CHUNKSERVERS: ", self.chunkservers
		for i in range(0, len(chunkuuids)): # write to each chunkserver
			chunkuuid = chunkuuids[i]
			chunklocs = self.master.get_chunklocs(chunkuuid)
			for chunkloc in chunklocs:
				#print "chunkloc: ", chunkloc
				self.chunkservers[chunkloc].rpc.write(chunkuuid, chunks[i])

	def num_chunks(self, size):
		return (size // self.master.get_chunksize()) \
			+ (1 if size % self.master.get_chunksize() > 0 else 0)

	def write_append(self, filename, data):
		if not self.exists(filename):
			raise Exception("append error, file does not exist: " + filename)
		num_append_chunks = self.num_chunks(len(data))
		append_chunkuuids = self.master.alloc_append(filename, \
			num_append_chunks)
		self.write_chunks(append_chunkuuids, data) 

	def exists(self, filename):
		return self.master.exists(filename)
		
	def read(self, filename): # get metadata, then read chunks direct
		if not self.exists(filename):
			raise Exception("read error, file does not exist: " + filename)
		chunks = []
		chunkuuids = self.master.get_chunkuuids(filename)
		#chunkservers = self.master.get_chunkservers()
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
				print "Select chunkloc %s from %d choices" % (chunkloc, len(chunklocs))
				try:
					chunk = self.chunkservers[chunkloc].rpc.read(chunkuuid)
					chunk_read = True
					done_chunkserver.append(chunkidrnd)
				except:
					print "Chunkserver %d failed" % chunkidrnd
			if not chunk_read:
				raise Exception("read error, chunkserver unavailable: " + filename)
			chunks.append(chunk)
		data = reduce(lambda x, y: x + y, chunks) # reassemble in order
		return data

	def delete(self, filename):
		self.master.delete(filename)

def main():
	parser = argparse.ArgumentParser(description='EAFS Simple Client')
	parser.add_argument('--master', dest='master', default='localhost:6799', help='Master server address')
	args = parser.parse_args()
	master = 'http://' + args.master
	
	client = EAFSClient(master)
	
	# test write, exist, read
	print "\nWriting..."
	#try:
	if False:
		client.write("/usr/python/readme.txt", """
		This file tells you all about python that you ever wanted to know.
		Not every README is as informative as this one, but we aim to please.
		Never yet has there been so much information in so little space.
		""")
	#except:
	#    print client.master.dump_metadata()
	print "File exists? ", client.exists("/usr/python/readme.txt")
	print client.read("/usr/python/readme.txt")
	# show structure of the filesystem
	print "\nMetadata Dump..." 
	print client.master.dump_metadata()

if __name__ == "__main__":
	main()

"""
	# test append, read after append
	#print "\nAppending..."
	#client.write_append("/usr/python/readme.txt", \
	#    "I'm a little sentence that just snuck in at the end.\n")
	#print client.read("/usr/python/readme.txt")

	# test delete
	#print "\nDeleting..."
	#client.delete("/usr/python/readme.txt")
	#print "File exists? ", client.exists("/usr/python/readme.txt")
	
	# test exceptions
	#print "\nTesting Exceptions..."
	#try:
	#    client.read("/usr/python/readme.txt")
	#except Exception as e:
	#    print "This exception should be thrown:", e
	#try:
	#    client.write_append("/usr/python/readme.txt", "foo")
	#except Exception as e:
	#    print "This exception should be thrown:", e
"""
