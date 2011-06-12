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

import math,uuid,os,time,operator,argparse

from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
import xmlrpclib

import sqlite3


db_filename = "eafs.db"
"""
create table file (name text, mode text, attrs text, ctime text, mtime text, atime text, PRIMARY KEY(name));
create table chunk (uuid text, PRIMARY KEY(uuid));
create table file_chunk (file_name text, chunk_uuid text, UNIQUE(file_name,chunk_uuid));
create table server (uuid text, address text, PRIMARY KEY(uuid));
create table chunk_server (chunk_uuid text, server_uuid text, UNIQUE(chunk_uuid,server_uuid));
"""
class EAFSInode:
	def __init__(self, attrs=None):
		self.id = ""
		self.name = ""
		self.type = ""
		self.parent = ""
		self.perms = ""
		self.uid = 0
		self.gid = 0
		self.attrs = ""
		self.ctime = ""
		self.mtime = ""
		self.atime = ""
		self.links = 0
		self.chunks = []
		if attrs is not None:
			self.id = attrs['id']
			self.name = attrs['name']
			self.type = attrs['type']
			self.parent = attrs['parent']
		self.size = 0
		#self.db = db
		#self.chunksize = chunksize
	
	def update_from_db(self, inode_raw):
		self.id = inode_raw[0]
		self.parent = inode_raw[1]
		self.name = inode_raw[2]
		self.type = inode_raw[3]
		self.perms = inode_raw[4]
		self.uid = inode_raw[5]
		self.gid = inode_raw[6]
		self.attrs = inode_raw[7]
		self.ctime = inode_raw[8]
		self.mtime = inode_raw[9]
		self.atime = inode_raw[10]
		self.links = inode_raw[11]
	#def load_chunks(self):
	#	c = self.db.cursor()
	#	c.execute("""select * from inode_chunk where inode_id=?""", (self.id, ) )
	#	for row in c:
	#		self.chunks.append( row[chunk_uuid] )
	#	self.size = len(self.chunks) * self.chunksize


class EAFSChunkserver:
	def __init__(self, uuid, address):
		self.uuid = uuid
		self.address = address
		self.rpc = xmlrpclib.ServerProxy(address)


class EAFSMaster:
	def __init__(self, rootfs, init):
		# Create root fs
		if not os.access(rootfs, os.W_OK):
			os.makedirs(rootfs)
		# Connect to DB
		self.db = sqlite3.connect(os.path.join(rootfs,db_filename))
		if init==1:
			# Init DB
			c = self.db.cursor()
			try:
				c.execute('drop table inode')
				c.execute('drop table chunk')
				c.execute('drop table inode_chunk')
				c.execute('drop table server')
				c.execute('drop table chunk_server')
				self.db.commit()
			except:
				pass
			#c.execute('create table file (name text, mode text, attrs text, ctime text, mtime text, atime text, PRIMARY KEY(name))')
			c.execute('create table inode (id INTEGER PRIMARY KEY AUTOINCREMENT, parent INTEGER, name text, type char(1), perms text, uid int, gid int, attrs text, ctime text, mtime text, atime text, links int, UNIQUE(parent, name))')
			c.execute('create table chunk (uuid text, PRIMARY KEY(uuid))')
			#c.execute('create table file_chunk (file_name text, chunk_uuid text, UNIQUE(file_name,chunk_uuid))')
			c.execute('create table inode_chunk (inode_id INTEGER, chunk_uuid text, UNIQUE(inode_id,chunk_uuid))')
			c.execute('create table server (uuid text, address text, PRIMARY KEY(uuid))')
			c.execute('create table chunk_server (chunk_uuid text, server_uuid text, UNIQUE(chunk_uuid,server_uuid))')
			self.db.commit()
			c.close()
		self.root_inode_id = 0
		self.max_chunkservers = 100
		#self.max_chunksperfile = 10000000
		self.chunksize = 4096
		#self.filetable = {}
		self.inodetable = {}
		self.inode_childrens = {}
		self.chunktable = {}
		self.chunkservers = {}
		self.load_chunkservers()
		#self.load_files()
		self.load_inodes()
		self.load_chunks()
	
	
	def get_chunksize(self):
		return self.chunksize
	
	
	def load_inodes(self):
		print "LOAD INODETABLE: ", 
		c = self.db.cursor()
		c.execute('select * from inode')
		num_inodetable = 0
		for row in c:
			inode = EAFSInode()
			inode.update_from_db( row )
			self.inodetable[row[0]] = inode
			#{"parent": row[1], "name":row[2], "type":row[3], "perms":row[4], "uid":row[5], "gid":row[6], "attrs":row[7], "ctime":row[8], "mtime":row[9], "atime":row[10], "links":row[11], "chunks":[]}
			if row[1] not in self.inode_childrens:
				self.inode_childrens[row[1]] = []
			self.inode_childrens[row[1]].append( self.inodetable[row[0]] )
			num_inodetable += 1
		if num_inodetable==0:
			# Create root directory
			root_inode = self.create_inode( "/", { "type":"d", "attrs":"", "ctime":"", "mtime":"", "atime":"" } )
			self.root_inode_id = root_inode.id
		else:
			root_inode = self.get_inode_from_filename( "/" )
			self.root_inode_id = root_inode.id
		c.execute('select * from inode_chunk')
		for row in c:
			self.inodetable[row[0]].chunks.append( row[1] )
		print " (%d)" % num_inodetable
	
	"""
	def load_files(self):
		print "LOAD FILETABLE: ", 
		c = self.db.cursor()
		c.execute('select * from file')
		num_filetable = 0
		for row in c:
			self.filetable[row[0]] = {"mode":row[1], "attrs":row[2], "ctime":row[3], "mtime":row[4], "atime":row[5],"chunks":[]}
			num_filetable += 1
		c.execute('select * from file_chunk')
		for row in c:
			self.filetable[row[0]]["chunks"].append( row[1] )
		print " (%d)" % num_filetable
	"""
	
	def load_chunks(self):
		print "LOAD CHUNKTABLE: ", 
		c = self.db.cursor()
		c.execute('select * from chunk_server')
		num_chunktable = 0
		for row in c:
			if row[0] in self.chunktable:
				self.chunktable[row[0]].append( row[1] )
			else:
				self.chunktable[row[0]] = [row[1]]
			num_chunktable += 1
		print " (%d)" % num_chunktable
	
	
	def load_chunkservers(self):
		print "LOAD CHUNKSERVERS: ", 
		c = self.db.cursor()
		c.execute('select * from server')
		num_chunkservers = 0
		for row in c:
			chunkserver_uuid = row[0]
			chunkserver_address = row[1]
			self.chunkservers[chunkserver_uuid] = EAFSChunkserver(chunkserver_uuid, chunkserver_address)
			num_chunkservers += 1
		print " (%d)" % num_chunkservers
	
	
	def connect_chunkserver(self, chunkserver_address, chunkserver_uuid=None):
		if chunkserver_uuid is None or chunkserver_uuid=="":
			chunkserver_uuid = str(uuid.uuid1())
		if chunkserver_uuid not in self.chunkservers:
			self.chunkservers[chunkserver_uuid] = EAFSChunkserver(chunkserver_uuid, chunkserver_address)
			c = self.db.cursor()
			c.execute("""insert into server values (?,?)""", (chunkserver_uuid, chunkserver_address))
			self.db.commit()
			c.close()
		return chunkserver_uuid
	
	
	def get_chunkservers(self):
		srvs = []
		for i in self.chunkservers:
			srvs.append( {"uuid":self.chunkservers[i].uuid, "address":self.chunkservers[i].address} )
		return srvs
	
	
	def choose_chunkserver_uuids(self):
		uuids = []
		for i in self.chunkservers:
			uuids.append( self.chunkservers[i].uuid )
		return uuids
	
	
	def alloc(self, filename, num_chunks, attributes): # return ordered chunkuuid list
		chunkuuids = self.alloc_chunks(num_chunks)
		self.save_filechunktable(filename, chunkuuids, attributes)
		return chunkuuids
	
	
	def alloc_chunks(self, num_chunks):
		chunkuuids = []
		c = self.db.cursor()
		for i in range(0, num_chunks):
			# Generate UUID
			chunk_uuid = str(uuid.uuid1())
			chunkserver_uuids = self.choose_chunkserver_uuids()
			self.chunktable[chunk_uuid] = chunkserver_uuids
			#print "INSERT CHUNK %s" % chunk_uuid
			c.execute("""insert into chunk values (?)""", (chunk_uuid, ))
			#print "INSERT CHUNK %s" % chunkserver.uuid
			for chunkserver_uuid in chunkserver_uuids:
				c.execute("""insert into chunk_server values (?, ?)""", (chunk_uuid, chunkserver_uuid))
			chunkuuids.append(chunk_uuid)
		self.db.commit()
		c.close()
		return chunkuuids
	
	
	def alloc_append(self, filename, num_append_chunks):
		chunkuuids = self.get_chunkuuids(filename)
		append_chunkuuids = self.alloc_chunks(num_append_chunks)
		chunkuuids.extend(append_chunkuuids)
		return append_chunkuuids
	
	
	def get_chunklocs(self, chunkuuid):
		return self.chunktable[chunkuuid]
	
	
	def get_parent_inode_from_filename( self, filename ):
		dirname = os.path.dirname( filename )
		print "  Dirname of %s: %s" % (filename, dirname)
		return self.get_inode_from_filename( dirname )
	
	def get_inode_from_filename( self, filename ):
		print "get_inode_from_filename: ", filename
		c = self.db.cursor()
		fs = filename.split("/")[1:]
		if filename=="/":
			parent_inode_id = 0
		else:
			parent_inode_id = self.root_inode_id
		curpath = ""
		print fs
		for fn in fs:
			print "  Lookup inode: ", parent_inode_id, fn,
			print "    select * from inode where parent=%d and name=%s" % (parent_inode_id, fn)
			c.execute("""select * from inode where parent=? and name=?""", (parent_inode_id, fn) )
			inode_raw = None
			for row in c:
				inode_raw = row
			if inode_raw is not None:
				curpath = curpath + "/" + fn
				inode_id = inode_raw[0]
				parent_inode_id = inode_id
				print "  Found inode: %d (%s : %s)" % (inode_id, curpath, filename)
				#inode = EAFSInode()
				#inode.update_from_db(inode_raw)
				if curpath==filename:
					#inode.load_chunks()
					if inode_id in self.inodetable:
						inode = self.inodetable[inode_id]
					else:
						inode = EAFSInode()
						inode.update_from_db(inode_raw)
					return inode
			else:
				print "  Failed"
				return False
		print "get_inode_from_filename return: ", inode
		return inode
		
	
	def get_chunkuuids(self, filename):
		inode = self.get_inode_from_filename( filename )
		if inode:
			return self.inodetable[inode.id].chunks
		return None
	
	
	def exists(self, filename):
		if not self.get_inode_from_filename( filename ):
			return False
		return True
		#return True if filename in self.filetable else False
	
	
	def delete(self, filename):
		inode = self.get_inode_from_filename( filename )
		if inode:
			c = self.db.cursor()
			chunkuuids = self.inodetable[inode.id].chunks
			for chunkuuid in chunkuuids:
				c.execute("""delete from chunk where uuid=?""", (chunkuuid, ))
				c.execute("""delete from chunk_server where chunk_uuid=?""", (chunkuuid, ))
			del self.inodetable[inode.id]
			c.execute("""delete from inode_chunk where inode_id=?""", (inode.id, ))
			c.execute("""delete from inode where id=?""", (inode.id, ))
			self.db.commit()
			c.close()
		return True
	
	
	def rename(self, filename, new_filename):
		inode = self.get_inode_from_filename( filename )
		#chunkuuids = self.get_chunkuuids(filename)
		#attributes = self.filetable[filename]
		attributes = self.inodetable[inode.id]
		del self.inodetable[inode.id]
		c = self.db.cursor()
		c.execute("""delete from inode_chunk where inode_id=?""", (inode.id, ))
		c.execute("""delete from inode where id=?""", (inode.id, ))
		self.db.commit()
		c.close()
		self.save_filechunktable(new_filename,chunkuuids,attributes)
		print "file: " + filename + " renamed to " + new_filename
	
	
	def create_inode(self, filename, attributes=None):
		print "Create inode: ", filename
		if attributes is None:
			attributes = { "type":"d", "attrs":"", "ctime":"", "mtime":"", "atime":"" }
		if filename=="/":
			parent_inode_id = 0
		else:
			parent_inode = self.get_parent_inode_from_filename( filename )
			if not parent_inode:
				return False
			parent_inode_id = parent_inode.id
		create_filename = os.path.basename( filename ) #.strip( "/" )
		c = self.db.cursor()
		c.execute("""insert into inode (parent,name,type,perms,uid,gid,attrs,ctime,mtime,atime,links) values (?,?,?,?,?,?,?,?,?,?,?)""", (parent_inode_id, create_filename, attributes["type"], "wrxwrxwrx", 0, 0, attributes["attrs"], attributes["ctime"], attributes["mtime"], attributes["atime"], 0))
		print "Create: ", "insert into inode (parent,name,type,perms,uid,gid,attrs,ctime,mtime,atime,links) values (%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" % (parent_inode_id, create_filename, attributes["type"], "wrxwrxwrx", 0, 0, attributes["attrs"], attributes["ctime"], attributes["mtime"], attributes["atime"], 0)
		self.db.commit()
		c.close()
		inode = self.get_inode_from_filename( filename )
		if inode:
			#print inode
			self.inodetable[inode.id] = inode
			if parent_inode_id not in self.inode_childrens:
				self.inode_childrens[parent_inode_id] = []
			self.inode_childrens[parent_inode_id].append( inode )
		return inode
	
	
	def save_filechunktable(self, filename, chunkuuids, attributes):
		inode = self.get_inode_from_filename( filename )
		c = self.db.cursor()
		if not inode:
			inode = self.create_inode( filename, attributes )
			if not inode:
				return False
		self.inodetable[inode.id].chunks = chunkuuids
		print "Save %d chunks for node %d" % (len(self.inodetable[inode.id].chunks), inode.id)
		for chunkuuid in chunkuuids:
			c.execute("""insert into inode_chunk values (?,?)""", (inode.id, chunkuuid))
		self.db.commit()
		c.close()
	
	def list_files(self, filename):
		file_list = []
		parent_inode = self.get_inode_from_filename( filename )
		if not parent_inode:
			return file_list
		#c = self.db.cursor()
		#print "List files: select * from inode where parent=%d" % parent_inode.id
		#c.execute("""select * from inode where parent=?""", (parent_inode.id, ) )
		#for row in c:
		print "list_files from %s: %d" % (filename, parent_inode.id)
		#print self.inode_childrens
		if parent_inode.id not in self.inode_childrens:
			return file_list
		#print "  LIST:"
		for inode in self.inode_childrens[parent_inode.id]:
			#print "    INODE:", inode
			#file_list.append( {"filename":row[2], "type":row[3]} )
			file_list.append( inode )
		return file_list
		
	"""
	def list_files(self, path):
		file_list = []
		if path=="/":
			paths = ['']
		else:
			paths = path.split("/")
		level = len(paths)
		for filename, attributes in self.filetable.items():
			filepaths = filename.split("/")
			if len(filepaths)<=len(paths):
				continue
			for i in range(0,len(paths)):
				if paths[i]==filepaths[i]:
					if level-1==i:
						file_list.append( {"filename":filepaths[i+1], "mode":attributes["mode"]} )
		return file_list
	"""
	
	def file_attr(self, filename):
		inode = self.get_inode_from_filename( filename )
		if inode:
			inode.size = len(inode.chunks) * self.chunksize
			print "Inode: %d Size: %d Chunks: %d ChunkSize:%d" % (inode.id, inode.size, len(inode.chunks), self.chunksize)
			#print inode
			return inode
		"""
		if path=="/":
			return {'mode':"dir",'size':0}
		if path in self.filetable:
			attributes = self.filetable[path]
			return {'mode':attributes["mode"],'size':len(attributes["chunks"])*self.chunksize}
		"""
		return None
	
	
	def dump_metadata(self):
		metadata = "Inodetable:\n"
		for inode_id, attributes in self.inodetable.items():
			metadata += "%d with %d chunks\n" % (inode_id, len(attributes.chunks))
		metadata += "Chunkservers: %d\n" % (len(self.chunkservers))
		#metadata += "Chunkserver Data:\n"
		#for chunkuuid, chunklocs in sorted(self.chunktable.iteritems(), key=operator.itemgetter(1)):
		#    for chunkloc in chunklocs:
		#        chunk = self.chunkservers[chunkloc].rpc.read(chunkuuid)
		#        metadata += "%s, %s\n" % (chunkloc, chunkuuid)
		return metadata



# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
	rpc_paths = ('/RPC2',)



def main():
	parser = argparse.ArgumentParser(description='EAFS Master Server')
	parser.add_argument('--host', dest='host', default='localhost', help='Bind to address')
	parser.add_argument('--port', dest='port', default=6799, type=int, help='Bind to port')
	parser.add_argument('--rootfs', dest='rootfs', default='/tmp', help='Save data to')
	parser.add_argument('--init', dest='init', default=0, type=int, help='Init DB: reset ALL meta data')
	args = parser.parse_args()
	
	# Create server
	server = SimpleXMLRPCServer((args.host, args.port), requestHandler=RequestHandler, allow_none=True, logRequests=False)
	server.register_introspection_functions()
	server.register_instance(EAFSMaster(args.rootfs, args.init))
	server.serve_forever()


if __name__ == "__main__":
	main()

