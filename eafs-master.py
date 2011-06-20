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

import math,uuid,os,time,operator,argparse,base64,sqlite3,threading,apsw

from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
import xmlrpclib

from eafslib import EAFSChunkServerRpc


db_filename = "eafs.db"


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
		self.size = 0
		self.chunks = []
		if attrs is not None:
			if 'id' in attrs:
				self.id = attrs['id']
			if 'parent' in attrs:
				self.parent = attrs['parent']
			if 'name' in attrs:
				self.name = attrs['name']
			if 'type' in attrs:
				self.type = attrs['type']
		self.size = 0
	
	def update_from_db(self, inode_raw):
		self.id = inode_raw[0]
		self.parent = inode_raw[1]
		self.name = inode_raw[2]
		self.type = inode_raw[3]
		self.perms = inode_raw[4]
		self.uid = int(inode_raw[5])
		self.gid = int(inode_raw[6])
		self.attrs = inode_raw[7]
		self.ctime = inode_raw[8]
		self.mtime = inode_raw[9]
		self.atime = inode_raw[10]
		self.links = int(inode_raw[11])
		self.size = int(inode_raw[12])


class EAFSMaster:
	def __init__(self, rootfs, init):
		self.debug = 0
		# Create root fs
		if not os.access(rootfs, os.W_OK):
			os.makedirs(rootfs)
		# Connect to DB
		self.db_path = os.path.join(rootfs,db_filename)
		#self.db = sqlite3.connect(self.db_path)
		self.db = apsw.Connection(self.db_path)
		self.db.setbusytimeout(500)
		if init==1:
			# Init DB
			c = self.db.cursor()
			try:
				c.execute("begin")
				c.execute('DROP TABLE IF EXISTS inode')
				c.execute('DROP TABLE IF EXISTS chunk')
				c.execute('DROP TABLE IF EXISTS inode_chunk')
				c.execute('DROP TABLE IF EXISTS server')
				c.execute('DROP TABLE IF EXISTS chunk_server')
				#self.db.commit()
				c.execute("commit")
			except:
				pass
			c.execute("begin")
			c.execute('CREATE TABLE inode (id INTEGER PRIMARY KEY AUTOINCREMENT, parent INTEGER, name text, type char(1), perms text, uid int, gid int, attrs text, ctime text, mtime text, atime text, links int, size int, UNIQUE(parent, name))')
			c.execute('CREATE TABLE chunk (uuid text, alloc_time TIMESTAMP, md5 TEXT, PRIMARY KEY(uuid))')
			c.execute('CREATE TABLE inode_chunk (inode_id INTEGER, chunk_uuid text, UNIQUE(inode_id,chunk_uuid))')
			c.execute('CREATE TABLE server (uuid text, address text, available INTEGER, last_seen DATETIME, size_total INTEGER, size_available INTEGER, PRIMARY KEY(uuid))')
			c.execute('CREATE TABLE chunk_server (chunk_uuid text, server_uuid text, UNIQUE(chunk_uuid,server_uuid))')
			#self.db.commit()
			c.execute("commit")
			c.close()
		self.root_inode_id = 0
		self.max_chunkservers = 100
		#self.chunksize = 4096000
		self.chunksize = 2048000
		self.replication_level = 2
		self.inodetable = {}
		self.inode_childrens = {}
		self.chunktable = {}
		self.chunkservers = {}
		self.load_chunkservers()
		self.load_inodes()
		self.load_chunks()
		#self.replicator = threading.Timer(30.0, self.hello)
		self.replicator = threading.Thread(None, self.replicator_thread)
		self.replicator.daemon = True
		self.replicator.start()
	
	
	def replicator_thread(self):
		#db = sqlite3.connect(self.db_path)
		db = apsw.Connection(self.db_path)
		db.setbusytimeout(500)
		while 1:
			#print "Regular replicator thread start"
			#print self.dump_metadata()
			# Check chunkservers
			c = db.cursor()
			chunkservers = self.chunkservers
			for chunkserver_uuid in chunkservers:
				chunkserver = chunkservers[chunkserver_uuid]
				try:
					(size_total, size_available) = chunkserver.rpc.stat()
					chunkserver.size_total = size_total
					chunkserver.size_available = size_available
					chunkserver.available = 1
					chunkserver.last_seen = time.time()
					#print "Chunkserver seen: ", chunkserver.uuid, chunkserver.size_total, chunkserver.size_available
				except:
					chunkserver.available = 0
					#print "Chunkserver _not_ responding: ", chunkserver.uuid
				#	c.execute("""update server set available=0 where uuid=?""", (chunkserver.uuid, ))
				#db = sqlite3.connect(self.db_path)
				c.execute("begin")
				c.execute("""update server set last_seen=?, available=?, size_total=?, size_available=? where uuid=?""", (chunkserver.last_seen, chunkserver.available, chunkserver.size_total, chunkserver.size_available, chunkserver.uuid))
				#db.commit()
				c.execute("commit")
				#c.close()
			
			# Check chunks
			chunks = self.chunktable
			c.execute('select chunk_uuid, alloc_time, count(*) as c from chunk_server left join chunk on (chunk.uuid=chunk_server.chunk_uuid) left join server on (server.uuid=chunk_server.server_uuid) where available=1 group by chunk_uuid, alloc_time having c<=1')
			chunks_to_replicate = []
			for row in c:
				alloc_time = row[1]
				if alloc_time is not None:
					time_diff = time.time()-alloc_time
					#print "Chunk time_diff:%d" % (time_diff)
					if time_diff>600:
						chunks_to_replicate.append(row[0])
			#c.close()
			#print "%d chunks to replicate" % len(chunks_to_replicate)
			num = min( 10, len(chunks_to_replicate) )
			num_replicated = 0
			for i in range( 0, num ):
				chunk_uuid = chunks_to_replicate[i]
				src_chunkserver_uuids = self.get_chunklocs( chunk_uuid )
				chunk_replicated = False
				src_chunkserver_id = 0
				while not chunk_replicated and src_chunkserver_id<len(src_chunkserver_uuids):
					src_chunkserver_uuid = src_chunkserver_uuids[src_chunkserver_id]
					if self.chunkservers[src_chunkserver_uuid].available==1:
						dest_chunkserver_uuid = None
						for chunkserver_uuid in self.chunkservers:
							if chunkserver_uuid<>src_chunkserver_uuid and self.chunkservers[chunkserver_uuid].available==1:
								dest_chunkserver_uuid = chunkserver_uuid
								break
						if dest_chunkserver_uuid is not None:
							src_chunkserver_address = self.chunkservers[src_chunkserver_uuid].address
							#print "replicate chunk %d::%s on %s(%s) to %s" % (i, chunk_uuid, src_chunkserver_uuid, src_chunkserver_address, dest_chunkserver_uuid)
							try:
								#if True:
								self.chunkservers[dest_chunkserver_uuid].rpc.replicate( chunk_uuid, src_chunkserver_uuid, src_chunkserver_address )
								num_replicated += 1
								chunk_replicated = True
							except:
								print "error connecting to chunkserver: ", dest_chunkserver_uuid
						else:
							pass
							#print "no chunk server to replicate %d::%s on %s" % (i, chunk_uuid, src_chunkserver_uuid)
					src_chunkserver_id += 1
			if num_replicated>0:
				print "%d of %d chunks replicated, total %d left" % (num_replicated, num, len(chunks_to_replicate)-num_replicated)
			
			#print "Regular replicator thread end"
			c.close()
			time.sleep( 30 )
	
	
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
			if row[1] not in self.inode_childrens:
				self.inode_childrens[row[1]] = []
			self.inode_childrens[row[1]].append( self.inodetable[row[0]] )
			num_inodetable += 1
		if num_inodetable==0:
			# Create root directory
			root_inode = self.create_inode( "/" )
		else:
			root_inode = self.get_inode_from_filename( "/" )
		self.root_inode_id = root_inode.id
		c.execute('select * from inode_chunk order by chunk_uuid')
		for row in c:
			self.inodetable[row[0]].chunks.append( row[1] )
		print " (%d)" % num_inodetable
	
	
	def load_chunks(self):
		print "LOAD CHUNKTABLE: ",
		c = self.db.cursor()
		c.execute('select chunk_server.chunk_uuid, chunk_server.server_uuid, chunk.md5 from chunk_server left join chunk on (chunk.uuid=chunk_server.chunk_uuid)')
		num_chunktable = 0
		for row in c:
			if row[0] not in self.chunktable:
				self.chunktable[row[0]] = {}
				self.chunktable[row[0]]['chunkserver_uuids'] = []
				self.chunktable[row[0]]['md5'] = row[2]
			self.chunktable[row[0]]['chunkserver_uuids'].append( row[1] )
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
			chunkserver = EAFSChunkServerRpc(chunkserver_uuid, chunkserver_address)
			chunkserver.available = row[2]
			chunkserver.last_seen = row[3]
			chunkserver.size_total = row[4]
			chunkserver.size_available = row[5]
			self.chunkservers[chunkserver_uuid] = chunkserver
			num_chunkservers += 1
		print " (%d)" % num_chunkservers
	
	
	def connect_chunkserver(self, chunkserver_address, chunkserver_uuid=None):
		if chunkserver_uuid is None or chunkserver_uuid=="":
			chunkserver_uuid = str(uuid.uuid1())
		if chunkserver_uuid not in self.chunkservers:
			self.chunkservers[chunkserver_uuid] = EAFSChunkServerRpc(chunkserver_uuid, chunkserver_address)
			c = self.db.cursor()
			c.execute("begin")
			c.execute("""insert into server (uuid, address) values (?,?)""", (chunkserver_uuid, chunkserver_address))
			#self.db.commit()
			c.execute("commit")
			c.close()
		return chunkserver_uuid
	
	
	def get_chunkservers(self):
		srvs = []
		for i in self.chunkservers:
			if self.chunkservers[i].available==1:
				srvs.append( {"uuid":self.chunkservers[i].uuid, "address":self.chunkservers[i].address} )
		return srvs
	
	
	def choose_chunkserver_uuids(self):
		uuids = []
		for i in self.chunkservers:
			if self.chunkservers[i].available==1:
				uuids.append( self.chunkservers[i].uuid )
			if len(uuids)>=self.replication_level:
				break
		return uuids
	
	
	def get_chunklocs(self, chunkuuid):
		return self.chunktable[chunkuuid]['chunkserver_uuids']
	
	
	def chunkserver_has_chunk(self, chunkserver_uuid, chunk_uuid, chunk_md5):
		# todo: check chunk md5
		#c = self.db.cursor()
		#c.execute("select md5 from chunk where uuid=?", (chunk_uuid, ) )
		#for row in c:
		if chunk_uuid not in self.chunktable:
			return False
		if 'md5' not in self.chunktable[chunk_uuid]:
			return False
		if chunk_md5<>self.chunktable[chunk_uuid]['md5']:
			print "MD5 mismatch: ", chunk_md5, self.chunktable[chunk_uuid]['md5']
			return False
		return self.alloc_chunks_to_chunkservers( chunk_uuid, [chunkserver_uuid] )
	
	
	def alloc(self, filename, num_chunks, attributes, chunk_md5):
		chunkuuids = self.alloc_chunks(num_chunks, chunk_md5)
		inode = EAFSInode(attributes)
		inode.name = os.path.basename( filename )
		self.save_inodechunktable(filename, chunkuuids, inode)
		return chunkuuids
	
	
	def alloc_chunks_to_chunkservers(self, chunk_uuid, chunkserver_uuids):
		if chunk_uuid not in self.chunktable:
			self.chunktable[chunk_uuid] = {}
			self.chunktable[chunk_uuid]['chunkserver_uuids'] = []
		c = self.db.cursor()
		c.execute("""PRAGMA synchronous = OFF""")
		c.execute("begin")
		for chunkserver_uuid in chunkserver_uuids:
			#print "alloc_chunks_to_chunkservers: ", chunk_uuid, chunkserver_uuid
			if chunkserver_uuid not in self.chunktable[chunk_uuid]['chunkserver_uuids']:
				self.chunktable[chunk_uuid]['chunkserver_uuids'].append( chunkserver_uuid )
				c.execute("""insert into chunk_server values (?, ?)""", (chunk_uuid, chunkserver_uuid))
		#self.db.commit()
		c.execute("commit")
		return True
	
	
	def alloc_chunks(self, num_chunks, chunk_md5):
		chunkuuids = []
		start = time.time()
		c = self.db.cursor()
		c.execute("""PRAGMA synchronous = OFF""")
		c.execute("begin")
		for i in range(0, num_chunks):
			# Generate UUID
			chunk_uuid = str(uuid.uuid1())
			# Insert new chunk
			chunkuuids.append(chunk_uuid)
			if chunk_uuid not in self.chunktable:
				self.chunktable[chunk_uuid] = {}
				self.chunktable[chunk_uuid]['chunkserver_uuids'] = []
			self.chunktable[chunk_uuid]['md5'] = chunk_md5
			c.execute("""insert into chunk values (?,?,?)""", (chunk_uuid, time.time(), chunk_md5))
			chunkserver_uuids = self.choose_chunkserver_uuids()
			# Insert chunk/chunkserver relation
			# No: this is the chunkserver job
		start_sql = time.time()
		#self.db.commit()
		c.execute("commit")
		if self.debug>0: print "[alloc_chunks] sql commit: ", (time.time()-start_sql)
		c.close()
		if self.debug>0: print "[alloc_chunks] total: ", len(chunkuuids), (time.time()-start)
		return chunkuuids
	
	
	def alloc_append(self, filename, num_append_chunks, chunk_md5):
		chunkuuids = self.get_chunkuuids(filename)
		append_chunkuuids = self.alloc_chunks(num_append_chunks, chunk_md5)
		self.save_inodechunktable(filename, append_chunkuuids)
		return append_chunkuuids
	
	
	def get_parent_inode_from_filename( self, filename ):
		dirname = os.path.dirname( filename )
		return self.get_inode_from_filename( dirname )
	
	
	def get_inode_from_filename( self, filename ):
		#print "get_inode_from_filename: ", filename
		c = self.db.cursor()
		fs = filename.split("/")[1:]
		if filename=="/":
			parent_inode_id = 0
		else:
			parent_inode_id = self.root_inode_id
		curpath = ""
		for fn in fs:
			#print "  Lookup inode: ", parent_inode_id, fn
			c.execute("""select * from inode where parent=? and name=?""", (parent_inode_id, fn) )
			inode_raw = None
			for row in c:
				inode_raw = row
			if inode_raw is not None:
				curpath = curpath + "/" + fn
				inode_id = inode_raw[0]
				parent_inode_id = inode_id
				#print "  Found inode: %d (%s : %s)" % (inode_id, curpath, filename)
				if curpath==filename:
					if inode_id in self.inodetable:
						inode = self.inodetable[inode_id]
					else:
						inode = EAFSInode()
						inode.update_from_db(inode_raw)
					return inode
			else:
				#print "  Not found"
				return False
		#print "get_inode_from_filename return: ", inode
		return inode
	
	
	def get_chunkuuids_offset(self, filename, size, offset):
		chunkuuids = self.get_chunkuuids(filename)
		start = int(math.floor( offset/self.chunksize ))
		new_offset = int(offset - (start*self.chunksize))
		num = 1 + math.ceil( (size+new_offset)/self.chunksize )
		end = int(start+num)
		if end>=len(chunkuuids):
			new_chunkuuids = chunkuuids[start:]
		else:
			new_chunkuuids = chunkuuids[start:end]
		next_chunkuuid = None
		if end+1<len(chunkuuids):
			next_chunkuuid = chunkuuids[end+1]
		#print chunkuuids
		#print "filename:%s size:%d offset:%d num:%d new_offset:%d start:%d end:%d" % (filename,size,offset,num,new_offset,start,end)
		#print new_chunkuuids
		chunkserver_uuids = {}
		chunkmd5s = {}
		for chunkuuid in new_chunkuuids:
			chunkserver_uuids[chunkuuid] = self.get_chunklocs(chunkuuid)
			chunkmd5s[chunkuuid] = self.get_chunkmd5(chunkuuid)
		return (new_chunkuuids, new_offset, chunkserver_uuids, chunkmd5s, next_chunkuuid)
	
	
	def get_chunkmd5(self, chunk_uuid):
		return self.chunktable[chunk_uuid]['md5']
	
	
	def get_chunkuuids(self, filename):
		inode = self.get_inode_from_filename( filename )
		if inode:
			"""
			c = self.db.cursor()
			c.execute("select chunk_uuid from inode_chunk where inode_id=? order by chunk_uuid", (inode.id, ) )
			chunks = []
			for row in c:
				chunks.append( row[0] )
			return chunks
			"""
			return self.inodetable[inode.id].chunks
		return None
	
	
	def exists(self, filename):
		if not self.get_inode_from_filename( filename ):
			return False
		return True
	
	
	def delete(self, filename):
		inode = self.get_inode_from_filename( filename )
		if inode:
			c = self.db.cursor()
			c.execute("begin")
			chunkuuids = self.inodetable[inode.id].chunks
			for chunkuuid in chunkuuids:
				c.execute("""delete from chunk where uuid=?""", (chunkuuid, ))
				c.execute("""delete from chunk_server where chunk_uuid=?""", (chunkuuid, ))
			del self.inodetable[inode.id]
			c.execute("""delete from inode_chunk where inode_id=?""", (inode.id, ))
			c.execute("""delete from inode where id=?""", (inode.id, ))
			#self.db.commit()
			c.execute("commit")
			c.close()
		return True
	
	
	def rename(self, filename, new_filename):
		#print "0 file: " + filename + " renamed to " + new_filename
		inode = self.get_inode_from_filename( filename )
		if inode:
			attributes = self.inodetable[inode.id]
			chunkuuids = attributes.chunks
			del self.inodetable[inode.id]
			c = self.db.cursor()
			c.execute("begin")
			c.execute("""delete from inode_chunk where inode_id=?""", (inode.id, ))
			c.execute("""delete from inode where id=?""", (inode.id, ))
			c.execute("commit")
			#self.db.commit()
			c.close()
			self.save_inodechunktable(new_filename,chunkuuids,attributes)
			print "file: " + filename + " renamed to " + new_filename
			return True
		return False
	
	
	def create_inode(self, filename, inode=None):
		#print "Create inode: ", filename
		create_filename = os.path.basename( filename )
		if inode is None:
			inode = EAFSInode({ "type":"d", "name":create_filename, "attrs":"", "ctime":"", "mtime":"", "atime":"" })
		if filename=="/":
			parent_inode_id = 0
		else:
			parent_inode = self.get_parent_inode_from_filename( filename )
			if not parent_inode:
				return False
			parent_inode_id = parent_inode.id
		c = self.db.cursor()
		c.execute("begin")
		c.execute("""insert into inode (parent,name,type,perms,uid,gid,attrs,ctime,mtime,atime,links,size) values (?,?,?,?,?,?,?,?,?,?,?,?)""", (parent_inode_id, create_filename, inode.type, "755", 0, 0, inode.attrs, inode.ctime, inode.mtime, inode.atime, 0, inode.size))
		#print "Create: ", "insert into inode (parent,name,type,perms,uid,gid,attrs,ctime,mtime,atime,links) values (%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" % (parent_inode_id, create_filename, attributes["type"], "wrxwrxwrx", 0, 0, attributes["attrs"], attributes["ctime"], attributes["mtime"], attributes["atime"], 0)
		c.execute("commit")
		#self.db.commit()
		c.close()
		inode = self.get_inode_from_filename( filename )
		if inode:
			self.inodetable[inode.id] = inode
			if parent_inode_id not in self.inode_childrens:
				self.inode_childrens[parent_inode_id] = []
			self.inode_childrens[parent_inode_id].append( inode )
		return inode
	
	
	def save_inodechunktable(self, filename, chunkuuids, save_inode=None):
		#print "save_inodechunktable: %s" % (filename, )
		inode = self.get_inode_from_filename( filename )
		c = self.db.cursor()
		c.execute("""PRAGMA synchronous = OFF""")
		if not inode:
			if save_inode is None:
				return False
			inode = self.create_inode( filename, save_inode )
			if not inode:
				return False
		self.inodetable[inode.id].chunks.extend( chunkuuids )
		#print "Save %d chunks for node %d" % (len(self.inodetable[inode.id].chunks), inode.id)
		c.execute("begin")
		for chunkuuid in chunkuuids:
			if self.debug>3: print "insert into inode_chunk values (%s, %s)" % (inode.id, chunkuuid)
			c.execute("""insert into inode_chunk values (?,?)""", (inode.id, chunkuuid))
		c.execute("commit")
		#self.db.commit()
		c.close()
	
	
	def list_files(self, filename):
		file_list = []
		parent_inode = self.get_inode_from_filename( filename )
		if not parent_inode:
			return file_list
		#print "list_files from %s: %d" % (filename, parent_inode.id)
		if parent_inode.id not in self.inode_childrens:
			return file_list
		#print "  LIST:"
		for inode in self.inode_childrens[parent_inode.id]:
			#print "    INODE:", inode
			name = base64.b64encode( inode.name.encode("utf-8") )
			file_list.append( {'name':name, 'type':inode.type, 'size':inode.size, 'mtime':inode.mtime} )
		return file_list
	
	
	def file_attr(self, filename):
		inode = self.get_inode_from_filename( filename )
		if inode:
			return inode
		return None
	
	
	def file_set_attr(self, filename, attr, val, op):
		inode = self.get_inode_from_filename( filename )
		c = self.db.cursor()
		c.execute("begin")
		if inode:
			if attr=='size':
				if op=='add':
					#print "file_set_attr: add new size: ", val
					self.inodetable[inode.id].size+= val
					c.execute("""update inode set size=? where id=?""", (inode.size, inode.id))
		#self.db.commit()
		c.execute("commit")
		c.close()
	
	
	def statfs(self, path):
		#print "statfs: %s" % path
		return dict(f_bsize=512, f_blocks=32768000, f_bavail=16384000)
	
	
	def dump_metadata(self):
		metadata = "Inodetable:\n"
		for inode_id, attributes in self.inodetable.items():
			metadata += "%d with %d chunks\n" % (inode_id, len(attributes.chunks))
		metadata += "Chunkservers: %d\n" % (len(self.chunkservers))
		return metadata



# Restrict to a particular path
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

