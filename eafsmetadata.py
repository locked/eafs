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
import MySQLdb


class EAFSMetaDataCursor:
	def __init__(self, parent):
		self.parent = parent
		self.db = self.parent.db
		self.cursor = self.begin()
		#print "[BEGIN] Cursor: ", self.cursor


class EAFSMetaData:
	def __init__(self, db_path):
		self.db_path = db_path
		self.db = None





class EAFSMetaDataSQLiteCursor(EAFSMetaDataCursor):
	def begin(self):
		c = self.db.cursor()
		c.execute("""PRAGMA synchronous = OFF""")
		c.execute("begin")
		return c
	
	def commit(self):
		#print "[COMMIT] Cursor: ", self.cursor
		self.cursor.execute("commit")
		self.cursor.close()


class EAFSMetaDataSQLite(EAFSMetaData):
	def connect(self):
		self.db = apsw.Connection(self.db_path)
		self.db.setbusytimeout(500)
	
	def get_cursor(self):
		return EAFSMetaDataSQLiteCursor( self )
	
	def init(self):
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
	
	def get_inodes(self):
		c = self.db.cursor()
		c.execute('select * from inode')
		rows = []
		for row in c:
			rows.append( row )
		return rows
	
	def get_inode_chunks(self):
		c = self.db.cursor()
		c.execute('select * from inode_chunk order by chunk_uuid')
		rows = []
		for row in c:
			rows.append( row )
		return rows
	
	def get_chunks_and_servers(self):
		c = self.db.cursor()
		c.execute('select chunk_server.chunk_uuid, chunk_server.server_uuid, chunk.md5 from chunk_server left join chunk on (chunk.uuid=chunk_server.chunk_uuid)')
		rows = []
		for row in c:
			rows.append( row )
		return rows
	
	def get_servers(self):
		c = self.db.cursor()
		c.execute('select * from server')
		rows = []
		for row in c:
			rows.append( row )
		return rows

	def add_server(self, chunkserver_uuid, chunkserver_address):
		c = self.db.cursor()
		c.execute("begin")
		c.execute("""insert into server (uuid, address) values (?,?)""", (chunkserver_uuid, chunkserver_address))
		c.execute("commit")
		c.close()
	
	def add_chunk_in_server(self, chunk_uuid, chunkserver_uuid, cursor=None):
		if cursor is None:
			cursor = self.get_cursor()
		cursor.cursor.execute("""insert into chunk_server values (?, ?)""", (chunk_uuid, chunkserver_uuid))
	
	def add_chunk(self, chunk_uuid, chunk_timestamp, chunk_md5, cursor=None):
		if cursor is None:
			cursor = self.get_cursor()
		cursor.cursor.execute("""insert into chunk values (?,?,?)""", (chunk_uuid, chunk_timestamp, chunk_md5))
	
	def search_inode_with_parent(self, parent_inode_id, filename):
		c = self.db.cursor()
		c.execute("""select * from inode where parent=? and name=?""", (parent_inode_id, filename) )
		rows = []
		for row in c:
			rows.append( row )
		return rows
	
	def del_chunk( self, chunkuuid, cursor=None ):
		if cursor is None:
			cursor = self.get_cursor()
		cursor.cursor.execute("""delete from chunk where uuid=?""", (chunkuuid, ))
	
	def del_chunk_server( self, chunkuuid, cursor=None ):
		if cursor is None:
			cursor = self.get_cursor()
		cursor.cursor.execute("""delete from chunk_server where chunk_uuid=?""", (chunkuuid, ))
	
	def del_inode_chunk( self, inode_id, cursor=None ):
		if cursor is None:
			cursor = self.get_cursor()
		cursor.cursor.execute("""delete from inode_chunk where inode_id=?""", (inode_id, ))
	
	def del_inode( self, inode_id, cursor=None ):
		if cursor is None:
			cursor = self.get_cursor()
		cursor.cursor.execute("""delete from inode where id=?""", (inode_id, ))
	
	def add_inode( self, parent_inode_id, filename, inode, cursor=None ):
		self_commit = False
		if cursor is None:
			cursor = self.get_cursor()
			self_commit = True
		cursor.cursor.execute("""insert into inode (parent,name,type,perms,uid,gid,attrs,ctime,mtime,atime,links,size) values (?,?,?,?,?,?,?,?,?,?,?,?)""", (parent_inode_id, filename, inode.type, "755", 0, 0, inode.attrs, inode.ctime, inode.mtime, inode.atime, 0, inode.size))
		#print "Create: ", "insert into inode (parent,name,type,perms,uid,gid,attrs,ctime,mtime,atime,links) values (%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" % (parent_inode_id, create_filename, attributes["type"], "wrxwrxwrx", 0, 0, attributes["attrs"], attributes["ctime"], attributes["mtime"], attributes["atime"], 0)
		if self_commit:
			cursor.commit()

	def add_inode_chunk( self, inode_id, chunkuuid, cursor=None ):
		self_commit = False
		if cursor is None:
			cursor = self.get_cursor()
			self_commit = True
		cursor.cursor.execute("""insert into inode_chunk values (?,?)""", (inode_id, chunkuuid))
		if self_commit:
			cursor.commit()

	def set_inode_size( self, inode, cursor=None ):
		self_commit = False
		if cursor is None:
			cursor = self.get_cursor()
			self_commit = True
		cursor.cursor.execute("""update inode set size=? where id=?""", (inode.size, inode.id))
		if self_commit:
			cursor.commit()
	
	def set_server( self, chunkserver, cursor ):
		cursor.cursor.execute("""update server set last_seen=?, available=?, size_total=?, size_available=? where uuid=?""", (chunkserver.last_seen, chunkserver.available, chunkserver.size_total, chunkserver.size_available, chunkserver.uuid))
	
	def get_missing_replicate_chunks_and_servers( self ):
		c = self.db.cursor()
		c.execute('select chunk_uuid, alloc_time, count(*) as c from chunk_server left join chunk on (chunk.uuid=chunk_server.chunk_uuid) left join server on (server.uuid=chunk_server.server_uuid) where available=1 group by chunk_uuid, alloc_time having c<=1')
		rows = []
		for row in c:
			rows.append( row )
		return rows






class EAFSMetaDataMySQLCursor(EAFSMetaDataCursor):
	def begin(self):
		try:
			c = self.parent.db.cursor()
		except (AttributeError, MySQLdb.OperationalError):
			self.parent.connect()
			c = self.parent.db.cursor()
		#c.execute("begin")
		return c
	
	def commit(self):
		#print "[COMMIT] Cursor: ", self.cursor
		self.parent.db.commit()
		#self.db.close()


class EAFSMetaDataMySQL(EAFSMetaData):
	def connect(self):
		self.db = MySQLdb.connect( host = "localhost", user = "eafs", passwd = "iopiop", db = "eafs" )
	
	def get_cursor(self):
		return EAFSMetaDataMySQLCursor( self )
	
	def get_read_cursor(self):
		try:
			c = self.db.cursor()
		except (AttributeError, MySQLdb.OperationalError):
			self.connect()
			c = self.db.cursor()
		return c
	
	def init(self):
		try:
			cursor = self.get_cursor()
			cursor.cursor.execute("begin")
			cursor.cursor.execute('DROP TABLE IF EXISTS inode')
			cursor.cursor.execute('DROP TABLE IF EXISTS chunk')
			cursor.cursor.execute('DROP TABLE IF EXISTS inode_chunk')
			cursor.cursor.execute('DROP TABLE IF EXISTS server')
			cursor.cursor.execute('DROP TABLE IF EXISTS chunk_server')
			cursor.commit()
		except:
			pass
		cursor = self.get_cursor()
		cursor.cursor.execute('CREATE TABLE inode (id INTEGER PRIMARY KEY AUTO_INCREMENT, parent INTEGER, name VARCHAR(900), type char(1), perms VARCHAR(16), uid int, gid int, attrs VARCHAR(64), ctime VARCHAR(64), mtime VARCHAR(64), atime VARCHAR(68), links int, size int, UNIQUE(parent, name))')
		cursor.cursor.execute('CREATE TABLE chunk (uuid VARCHAR(128), alloc_time TIMESTAMP, md5 VARCHAR(128), PRIMARY KEY(uuid))')
		cursor.cursor.execute('CREATE TABLE inode_chunk (inode_id INTEGER, chunk_uuid VARCHAR(128), UNIQUE(inode_id,chunk_uuid))')
		cursor.cursor.execute('CREATE TABLE server (uuid VARCHAR(128), address VARCHAR(2048), available INTEGER, last_seen TIMESTAMP, size_total INTEGER, size_available INTEGER, PRIMARY KEY(uuid))')
		cursor.cursor.execute('CREATE TABLE chunk_server (chunk_uuid VARCHAR(128), server_uuid VARCHAR(128), UNIQUE(chunk_uuid,server_uuid))')
		cursor.commit()
	
	def get_inodes(self):
		c = self.get_read_cursor()
		c.execute('select * from inode')
		rows = c.fetchall()
		return rows
	
	def get_inode_chunks(self):
		c = self.get_read_cursor()
		c.execute('select * from inode_chunk order by chunk_uuid')
		rows = c.fetchall()
		return rows
	
	def get_chunks_and_servers(self):
		c = self.get_read_cursor()
		c.execute('select chunk_server.chunk_uuid, chunk_server.server_uuid, chunk.md5 from chunk_server left join chunk on (chunk.uuid=chunk_server.chunk_uuid)')
		rows = c.fetchall()
		return rows
	
	def get_servers(self):
		c = self.get_read_cursor()
		c.execute('select uuid, address, available, UNIX_TIMESTAMP(last_seen) AS last_seen, size_total, size_available from server')
		rows = c.fetchall()
		return rows

	def add_server(self, chunkserver_uuid, chunkserver_address):
		cursor = self.get_cursor()
		cursor.cursor.execute("""insert into server (uuid, address) values (%s,%s)""", (chunkserver_uuid, chunkserver_address))
		cursor.commit()
	
	def add_chunk_in_server(self, chunk_uuid, chunkserver_uuid, cursor=None):
		if cursor is None:
			cursor = self.get_cursor()
		cursor.cursor.execute("""insert into chunk_server values (%s, %s)""", (chunk_uuid, chunkserver_uuid))
	
	def add_chunk(self, chunk_uuid, chunk_timestamp, chunk_md5, cursor=None):
		if cursor is None:
			cursor = self.get_cursor()
		cursor.cursor.execute("""insert into chunk values (%s,FROM_UNIXTIME(%s),%s)""", (chunk_uuid, chunk_timestamp, chunk_md5))
	
	def search_inode_with_parent(self, parent_inode_id, filename):
		c = self.db.cursor()
		c.execute("""select * from inode where parent=%s and name=%s""", (parent_inode_id, filename) )
		rows = c.fetchall()
		return rows
	
	def del_chunk( self, chunkuuid, cursor=None ):
		if cursor is None:
			cursor = self.get_cursor()
		cursor.cursor.execute("""delete from chunk where uuid=%s""", (chunkuuid, ))
	
	def del_chunk_server( self, chunkuuid, cursor=None ):
		if cursor is None:
			cursor = self.get_cursor()
		cursor.cursor.execute("""delete from chunk_server where chunk_uuid=%s""", (chunkuuid, ))
	
	def del_inode_chunk( self, inode_id, cursor=None ):
		if cursor is None:
			cursor = self.get_cursor()
		cursor.cursor.execute("""delete from inode_chunk where inode_id=%s""", (inode_id, ))
	
	def del_inode( self, inode_id, cursor=None ):
		if cursor is None:
			cursor = self.get_cursor()
		cursor.cursor.execute("""delete from inode where id=%s""", (inode_id, ))
	
	def add_inode( self, parent_inode_id, filename, inode, cursor=None ):
		self_commit = False
		if cursor is None:
			cursor = self.get_cursor()
			self_commit = True
		cursor.cursor.execute("""insert into inode (parent,name,type,perms,uid,gid,attrs,ctime,mtime,atime,links,size) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (parent_inode_id, filename, inode.type, "755", 0, 0, inode.attrs, inode.ctime, inode.mtime, inode.atime, 0, inode.size))
		#print "Create: ", "insert into inode (parent,name,type,perms,uid,gid,attrs,ctime,mtime,atime,links) values (%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" % (parent_inode_id, create_filename, attributes["type"], "wrxwrxwrx", 0, 0, attributes["attrs"], attributes["ctime"], attributes["mtime"], attributes["atime"], 0)
		if self_commit:
			cursor.commit()

	def add_inode_chunk( self, inode_id, chunkuuid, cursor=None ):
		self_commit = False
		if cursor is None:
			cursor = self.get_cursor()
			self_commit = True
		cursor.cursor.execute("""insert into inode_chunk values (%s,%s)""", (inode_id, chunkuuid))
		if self_commit:
			cursor.commit()

	def set_inode_size( self, inode, cursor=None ):
		self_commit = False
		if cursor is None:
			cursor = self.get_cursor()
			self_commit = True
		cursor.cursor.execute("""update inode set size=%s where id=%s""", (inode.size, inode.id))
		if self_commit:
			cursor.commit()
	
	def set_server( self, chunkserver, cursor ):
		#print "update server set last_seen=FROM_UNIXTIME(%s), available=%s, size_total=%s, size_available=%s where uuid=%s" % (chunkserver.last_seen, chunkserver.available, chunkserver.size_total, chunkserver.size_available, chunkserver.uuid)
		cursor.cursor.execute("""update server set last_seen=FROM_UNIXTIME(%s), address=%s, available=%s, size_total=%s, size_available=%s where uuid=%s""", (chunkserver.last_seen, chunkserver.address, chunkserver.available, chunkserver.size_total, chunkserver.size_available, chunkserver.uuid))
	
	def get_missing_replicate_chunks_and_servers( self ):
		c = self.get_read_cursor()
		c.execute('select chunk_uuid, UNIX_TIMESTAMP(alloc_time), count(*) as c from chunk_server left join chunk on (chunk.uuid=chunk_server.chunk_uuid) left join server on (server.uuid=chunk_server.server_uuid) where available=1 group by chunk_uuid, alloc_time having c<=1')
		rows = c.fetchall()
		return rows