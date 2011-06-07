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
            c.execute('drop table file')
            c.execute('drop table chunk')
            c.execute('drop table file_chunk')
            c.execute('drop table server')
            c.execute('drop table chunk_server')
            self.db.commit()
            c.execute('create table file (name text, mode text, attrs text, ctime text, mtime text, atime text, PRIMARY KEY(name))')
            c.execute('create table chunk (uuid text, PRIMARY KEY(uuid))')
            c.execute('create table file_chunk (file_name text, chunk_uuid text, UNIQUE(file_name,chunk_uuid))')
            c.execute('create table server (uuid text, address text, PRIMARY KEY(uuid))')
            c.execute('create table chunk_server (chunk_uuid text, server_uuid text, UNIQUE(chunk_uuid,server_uuid))')
            self.db.commit()
            c.close()
        self.max_chunkservers = 100
        self.max_chunksperfile = 10000000
        self.chunksize = 4096
        self.filetable = {} # file to chunk mapping
        self.chunktable = {} # chunkuuid to chunkloc mapping
        self.chunkservers = {} # loc id to chunkserver mapping
        self.load_chunkservers()
        self.load_filetable()
        self.load_chunktable()
    
    
    def get_chunksize(self):
        return self.chunksize
    
    
    def load_filetable(self):
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
    
    def load_chunktable(self):
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

    def get_chunkuuids(self, filename):
        return self.filetable[filename]["chunks"]

    def exists(self, filename):
        return True if filename in self.filetable else False
    
    def delete(self, filename):
        #timestamp = repr(time.time())
        #deleted_filename = "/hidden/deleted/" + timestamp + filename 
        #self.rename( filename, deleted_filename )
        c = self.db.cursor()
        chunkuuids = self.get_chunkuuids(filename)
        for chunkuuid in chunkuuids:
            c.execute("""delete from chunk where uuid=?""", (chunkuuid, ))
            c.execute("""delete from chunk_server where chunk_uuid=?""", (chunkuuid, ))
        del self.filetable[filename]
        c.execute("""delete from file_chunk where file_name=?""", (filename, ))
        c.execute("""delete from file where name=?""", (filename, ))
        self.db.commit()
        c.close()

    def rename(self, filename, new_filename):
        chunkuuids = self.get_chunkuuids(filename)
        attributes = self.filetable[filename]
        del self.filetable[filename]
        c = self.db.cursor()
        c.execute("""delete from file_chunk where file_name=?""", (filename, ))
        c.execute("""delete from file where name=?""", (filename, ))
        self.db.commit()
        c.close()
        self.save_filechunktable(new_filename,chunkuuids,attributes)
        print "file: " + filename + " renamed to " + new_filename
    
    
    def save_filechunktable(self, filename, chunkuuids, attributes):
        if filename not in self.filetable:
            self.filetable[filename] = {"mode":attributes["mode"], "attrs":attributes["attrs"], "ctime":attributes["ctime"], "mtime":attributes["mtime"], "atime":attributes["atime"],"chunks":[]}
        self.filetable[filename]["chunks"] = chunkuuids
        c = self.db.cursor()
        c.execute("""insert into file values (?,?,?,?,?,?)""", (filename, attributes["mode"], attributes["attrs"], attributes["ctime"], attributes["mtime"], attributes["atime"]))
        for chunkuuid in chunkuuids:
            c.execute("""insert into file_chunk values (?,?)""", (filename, chunkuuid))
        self.db.commit()
        c.close()
    
    
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
    
    
    def file_attr(self, path):
        if path=="/":
            return {'mode':"dir",'size':0}
        if path in self.filetable:
            attributes = self.filetable[path]
            return {'mode':attributes["mode"],'size':len(attributes["chunks"])*self.chunksize}
        return None
    
    
    def dump_metadata(self):
        metadata = "Filetable:\n"
        for filename, attributes in self.filetable.items():
            metadata += "%s with %d chunks\n" % (filename, len(attributes["chunks"]))
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
	server = SimpleXMLRPCServer((args.host, args.port), requestHandler=RequestHandler, allow_none=True)
	server.register_introspection_functions()
	server.register_instance(EAFSMaster(args.rootfs, args.init))
	server.serve_forever()


if __name__ == "__main__":
	main()

