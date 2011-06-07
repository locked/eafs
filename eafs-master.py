import math,uuid,os,time,operator

from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
import xmlrpclib

import sqlite3


fs_base = "/tmp"
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
    def __init__(self):
        self.db = sqlite3.connect(os.path.join(fs_base,db_filename))
        self.max_chunkservers = 10
        self.max_chunksperfile = 100
        self.chunksize = 10
        self.filetable = {} # file to chunk mapping
        self.chunktable = {} # chunkuuid to chunkloc mapping
        self.chunkservers = {} # loc id to chunkserver mapping
        self.load_chunkservers()
        self.load_filetable()
        self.load_chunktable()

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

    def get_chunksize(self):
        return self.chunksize

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

    def alloc(self, filename, num_chunks, attributes): # return ordered chunkuuid list
        chunkuuids = self.alloc_chunks(num_chunks)
        self.save_filechunktable(filename, chunkuuids, attributes)
        return chunkuuids
    
    def choose_chunkserver(self):
        for i in self.chunkservers:
            return self.chunkservers[i]
    
    def choose_chunkserver_uuids(self):
        uuids = []
        for i in self.chunkservers:
            uuids.append( self.chunkservers[i].uuid )
        return uuids
    
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

    def alloc_append(self, filename, num_append_chunks): # append chunks
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
    
    def delete(self, filename): # rename for later garbage collection
        chunkuuids = self.get_chunkuuids(filename)
        attributes = self.filetable[filename]
        del self.filetable[filename]
        c = self.db.cursor()
        c.execute("""delete from file_chunk where file_name=?""", (filename, ))
        c.execute("""delete from file where name=?""", (filename, ))
        self.db.commit()
        c.close()
        timestamp = repr(time.time())
        deleted_filename = "/hidden/deleted/" + timestamp + filename 
        self.save_filechunktable(deleted_filename,chunkuuids,attributes)
        print "deleted file: " + filename + " renamed to " + \
             deleted_filename + " ready for gc"
    
    
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
        metadata += "Chunkserver Data:\n"
        for chunkuuid, chunklocs in sorted(self.chunktable.iteritems(), key=operator.itemgetter(1)):
            for chunkloc in chunklocs:
                chunk = self.chunkservers[chunkloc].rpc.read(chunkuuid)
                metadata += "%s, %s, %s\n" % (chunkloc, chunkuuid, chunk)
        return metadata
    """
    def handle(self):
        # self.request is the TCP socket connected to the client
        self.data = self.request.recv(1024).strip()
        print "%s wrote:" % self.client_address[0]
        print self.data
        # just send back the same data, but upper-cased
        self.request.send(self.data.upper())
    """


# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
	rpc_paths = ('/RPC2',)


def main():
	HOST, PORT = "localhost", 6799
	# Create server
	server = SimpleXMLRPCServer((HOST, PORT), requestHandler=RequestHandler, allow_none=True)
	server.register_introspection_functions()
	#server.register_function(adder_function, 'add')
	server.register_instance(EAFSMaster())
	server.serve_forever()


if __name__ == "__main__":
	main()

