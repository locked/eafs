import math,uuid,os,time,operator,sys
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
import xmlrpclib

fs_base = "/tmp/eafs/"
uuid_filename = "chunkserver.uuid"

class EAFSChunkserver:
    def __init__(self, master_host, host, port, uuid):
        self.address = "http://%s:%d" % (host, port)
        self.master = xmlrpclib.ServerProxy(master_host)
        self.uuid = self.master.connect_chunkserver( self.address, uuid )
        if self.uuid is None:
            return False
        if uuid is None or uuid=="":
            print "Create UUID file %s" % os.path.join( fs_base, str(port)+"-"+uuid_filename )
            f = open(os.path.join( fs_base, str(port)+"-"+uuid_filename ), 'w+')
            f.write(self.uuid)
            f.close()
        self.chunktable = {}
        self.local_filesystem_root = os.path.join( fs_base, "chunks", str(self.uuid) ) #repr
        #print "FS ROOT: %s" % self.local_filesystem_root
        if not os.access(self.local_filesystem_root, os.W_OK):
            os.makedirs(self.local_filesystem_root)

    def write(self, chunkuuid, chunk):
        local_filename = self.chunk_filename(chunkuuid)
        with open(local_filename, "w") as f:
            f.write(chunk)
        self.chunktable[chunkuuid] = local_filename

    def read(self, chunkuuid):
        data = None
        local_filename = self.chunk_filename(chunkuuid)
        with open(local_filename, "r") as f:
            data = f.read()
        return data

    def chunk_filename(self, chunkuuid):
        return os.path.join( self.local_filesystem_root, str(chunkuuid) ) + '.gfs'

class RequestHandler(SimpleXMLRPCRequestHandler):
        rpc_paths = ('/RPC2',)

def main():
	HOST, PORT = "localhost", int(sys.argv[1])
	
	uuid = ""
	try:	
		f = open(os.path.join( fs_base, str(PORT)+"-"+uuid_filename ), 'r+')
		lines = f.readlines()
		if lines is not None and len(lines)>0:
			uuid = str(lines[0])
	except:
		print "No uuid file. Will create it."
	
        master_host = "http://localhost:6799"
	
	# Create server
	server = SimpleXMLRPCServer((HOST, PORT), requestHandler=RequestHandler, allow_none=True)
	server.register_introspection_functions()
	server.register_instance(EAFSChunkserver(master_host, HOST, PORT, uuid))
	server.serve_forever()

if __name__ == "__main__":
    main()

