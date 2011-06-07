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

import math,uuid,os,time,operator,sys,argparse,zlib
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
import xmlrpclib

uuid_filename = "chunkserver.uuid"

class EAFSChunkserver:
    def __init__(self, master_host, host, port, rootfs):
        # Create root fs
        if not os.access(rootfs, os.W_OK):
            os.makedirs(rootfs)
	self.uuid_filename_full = os.path.join( rootfs, str(port)+"-"+uuid_filename )
	uuid = ""
	try:	
		f = open(self.uuid_filename_full, 'r+')
		lines = f.readlines()
		if lines is not None and len(lines)>0:
			uuid = str(lines[0])
	except:
		print "No uuid file. Will create it."
	
        self.address = "http://%s:%d" % (host, port)
        self.master = xmlrpclib.ServerProxy(master_host)
        self.uuid = self.master.connect_chunkserver( self.address, uuid )
        if self.uuid is None:
            return False
        if uuid is None or uuid=="":
            print "Create UUID file %s" % self.uuid_filename_full,
            f = open(self.uuid_filename_full, 'w+')
            f.write(self.uuid)
            f.close()
        self.chunktable = {}
        self.local_filesystem_root = os.path.join( rootfs, "chunks", str(self.uuid) ) #repr
        #print "FS ROOT: %s" % self.local_filesystem_root
        if not os.access(self.local_filesystem_root, os.W_OK):
            os.makedirs(self.local_filesystem_root)

    def write(self, chunkuuid, chunk):
        local_filename = self.chunk_filename(chunkuuid)
        with open(local_filename, "w") as f:
            f.write(zlib.decompress(chunk.data))
        self.chunktable[chunkuuid] = local_filename

    def read(self, chunkuuid):
        data = None
        local_filename = self.chunk_filename(chunkuuid)
        with open(local_filename, "r") as f:
            data = f.read()
        return xmlrpclib.Binary(data)

    def chunk_filename(self, chunkuuid):
        return os.path.join( self.local_filesystem_root, str(chunkuuid) ) + '.gfs'


class RequestHandler(SimpleXMLRPCRequestHandler):
        rpc_paths = ('/RPC2',)


def main():
	parser = argparse.ArgumentParser(description='EAFS Chunk Server')
	parser.add_argument('--host', dest='host', default='localhost', help='Bind to address')
	parser.add_argument('--port', dest='port', default=6800, type=int, help='Bind to port')
	parser.add_argument('--master', dest='master', default='localhost:6799', help='Master server address')
	parser.add_argument('--rootfs', dest='rootfs', default='/tmp/eafs/', help='Save chunk to')
	args = parser.parse_args()
	
        master_host = "http://" + args.master
	
	# Create server
	server = SimpleXMLRPCServer((args.host, args.port), requestHandler=RequestHandler, allow_none=True, logRequests=False)
	server.register_introspection_functions()
	server.register_instance(EAFSChunkserver(master_host, args.host, args.port, args.rootfs))
	server.serve_forever()

if __name__ == "__main__":
    main()

