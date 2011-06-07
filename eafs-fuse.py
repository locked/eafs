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

import math,uuid,os,time,operator,random,xmlrpclib,argparse,zlib


# For FUSE
from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


class EAFSChunkserver:
        def __init__(self, uuid, address):
                self.uuid = uuid
                self.address = address
                self.rpc = xmlrpclib.ServerProxy(address)

#LoggingMixIn
class EAFSClientFuse(Operations):
    def __init__(self, master_host):
        self.master = xmlrpclib.ServerProxy(master_host)
        self.chunk_size = self.master.get_chunksize()
        self.chunkservers = {}
        self.fd = 0
        print self.master.dump_metadata()
    
    def update_chunkservers(self):
        chunkservers = self.master.get_chunkservers()
        for chunkserver in chunkservers:
            if chunkserver['uuid'] not in self.chunkservers:
                print "ADD CHUNKSERVER: ", chunkserver['uuid'], chunkserver['address']
                self.chunkservers[chunkserver['uuid']] = EAFSChunkserver( chunkserver['uuid'], chunkserver['address'] )
        
    def write_chunks(self, chunkuuids, data):
        chunks = [ data[x:x+self.chunk_size] \
            for x in range(0, len(data), self.chunk_size) ]
        self.update_chunkservers()
        print "CHUNKSERVERS: ", len(self.chunkservers)
        for i in range(0, len(chunkuuids)): # write to each chunkserver
            chunkuuid = chunkuuids[i]
            chunklocs = self.master.get_chunklocs(chunkuuid)
            chunkserver_writes = 0
            for chunkloc in chunklocs:
                #print "chunkloc: ", chunkloc
                try:
                    self.chunkservers[chunkloc].rpc.write(chunkuuid, xmlrpclib.Binary(zlib.compress(chunks[i])))
                    chunkserver_writes += 1
                except:
                    print "Chunkserver %s failed" % chunkloc
            if chunkserver_writes==0:
                raise Exception("write_chunks error, not enough chunkserver available")
  
    def num_chunks(self, size):
        return (size // self.chunk_size) \
            + (1 if size % self.chunk_size > 0 else 0)

    def write_append(self, filename, data):
        if not self.exists(filename):
            raise Exception("append error, file does not exist: " + filename)
        num_append_chunks = self.num_chunks(len(data))
        append_chunkuuids = self.master.alloc_append(filename, num_append_chunks)
        self.write_chunks(append_chunkuuids, data) 
        return len(data)

    def exists(self, filename):
        return self.master.exists(filename)
    
    def delete(self, filename):
        self.master.delete(filename)

    def rename(self, old, new):
        self.master.rename(old, new)

    def write(self, path, data, offset, fh):
        if offset>0:
            return self.write_append(path, data)
        if self.exists(path):
            self.delete(path)
        num_chunks = self.num_chunks(len(data))
        attributes = {"mode":"file", "atime":"", "ctime":"", "mtime":"", "attrs":""}
        chunkuuids = self.master.alloc(path, num_chunks, attributes)
        self.write_chunks(chunkuuids, data)
        return len(data)

    def create(self, path, mode):
        attributes = {"mode":"file", "atime":"", "ctime":"", "mtime":"", "attrs":""}
        chunkuuids = self.master.alloc(path, 0, attributes)
        #self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
        #    st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())
        self.fd += 1
        return self.fd

    def truncate(self, path, length, fh=None):
        s = ""
        for i in range(0,length):
            s += "0"
        self.write(path, s, length, fh)

    def mkdir(self, path, mode):
        filename = path
        attributes = {"mode":"dir", "atime":"", "ctime":"", "mtime":"", "attrs":""}
        chunkuuids = self.master.alloc(filename, 0, attributes)
    
    def rmdir(self, path):
        self.delete(path)
    """
    def write(self, path, data, offset, fh):
        self.write_append(self, path, data)
        return len(data)
    """

    def read(self, path, size, offset, fh):
        filename = path
        if not self.exists(filename):
            raise Exception("read error, file does not exist: " \
                + filename)
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
                #print "Select chunkloc %s from %d choices" % (chunkloc, len(chunklocs))
                #try:
                if True:
                    chunk_raw = self.chunkservers[chunkloc].rpc.read(chunkuuid)
                    chunk = chunk_raw.data
                    chunk_read = True
                #except:
                #    print "Chunkserver %d failed %d remaining" % (chunkidrnd, len(chunklocs)-len(done_chunkserver))
                done_chunkserver.append(chunkidrnd)
            if not chunk_read:
                raise Exception("read error, chunkserver unavailable: " + filename)
            chunks.append(chunk)
        data = reduce(lambda x, y: x + y, chunks) # reassemble in order
        return data[offset:offset+size]

    def unlink(self, path):
        self.master.delete(path)

    def readdir(self, path, fh):
        fl = self.master.list_files(path)
        files = {}
        now = time.time()
        for f in fl:
            if f["mode"]=="dir":
                files[f["filename"]] = dict(st_mode=(S_IFDIR | 0755), st_size=50, st_ctime=now, st_mtime=now, st_atime=now, st_nlink=2)
            else:
                files[f["filename"]] = dict(st_mode=(S_IFREG | 0755), st_size=50, st_ctime=now, st_mtime=now, st_atime=now, st_nlink=2)
        return ['.', '..'] + [x[0:] for x in files if x != '/']

    def statfs(self, path):
        #return dict(f_bsize=2048, f_blocks=16384, f_bavail=16384)
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        #print "CHECK PATH %s" % path
        f = self.master.file_attr(path)
        if f is None:
            raise FuseOSError(ENOENT)
        now = time.time()
        if f['mode']=="dir":
            st = dict(st_mode=(S_IFDIR | 0755), st_ctime=now, st_mtime=now, st_atime=now, st_nlink=2)
        elif f['mode']=="file":
            st = dict(st_mode=(S_IFREG | 0755), st_size=f['size'], st_ctime=now, st_mtime=now, st_atime=now, st_nlink=2)
        else:
            raise FuseOSError(ENOENT)
        return st


def main():
	parser = argparse.ArgumentParser(description='EAFS Fuse Client')
	parser.add_argument('--mount', dest='mount_point', default='/mnt', help='Mount point')
	parser.add_argument('--master', dest='master', default='localhost:6799', help='Master server address')
	args = parser.parse_args()
	master = 'http://' + args.master
	fuse = FUSE(EAFSClientFuse(master), args.mount_point, foreground=True)

if __name__ == "__main__":
    main()

