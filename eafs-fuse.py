import math,uuid,os,time,operator,random,xmlrpclib


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

class EAFSClientFuse(LoggingMixIn, Operations):
    def __init__(self, master_host):
        self.master = xmlrpclib.ServerProxy(master_host)
        self.chunkservers = {}
        self.fd = 0
        print self.master.dump_metadata()
    
    def update_chunkservers(self):
        chunkservers = self.master.get_chunkservers()
        for chunkserver in chunkservers:
            if chunkserver['uuid'] not in self.chunkservers:
                self.chunkservers[chunkserver['uuid']] = EAFSChunkserver( chunkserver['uuid'], chunkserver['address'] )
        
    def write_chunks(self, chunkuuids, data):
        chunks = [ data[x:x+self.master.get_chunksize()] \
            for x in range(0, len(data), self.master.get_chunksize()) ]
        self.update_chunkservers()
        print "CHUNKSERVERS: ", self.chunkservers
        for i in range(0, len(chunkuuids)): # write to each chunkserver
            chunkuuid = chunkuuids[i]
            chunklocs = self.master.get_chunklocs(chunkuuid)
            for chunkloc in chunklocs:
                print "chunkloc: ", chunkloc
                self.chunkservers[chunkloc].rpc.write(chunkuuid, chunks[i])
  
    def num_chunks(self, size):
        return (size // self.master.get_chunksize()) \
            + (1 if size % self.master.get_chunksize() > 0 else 0)

    def write_append(self, filename, data):
        if not self.exists(filename):
            raise Exception("append error, file does not exist: " \
                 + filename)
        num_append_chunks = self.num_chunks(len(data))
        append_chunkuuids = self.master.alloc_append(filename, \
            num_append_chunks)
        self.write_chunks(append_chunkuuids, data) 

    def exists(self, filename):
        return self.master.exists(filename)
    
    def delete(self, filename):
        self.master.delete(filename)

    def write(self, path, data, offset, fh):
        filename = path
        if self.exists(filename): # if already exists, overwrite
            self.delete(filename)
        num_chunks = self.num_chunks(len(data))
        attributes = {"mode":"file", "atime":"", "ctime":"", "mtime":"", "attrs":""}
        chunkuuids = self.master.alloc(filename, num_chunks, attributes)
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
    master = 'http://localhost:6799'
    fuse = FUSE(EAFSClientFuse(master), argv[1], foreground=True)

if __name__ == "__main__":
    main()

