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

import math,uuid,os,time,operator,random,xmlrpclib,argparse,zlib,base64


# For FUSE
from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

from eafsclientlib import EAFSClientLib


class EAFSClientFuse(EAFSClientLib, Operations):
#class EAFSClientFuse(EAFSClientLib, LoggingMixIn, Operations):
	def rename(self, old, new):
		return 0 if self.master.rename(old, new) else 0
	
	def write(self, path, data, offset, fh):
		#print "FUSE Write path:%s fh:%d" % (path, fh)
		if offset>0:
			return self.eafs_write_append(path, data, offset, fh)
		return self.eafs_write(path, data, fh)
	
	def fsync(self, path, datasync, fh):
		#print "FUSE Sync path:%s fh:%d" % (path, fh)
		if self.eafs_flush( path, fh ):
			return 0
		return 1
	
	def flush(self, path, fh):
		#print "FUSE Flush path:%s fh:%d" % (path, fh)
		if self.eafs_flush( path, fh ):
			return 0
		return 1
	
	def create(self, path, mode):
		print "FUSE Create path:%s" % (path)
		attributes = {"type":"f", "atime":int(time.time()), "ctime":int(time.time()), "mtime":int(time.time()), "size":0, "links":1, "attrs":""}
		chunkuuids = self.master.alloc(path, 0, attributes, '')
		self.fd += 1
		return self.fd
	
	def truncate(self, path, length, fh=None):
		print "FUSE truncate %s" % path
		return 0
		s = ""
		for i in range(0,length):
			s += "0"
		#self.accumulate(path, s, fh)
		return length
	
	def mkdir(self, path, mode):
		filename = path
		attributes = {"type":"d", "atime":int(time.time()), "ctime":int(time.time()), "mtime":int(time.time()), "attrs":""}
		chunkuuids = self.master.alloc(filename, 0, attributes, '')
	
	def rmdir(self, path):
		self.master.delete(path)
	
	def unlink(self, path):
		print "FUSE unlink path:%s" % (path)
		self.master.delete(path)
	
	def readdir(self, path, fh):
		fl = self.master.list_files(path)
		files = {}
		now = time.time()
		for f in fl:
			filename = base64.b64decode( f['name'] ) #.decode("utf-8")
			if f['type']=="d":
				files[filename] = dict(st_mode=(S_IFDIR | 0755), st_size=f['size'], st_ctime=now, st_mtime=f['mtime'], st_atime=now, st_nlink=2)
			else:
				files[filename] = dict(st_mode=(S_IFREG | 0755), st_size=f['size'], st_ctime=now, st_mtime=f['mtime'], st_atime=now, st_nlink=1)
		#print "readdir: ", files
		return ['.', '..'] + [x[0:] for x in files if x != '/']

	def read(self, path, size, offset, fh):
		return self.eafs_read( path, size, offset )
	
	def statfs(self, path):
		print "FUSE statfs %s" % path
		return self.master.statfs( path )
		##return dict(f_bsize=512, f_blocks=32768000, f_bavail=16384000)
		#return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)
	
	def open(self, path, flags):
		print "FUSE open %s" % path
		self.fd += 1
		return self.fd
	
	def getattr(self, path, fh=None):
		print "FUSE getattr path:%s" % (path)
		f = self.master.file_attr(path)
		if f is None:
			raise FuseOSError(ENOENT)
		now = time.time()
		if f['type']=="d":
			st = dict(st_mode=(S_IFDIR | 0755), st_size=4096, st_ctime=f['ctime'], st_mtime=f['mtime'], st_atime=f['atime'], st_nlink=0)
		elif f['type']=="f":
			st = dict(st_mode=(S_IFREG | 0755), st_size=f['size'], st_ctime=f['ctime'], st_mtime=f['mtime'], st_atime=f['atime'], st_nlink=0)
		else:
			raise FuseOSError(ENOENT)
		return st
	
	def utimens(self, path, times=None):
		now = time.time()
		print "FUSE utimens path:%s" % (path)
		"""
		atime, mtime = times if times else (now, now)
		self.files[path]['st_atime'] = atime
		self.files[path]['st_mtime'] = mtime
		"""
	
	def symlink(self, target, source):
		print "FUSE symlink path:%s" % (path)
		"""
		self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
			st_size=len(source))
		self.data[target] = source
		"""
	
	def setxattr(self, path, name, value, options, position=0):
		pass
		"""
		attrs = self.files[path].setdefault('attrs', {})
		attrs[name] = value
		"""
	
	def removexattr(self, path, name):
		pass
	
	def readlink(self, path):
		print "FUSE readlink path:%s" % (path)
		return self.data[path]
	
	def listxattr(self, path):
		return []
	
	def getxattr(self, path, name, position=0):
		return ''
	
	def chmod(self, path, mode):
		print "chmod %s" % path
		"""
		self.files[path]['st_mode'] &= 0770000
		self.files[path]['st_mode'] |= mode
		"""
		return 0
	
	def chown(self, path, uid, gid):
		print "chown %s" % path
		pass
		"""
		self.files[path]['st_uid'] = uid
		self.files[path]['st_gid'] = gid
		"""



def main():
	parser = argparse.ArgumentParser(description='EAFS Fuse Client')
	parser.add_argument('--mount', dest='mount_point', default='/mnt', help='Mount point')
	parser.add_argument('--master', dest='master', default='localhost:6799', help='Master server address')
	parser.add_argument('--debug', dest='debug', default=0, help='Activate debug messages')
	args = parser.parse_args()
	master = 'http://' + args.master
	fuse = FUSE(EAFSClientFuse(master, args.debug), args.mount_point, foreground=True, allow_other=True)

if __name__ == "__main__":
	main()

