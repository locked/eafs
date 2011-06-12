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

from eafsclientlib import EAFSClientLib


#LoggingMixIn
class EAFSClientFuse(EAFSClientLib, LoggingMixIn, Operations):
	def exists(self, filename):
		return self.master.exists(filename)
	
	def delete(self, filename):
		self.master.delete(filename)
	
	def rename(self, old, new):
		self.master.rename(old, new)
	
	def write(self, path, data, offset, fh):
		if offset>0:
			return self.eafs_write_append(path, data)
		attributes = {"type":"f", "atime":"", "ctime":"", "mtime":"", "attrs":""}
		return self.eafs_write(path, data, attributes)
	
	def create(self, path, mode):
		attributes = {"type":"f", "atime":"", "ctime":"", "mtime":"", "attrs":""}
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
		attributes = {"type":"d", "atime":"", "ctime":"", "mtime":"", "attrs":""}
		chunkuuids = self.master.alloc(filename, 0, attributes)
	
	def rmdir(self, path):
		self.delete(path)
	
	def unlink(self, path):
		self.master.delete(path)
	
	def readdir(self, path, fh):
		fl = self.master.list_files(path)
		files = {}
		now = time.time()
		for f in fl:
			if f['type']=="d":
				files[f['name']] = dict(st_mode=(S_IFDIR | 0755), st_size=f['size'], st_ctime=now, st_mtime=f['mtime'], st_atime=now, st_nlink=0)
			else:
				files[f['name']] = dict(st_mode=(S_IFREG | 0755), st_size=f['size'], st_ctime=now, st_mtime=f['mtime'], st_atime=now, st_nlink=0)
		return ['.', '..'] + [x[0:] for x in files if x != '/']

	def read(self, path, size, offset, fh):
		return self.eafs_read( path, size, offset )
	
	def statfs(self, path):
		return dict(f_bsize=512, f_blocks=32768, f_bavail=16384)
		#return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)
	
	def open(self, path, flags):
		self.fd += 1
		return self.fd
	
	def getattr(self, path, fh=None):
		#print "CHECK PATH %s" % path
		f = self.master.file_attr(path)
		if f is None:
			raise FuseOSError(ENOENT)
		now = time.time()
		if f['type']=="d":
			st = dict(st_mode=(S_IFDIR | 0755), st_ctime=now, st_mtime=now, st_atime=now, st_nlink=2)
		elif f['type']=="f":
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

