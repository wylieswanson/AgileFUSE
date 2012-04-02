"""

	agilefuse: a filesystem abstraction of Agile Cloud Storage

This module provides an abstract base class "AgileFUSE"

"""

__version__ = "0.0.1"
__author__ = "Wylie Swanson (wylie@pingzero.net)"


import os
from AgileCLU import AgileCLU
from time import time
from stat import S_IFDIR, S_IFREG
from fuse import FUSE, Operations
import urllib2
import json

class AgileFUSE(Operations):

	def __init__(self):
		self.agile = AgileCLU('agile')
		self.root = '/'
		self.path = '/'
		self.cache_paths = {}
	
	def __del__(self):
		self.agile.logout()
	
	def __call__(self, op, path, *args):
		print '->', op, path, args[0] if args else ''
		ret = '[Unhandled Exception]'
		try:
			ret = getattr(self, op)(self.root + path, *args)
			return ret
		except OSError, e:
			ret = str(e)
			raise
		except IOError, e:
			ret = str(e)
			raise OSError(*e.args)
		finally:
			print '<-', op
	
	def create(self, path, mode):
		'''f = self.sftp.open(path, 'w')
		f.chmod(mode)
		f.close()'''
		return 0

	def getcachepath(self,path):
		try: self.cache_paths.get('p'+path.replace('/',''))
		except KeyError: self.cache_paths['p'+path.replace('/','')] = None
		print 'get cachepathkey(%s) = %s ' %  (str('p'+path.replace('/','')), str(self.cache_paths.get('p'+path.replace('/',''))))
		return self.cache_paths.get('p'+path.replace('/',''))

	def setcachepath(self,path,val):
		self.cache_paths.get('p'+path.replace('/',''))
		print 'set cachepathkey(%s) = %s ' %  (str('p'+path.replace('/','')), str(self.cache_paths.get('p'+path.replace('/',''))))
		self.cache_paths['p'+path.replace('/','')]=val

	def getattr(self, path, fh=None):
		#print 'path = ',self.path
		#print 'cache = ',str(self.getcachepath(self.path))

		cache = self.getcachepath(self.path)
		if cache is not None:
			for object in cache['objects']:
				if object['name']==os.path.split(path)[1]:
					if object['isdir']:
						return dict( st_gid=20, st_uid=501, st_mode=S_IFDIR | 0755, st_nlink=1, st_ctime=time(), st_mtime=time(), st_atime=time() )
					else:
						return dict( st_gid=20, st_uid=501, st_mode=S_IFREG | 0777, st_size=object['size'], st_nlink=1, st_ctime=object['ctime'], st_mtime=object['mtime'], st_atime=time() )

		astat = self.agile.stat( path )
		if 'type' in astat:
			if astat['type']==1: # directory
				if path=='/':
					return dict( st_gid=20, st_uid=501, st_mode=S_IFDIR | 0755, st_nlink=1, st_ctime=time(), st_mtime=time(), st_atime=time() )
				else:
					return dict( st_gid=20, st_uid=501, st_mode=S_IFDIR | 0755, st_nlink=2, st_ctime=time(), st_mtime=time(), st_atime=time() )
			elif astat['type']==2: # file
				return dict( st_gid=20, st_uid=501, st_mode=S_IFREG | 0777, st_size=astat['size'], st_nlink=1, st_ctime=time(), st_mtime=time(), st_atime=time() )
		else:
			return dict()

	def mkdir(self, path, mode):
		return self.agile.makeDir(path)

	def read(self, path, size, offset, fh):
		print 'READ(%s,%d,%d)' % (path, size,offset)
		url = EGRESS_BASE_URL+urllib2.quote(path.replace("//","/"))
		print url
		req = urllib2.Request(url)
		req.headers['Range'] = 'bytes=%s-%s' % (offset, offset+size)
		f = urllib2.urlopen(req)
		data = f.read()
		# if offset + size > len(data): size = len(data) - offset
		f.close()
		return data # [offset:offset+size]

	def cachelistFileDir( self, path ):
		print "cachelistFileDir(%s)" % path
		if self.getcachepath(path) is None:
			dl = self.agile.listDir( path )
			fl = self.agile.listFile( path ) 
			jsonstr = '''{ "path": "'''+path+'''", "objects": [ '''
			for object in fl['list']: 
				jsonstr = jsonstr + '''{ "name": "'''+object['name']+'''", "isdir": 0, "ctime": '''+str(object['stat']['ctime'])+''', "mtime": '''+str(object['stat']['mtime'])+''', "size":  '''+str(object['stat']['size'])+''' },'''
			for object in dl['list']:
				jsonstr = jsonstr + '''{ "name": "'''+object['name']+'''", "isdir": 1 },'''
			jsonstr = jsonstr[0:len(jsonstr)-1]
			jsonstr = jsonstr + ''' ] }'''
			self.setcachepath(path,json.loads(jsonstr))
		else:
			print "self.getcachepath(%s) is not None?"
		return self.getcachepath(path)

	def readdir(self, path, fh):
		self.path=path
		objects = self.cachelistFileDir( path ) 
		listing = ['.', '..']
		for object in objects['objects']: listing.append(object['name'].encode('utf-8'))
		return listing 

	def rename(self, old, new):
		return self.agile.rename(old, self.root + new)

	def rmdir(self, path):
		return self.agile.rmdir(path)

	def unlink(self, path):
		return self.agile.unlink(path)

	def write(self, path, data, offset, fh):
		f = self.agile.post(path, 'r+')
		f.seek(offset, 0)
		f.write(data)
		f.close()
		return len(data)
	
	# Disable unused operations:
	flush = None
	getxattr = None
	listxattr = None
	release = None
	releasedir = None
	readlink = None
	statfs = None
	symlink = None
	truncate = None
	chmod = None
	chown = None
	utimens = None
