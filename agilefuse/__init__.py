"""

	agilefuse: a filesystem abstraction of Agile Cloud Storage

This module provides an abstract base class "AgileFUSE"

"""

__version__ = "0.0.2"
__author__ = "Wylie Swanson (wylie@pingzero.net)"


import os, sys
from AgileCLU import AgileCLU
import datetime
import time
from stat import S_IFDIR, S_IFREG
from fuse import FUSE, Operations
import urllib2
import json
import logging

class AgileFUSE(Operations):

	def __init__(self):
		self.log = logging.getLogger(self.__class__.__name__)
		self.api = AgileCLU('agile')
		self.root = '/'
		self.path = '/'
		self.cache_paths = {}
	
	def __del__(self):
		self.api.logout()
	
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

	def getattr(self, path, fh=None):
		astat = self.api.stat( path )
		if 'type' in astat:
			if astat['type']==1: # directory
				if path=='/': return dict( st_gid=20, st_uid=501, st_mode=S_IFDIR | 0755, st_nlink=1, st_ctime=time(), st_mtime=time(), st_atime=time() )
				else: return dict( st_gid=20, st_uid=501, st_mode=S_IFDIR | 0755, st_nlink=2, st_ctime=time(), st_mtime=time(), st_atime=time() )
			elif astat['type']==2: # file
				return dict( st_gid=20, st_uid=501, st_mode=S_IFREG | 0777, st_size=astat['size'], st_nlink=1, st_ctime=time(), st_mtime=time(), st_atime=time() )
		else:
			return dict()

	def mkdir(self, path, mode):
		return self.api.makeDir(path)

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

	def update_cachepaths( self, path='./', dirs_only=False, files_only=False, overrideCache=False ):
		djson=[] ; fjson=[] ; df=[] ; d=[] ; f=[] ; list=[]
		if not path.startswith( '/' ): path=u'/%s' % path

		cachepath = path
		cache = self.cache_paths.get( cachepath )
		caller = sys._getframe(1).f_code.co_name

		print "[%s] update_cachepaths(%s)" % (caller, cachepath)

		if cache is None:
			st = self.api.stat( path )
			if st['code'] == -1: return False # this is not a directory or file object
			if st['type'] == 2: return False # this is a file object, not a directory

			djson = self.api.listDir( path, 10000, 0, True)
			fjson = self.api.listFile( path, 10000, 0, True )

			if not files_only:
				for f in djson['list']:
				   s = f['stat']
				   fst = {
					  'size' : s['size'],
					  'created_time' : datetime.datetime.fromtimestamp(s['mtime']),
					  'accessed_time' : datetime.datetime.fromtimestamp(time.time()),
					  'modified_time' : datetime.datetime.fromtimestamp(s['mtime']),
					  'st_mode' : 0700 | S_IFDIR
				   }
				   list.append((f['name'], fst))

			if not dirs_only: 
				for f in fjson['list']:
				   s = f['stat']
				   fst = {
					  'size' : s['size'],
					  'created_time' : datetime.datetime.fromtimestamp(s['mtime']),
					  'accessed_time' : datetime.datetime.fromtimestamp(time.time()),
					  'modified_time' : datetime.datetime.fromtimestamp(s['mtime']),
					  'st_mode' : 0700 | S_IFREG
				   }
				   list.append((f['name'], fst))

			jsonstr = '''{ "path": "'''+path+'''", "size": '''+str(st.get('size', 0))+''', "ctime": '''+str(st['ctime'])+''', "mtime": '''+str(st['mtime'])+''', '''
			jsonstr = jsonstr + '''"files": [ '''
			for object in fjson['list']: 
				jsonstr = jsonstr + '''{ "name": "'''+object['name']+'''", "ctime": '''+str(object['stat']['ctime'])+''', "mtime": '''+str(object['stat']['mtime'])+''', "size": '''+str(object['stat']['size'])+''' },'''
			jsonstr = jsonstr[0:len(jsonstr)-1]
			jsonstr = jsonstr + ''' ], "directories": [ '''
			for object in djson['list']:
				jsonstr = jsonstr + '''{ "name": "'''+object['name']+'''", "ctime": '''+str(object['stat']['ctime'])+''', "mtime": '''+str(object['stat']['mtime'])+''', "size": 0 },'''
			jsonstr = jsonstr[0:len(jsonstr)-1]
			jsonstr = jsonstr + ''' ] }'''

			self.cache_paths[cachepath] = json.loads(jsonstr)
			cache = self.cache_paths.get( cachepath )
			print "[%s] WRITE CACHE %s -> %s\n" % (caller, cachepath, repr(cache))
		return True

	def readdir(self, path, fh):
		self.path=path
		cache = self.cache_paths.get( path )
		if cache is None:
			if not update_cachepaths( path ):
				return []
		
		listing = [u'.', u'..']
		d=[o['name'] for o in cache['directories']] ; listing.extend(d)
		f=[o['name'] for o in cache['files']] ; listing.extend(f)

		# for object in cache['objects']: listing.append(object['name'].encode('utf-8'))
		return listing 

	def rename(self, old, new):
		return self.api.rename(old, self.root + new)

	def rmdir(self, path):
		return self.api.rmdir(path)

	def unlink(self, path):
		return self.api.unlink(path)

	def write(self, path, data, offset, fh):
		f = self.api.post(path, 'r+')
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
