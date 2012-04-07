"""
agilefuse: a filesystem abstraction of Agile Cloud Storage
This module provides an abstract base class "AgileFUSE"
"""

__version__ = "0.0.4"
__author__ = "Wylie Swanson (wylie@pingzero.net)"


import os
import pylibmc
#import memcache
import hashlib
from AgileCLU import AgileCLU
import time
import datetime
from stat import S_IFDIR, S_IFREG
from fuse import FUSE, Operations
import urllib2
import json
import pycurl

write_buf = ''
def	write_stream(buf):
	global write_buf
	write_buf += buf

class 	AgileFUSE(Operations):

	def	__init__(self):
		self.agile = AgileCLU('agile')
		self.mc = pylibmc.Client(['127.0.0.1:11211'], binary=True, behaviors={"tcp_nodelay": True, "ketama": True})
		self.pool = pylibmc.ClientPool(self.mc, 10)

		#self.mc = memcache.Client(['127.0.0.1:11211'], debug=0)
		self.root = '/'
		self.path = '/'
	
	def	__del__(self):
		self.agile.logout()
	
	def	__call__(self, op, path, *args):
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
	
	def	fixpath(self, path):
		path = os.path.normpath(path).replace('//','/')
		if path=='': path=u'/'
		return path

	def	getcache(self, path):
		with self.pool.reserve() as mc:
			return mc.get( hashlib.sha256(path).hexdigest() )

	def	setcache(self, path, val):
		with self.pool.reserve() as mc:
			mc.set( hashlib.sha256(path).hexdigest(), val )
		return True

	def	delcache(self, path):
		with self.pool.reserve() as mc:
			return mc.delete( hashlib.sha256(path).hexdigest() )

	def	create(self, path, mode):
		'''f = self.sftp.open(path, 'w')
		f.chmod(mode)
		f.close()'''
		return 0

	def	getattr(self, path, fh=None):
		path=self.fixpath(path)
		object=path
		cache = self.getcache(path)
		if cache is None:
			cache = self.getcache(os.path.split(path)[0])

		if cache is not None:
			found=None
			if cache['path']==path:
				found=cache ; omode = 0755 | S_IFDIR ; found['name']=path ; olink=1
			if not found:
				for o in cache['files']:
					if o['name']==os.path.split(path)[1]:
						found=o ; omode = 0755 | S_IFREG ; olink = 1
			if not found:
				for o in cache['directories']:
					if o['name']==os.path.split(path)[1]:
						found=o ; omode = 0755 | S_IFDIR ; olink = 2
			if found:
				ret = dict( st_gid=os.getgid(), st_uid=os.getuid(), st_size=found['size'], st_nlink=olink, st_mode=omode, st_atime=time.time(), st_ctime=found['ctime'], st_mtime=found['mtime'] )
				# print "getattr(%s) found %s -> %s" % (path,found['name'],repr(ret))
				return ret
			else:
				return dict()

		print "MANUAL STAT - NO CACHE?"
		astat = self.agile.stat( path )
		if 'type' in astat:
			if astat['type']==1: # directory
				if path=='/':
					return dict( st_gid=os.getgid(), st_uid=os.getuid(), st_mode=S_IFDIR | 0755, st_nlink=1, st_ctime=time.time(), st_mtime=time.time(), st_atime=time.time() )
				else:
					return dict( st_gid=os.getgid(), st_uid=os.getuid(), st_mode=S_IFDIR | 0755, st_nlink=2, st_ctime=time.time(), st_mtime=time.time(), st_atime=time.time() )
			elif astat['type']==2: # file
				return dict( st_gid=os.getgid(), st_uid=os.getuid(), st_mode=S_IFREG | 0777, st_size=astat['size'], st_nlink=1, st_ctime=time.time(), st_mtime=time.time(), st_atime=time.time() )
		else:
			return dict()

	def	mkdir(self, path, mode):
		return self.agile.makeDir(path)

	def	read(self, path, size, offset, fh):
		global write_buf
		print 'READ(%s,%d,%d)' % (path,size,offset)
		url = self.agile.mapperurl+urllib2.quote(path.replace("//","/"))
		'''
		req = urllib2.Request(url)
		req.headers['Range'] = 'bytes=%s-%s' % (offset, offset+size)
		f = urllib2.urlopen(req)
		data = f.read()
		# if offset + size > len(data): size = len(data) - offset
		f.close()
		return data # [offset:offset+size]
		'''
		USERAGENT='agilefuse %d-%d' % (offset, offset+size)
		write_buf = ''
		try:
			curl = pycurl.Curl()
			curl.setopt(pycurl.URL, url)
			curl.setopt(pycurl.RANGE, '%d-%d' % (offset, offset+size))
			curl.setopt(pycurl.USERAGENT, USERAGENT)
			curl.setopt(pycurl.WRITEFUNCTION, write_stream)
			curl.perform()
			curl.close()
		except:
			raise
		return write_buf

	def	updatecachepath( self, path='/', dirs_only=False, files_only=False, overrideCache=False ):
		djson=[] ; fjson=[] ; df=[] ; d=[] ; f=[] ; list=[]
		path = self.fixpath(path)
		if not path.startswith( '/' ): path=u'/%s' % path

		cachepath = path
		cache = self.getcache(cachepath)

		if cache is None:
			st = self.agile.stat( path )
			if st['code'] == -1: return False # this is not a directory or file object
			if st['type'] == 2: return False # this is a file object, not a directory

			djson = self.agile.listDir( path, 10000, 0, True)
			fjson = self.agile.listFile( path, 10000, 0, True )

			if not files_only:
				for f in djson['list']:
				   s = f['stat']
				   fst = {
					  'size' : s['size'],
					  'created_time' : datetime.datetime.fromtimestamp(s['mtime']),
					  'accessed_time' : datetime.datetime.fromtimestamp(time.time()),
					  'modified_time' : datetime.datetime.fromtimestamp(s['mtime']),
					  'st_mode' : 0755 | S_IFDIR
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
					  'st_mode' : 0755 | S_IFREG
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

			self.setcache(cachepath,json.loads(jsonstr))

			return self.getcache(cachepath)
		return self.getcache(cachepath)

	def	readdir(self, path, fh):
		path=self.fixpath(path)
		self.path=path
		cache = self.updatecachepath( path )

		# print "%s %s" % (repr(path), repr(fh))
		# print "%s" % (repr(cache))
		listing = ['.', '..']
		for o in cache['directories']: 
			object = o['name']
			listing.append( object.encode('utf-8') )
		for o in cache['files']: 
			object = o['name']
			listing.append( object.encode('utf-8') )

		#d=[o['name'] for o in cache['directories']] ; listing.extend(d)
		#f=[o['name'] for o in cache['files']] ; listing.extend(f)
		# print repr(listing)
		return listing 

	def	rename(self, old, new):
		return self.agile.rename(old, self.root + new)

	def	rmdir(self, path):
		return self.agile.rmdir(path)

	def	unlink(self, path):
		result = self.agile.deleteFile(path)
		self.delcache(os.path.split(path)[0])
		self.updatecachepath(os.path.split(path)[0])
		return result

	def	write(self, path, data, offset, fh):
		f = self.agile.post(path, 'r+')
		f.seek(offset, 0)
		f.write(data)
		f.close()
		return len(data)
	
	# Disable unused operations:
	access = None
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
