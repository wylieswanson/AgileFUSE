"""
agilefuse: a filesystem abstraction of Agile Cloud Storage
This module provides an abstract base class "AgileFUSE"
"""

__version__ = "0.0.5"
__author__ = "Wylie Swanson (wylie@pingzero.net)"

from AgileCLU import AgileCLU
import os, time, datetime, pylibmc, hashlib, json, urllib2, pycurl, socket
from stat import S_IFDIR, S_IFREG
from fuse import FUSE, Operations


class	AgileReader(object):

	def	__init__(self, sockpath=None, autostart=True):
		try:
			self.sockpath = sockpath
			if autostart:
				self.setup()
				self.connect()
		except:
			raise

	def	setup(self):
		try:
			self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
		except:
			raise

	def	connect(self):
		try:
			self.socket.connect(self.sockpath)
		except:
			raise

	def	create_payload(self, url=None, offset=None, size=None):
		try:
			payload_dict = {}
			payload_dict['url'] = url
			payload_dict['offset'] = offset
			payload_dict['size'] = size
			payload_bin = json.dumps(payload_dict)
			return payload_bin
		except:
			raise

	def	send_request(self, payload):
		try:
			self.socket.send(payload)
		except:
			raise


write_buf = ''
def	write_stream(buf):
	global write_buf
	write_buf += buf

class 	AgileFUSE(Operations):

	def	__init__(self, readlib='urllib', verbosity=0):
		self.verbosity = verbosity
		self.readlib = readlib
		self.agile = AgileCLU('agile')
		self.mc = pylibmc.Client(['127.0.0.1:11211'], binary=True, behaviors={"tcp_nodelay": True, "ketama": True})
		self.pool = pylibmc.ClientPool(self.mc, 10)
		self.cache = {}
		self.key = None

		self.root = '/'
		self.path = '/'
	
	def	__del__(self):
		try:
			self.agile.logout()
		except AttributeError:
			pass

	
	def	__call__(self, op, path, *args):
		if self.verbosity: print '->', op, path, args[0] if args else ''
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
			if self.verbosity: print '<-', op
	
	def	fixpath(self, path):
		path = os.path.normpath(path).replace('//','/')
		if path=='': path=u'/'
		return path

	def	getcache(self, path):
		path = self.fixpath(path)
		key = hashlib.sha256(path).hexdigest()
		if self.key==key: return self.cache
		with self.pool.reserve() as mc:
			if self.verbosity: print "- getcache %s (%s)" % (path, key)
			self.key = key
			self.cache = mc.get( key )
			return self.cache

	def	setcache(self, path, val):
		path = self.fixpath(path)
		key = hashlib.sha256(path).hexdigest()
		self.key = key
		self.cache = val
		with self.pool.reserve() as mc:
			if self.verbosity: print "- setcache %s (%s) = %s" % (path, key, val)
			mc.set( key, val, 300, 256 )
		return True

	def	delcache(self, path):
		path = self.fixpath(path)
		key = hashlib.sha256(path).hexdigest()
		if self.verbosity: print "- delcache %s (%s)" % (path, key)
		with self.pool.reserve() as mc:
			return mc.delete( key )

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

		if self.verbosity: print "MANUAL STAT - NO CACHE (%s)?" % (path)
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

	def	oldread(self, path, size, offset, fh):
		global write_buf
		url = self.agile.mapperurl+urllib2.quote(path.replace("//","/"))
		if self.verbosity: print 'READ(%s,%d,%d)' % (path.replace("//","/"),size,offset)

		USERAGENT='AgileFUSE %s (%d-%d)' % (__version__, offset, offset+size)
		if self.readlib=='urllib':
			# proxy = urllib2.ProxyHandler({'http': 'http://localhost:6081'})
			# opener = urllib2.build_opener(proxy)
			# urllib2.install_opener(opener)
			req = urllib2.Request(url)
			req.headers['Range'] = 'bytes=%s-%s' % (offset, offset+size)
			req.headers['User-Agent'] = USERAGENT
			f = urllib2.urlopen(req)
			data = f.read()
			f.close()
			return data # [offset:offset+size]
		elif self.readlib=='curl':
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
		else:
			if verbosity: print "%s: invalid read library" % (self.readlib)
			return False


	def	read(self, path, size, offset, fh):
		MAX_RECV=4096

		url = self.agile.mapperurl+urllib2.quote(path.replace("//","/"))
		if self.verbosity: print 'READ(%s,%d,%d)' % (path.replace("//","/"),size,offset)

		data = ''
		reader = AgileReader(sockpath='/tmp/lama-readerd.sock')
		if self.verbosity: print repr(reader)
		payload = reader.create_payload(url=url, offset=offset, size=size)
		if self.verbosity: print "created payload, len: %d" %(len(payload))
		if len(payload):
			while True:
				data += reader.socket.recv(MAX_RECV)
				if not data: break
				if self.verbosity: print "recvd %d len packet" % (len(data))
			reader.socket.close()
		else:
			if self.verbosity: print "ERROR: bad payload"
		if self.verbosity: print "returning data, len: %d" % (len(data))
		return data


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

		return listing 

	def	rename(self, old, new):
		if self.verbosity: print "rename %s %s" % (old, new)
		return self.agile.rename(old, self.root + new)

	def	rmdir(self, path):
		result = self.agile.deleteDir(path)
		self.delcache(path)
		self.delcache(os.path.split(path)[0])
		self.updatecachepath(os.path.split(path)[0])
		return result

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
