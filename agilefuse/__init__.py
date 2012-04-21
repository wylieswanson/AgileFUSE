"""
agilefuse: a filesystem abstraction of Agile Cloud Storage
This module provides an abstract base class "AgileFUSE"
"""

__version__ = "0.0.5"
__author__ = "Wylie Swanson (wylie@pingzero.net)"

from AgileCLU import AgileCLU
import os, time, datetime, pylibmc, hashlib, json, urllib2, pycurl, socket
from stat import S_IFDIR, S_IFREG
from fuse import FUSE, Operations, FuseOSError
from errno import ENOENT


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
		self.cache={} ; self.key={} ; self.cache[0] = {} ; self.key[0] = [] ; self.cache[1] = {} ; self.key[1] = []

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
		# if self.verbosity>=10: print 'fixpath(%s)' % (path)
		path = os.path.normpath(path).replace('//','/')
		if path=='': path=u'/'
		return path

	def	getcache(self, path, typ=0):
		path = self.fixpath(path)
		key = str(typ)+hashlib.sha256(path).hexdigest()
		if self.key[typ]==key: return self.cache[typ]
		with self.pool.reserve() as mc:
			self.key[typ] = key ; self.cache[typ] = mc.get( key )
			if self.verbosity>=2: print "- getcache %s (%s) = %s" % (path, self.key[typ], str(self.cache[typ]) )
			return self.cache[typ]

	def	setcache(self, path, val, typ=0):
		TIMEOUT=300
		path = self.fixpath(path) ; key = str(typ)+hashlib.sha256(path).hexdigest() ; self.key[typ] = key ; self.cache[typ] = val
		with self.pool.reserve() as mc:
			if self.verbosity>=2: print "- setcache %s (%s) = %s" % (path, self.key[typ], self.cache[typ])
			mc.set( self.key[typ], self.cache[typ], TIMEOUT, 256 )
		return True

	def	delcache(self, path, typ=0):
		path = self.fixpath(path) ; key = str(typ)+hashlib.sha256(path).hexdigest() 
		if self.verbosity>=2: print "- delcache %s (%s)" % (path, key )
		with self.pool.reserve() as mc:
			return mc.delete( key )


	def cache2lists(self,cache):
		f=[] ; d=[]
		if cache is not None:
			for o in cache['files']: f.append(o['name'])
			for o in cache['directories']: d.append(o['name'])
		return f,d
	
	def what(self,name,cache):
		if name is '': 
			print "/ is a directory" 
			return
		if cache is not None:
			f=[] ; d=[]
			(f,d)=cache2lists(cache)
	
			if name in f: 		print name,"is a file"
			elif name in d: 	print name,"is a directory"
			else: 				print name,"does not exist"
		else:					print name,"does not exist"


	def dirparents(self,path):
		parents=[]
		for i in xrange(len(path.split('/'))):
			(head, tail) = os.path.split(path)
			parents.append(head)
			path = head
		parents.pop()
		return parents


	def thetest(self, search ):
		(directory, name) = os.path.split(search)
		parents = dirparents(search)
	
		cache = agile.getcache( parents[0],0 )
		if cache is None: 
			agile.path2caches( parents[0] )
			cache = agile.getcache( parents[0] )
		
		what(name, cache)


	def	create(self, path, mode):
		if self.verbosity>=3: print 'create(%s,%s)' % (path,str(mode))
		'''f = self.sftp.open(path, 'w')
		f.chmod(mode)
		f.close()'''
		return 0

	def	getattr(self, path, fh=None):
		if self.verbosity>=3: print 'getattr(%s,%s)' % (path,str(fh)),
		path=self.fixpath(path)

		(dirname,filename) = os.path.split(path)
		parents = self.dirparents(path)
		
		self.getcache( parents[0], 0 )
		self.getcache( parents[0], 1 )
		if self.cache[0] is None: 
			self.path2caches( parents[0] ) # maybe it just expired?  retry

		if self.cache[0] is None:
			raise FuseOSError(ENOENT)
			

		l=self.cache[0];f=[];d=[]
		if filename=='': 
			return dict( st_gid=os.getgid(), st_uid=os.getuid(), st_size=666, st_mode=0755|S_IFDIR, st_atime=time.time(), st_ctime=time.time(), st_mtime=time.time() )
		for o in l['files']:
			if filename in o['name']:
				return dict( st_gid=os.getgid(), st_uid=os.getuid(), st_size=o['size'], st_mode=0755|S_IFREG, st_atime=o['ctime'], st_ctime=o['ctime'], st_mtime=o['mtime'] )
		for o in l['directories']:
			if filename in o['name']:
				return dict( st_gid=os.getgid(), st_uid=os.getuid(), st_size=o['size'], st_mode=0755|S_IFDIR, st_atime=o['ctime'], st_ctime=o['ctime'], st_mtime=o['mtime'] )
		raise FuseOSError(ENOENT)

	def	mkdir(self, path, mode):
		if self.verbosity>=3: print 'mkdir(%s,%s)' % (path,str(mode))
		result = self.agile.makeDir(path)
		self.delcache(path,0) ; self.delcache(path,1)
		self.delcache(os.path.split(path)[0],0) ; self.delcache(os.path.split(path)[0],1)
		self.path2caches(os.path.split(path)[0])
		self.path2caches(path)
		return 0

	def	read(self, path, size, offset, fh):
		MAX_RECV=4096
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
		elif self.readlib=='agile':
			if self.verbosity>=3: print "ASKING FOR: %d OF %d, total=%d" % (offset, size, (offset+size))
			big_buffer = ''
			data = ''
			reader = AgileReader(sockpath='/tmp/lama-readerd.sock')
			if self.verbosity: print repr(reader)
			payload = reader.create_payload(url=url, offset=offset, size=size)
			if self.verbosity: print "created payload, offset=%d, size=%d, len: %d" %(offset, size, len(payload))
			if len(payload):
				totalread = 0
				reader.send_request(payload)
				if self.verbosity: print "sent payload to %s" % (reader.sockpath)
				while True:
					data = reader.socket.recv(size)
					if not data: break
					big_buffer += data
					if self.verbosity>=3: print "%d/%d" % (len(data), len(big_buffer))
				reader.socket.close()
				if self.verbosity>=3: print "closed socket"
			else:
				if self.verbosity: print "ERROR: bad payload"
			if self.verbosity: print "returning big_buffer, len: %d" % (len(big_buffer))
			return big_buffer
		else:
			if verbosity: print "%s: invalid read library" % (self.readlib)
			return False

	def	path2caches( self, path='/', dirs_only=False, files_only=False, overrideCache=False ):
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
			for object in fjson['list']: jsonstr = jsonstr + '''{ "name": "'''+object['name']+'''", "ctime": '''+str(object['stat']['ctime'])+''', "mtime": '''+str(object['stat']['mtime'])+''', "size": '''+str(object['stat']['size'])+''' },'''
			jsonstr = jsonstr[0:len(jsonstr)-1] 
			jsonstr = jsonstr + ''' ], "directories": [ '''
			for object in djson['list']: jsonstr = jsonstr + '''{ "name": "'''+object['name']+'''", "ctime": '''+str(object['stat']['ctime'])+''', "mtime": '''+str(object['stat']['mtime'])+''', "size": 0 },'''
			jsonstr = jsonstr[0:len(jsonstr)-1] 
			jsonstr = jsonstr + ''' ] }'''
			self.setcache(cachepath,json.loads(jsonstr),0)
	
			jsonstr = '''{ "path": "'''+path+'''", ''' ; jsonstr = jsonstr + '''"files": [ '''
			for object in fjson['list']: jsonstr = jsonstr + '''"'''+object['name']+'''",'''
			jsonstr = jsonstr[0:len(jsonstr)-1] ; jsonstr = jsonstr + ''' ], ''' ; jsonstr = jsonstr + '''"directories": [ '''
			for object in djson['list']: jsonstr = jsonstr + '''"'''+object['name']+'''",'''
			jsonstr = jsonstr[0:len(jsonstr)-1] ; jsonstr = jsonstr + ''' ] }'''
			self.setcache(cachepath,json.loads(jsonstr),1)


			return self.getcache(cachepath)
		return self.getcache(cachepath)

	def	readdir(self, path, fh):
		if self.verbosity>=3: print 'readdir(%s,%s)' % (path,str(fh))
		path=self.fixpath(path)
		self.path=path
		cache = self.path2caches( path )

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

	def	rename(self, path, newpath):
		if self.verbosity>=3: print 'rename(%s,%s)' % (path,newpath)
		result = self.agile.rename(path, self.root + newpath)
		self.delcache(path,0)
		self.delcache(path,1)
		self.delcache(os.path.split(path)[0],0)
		self.delcache(os.path.split(path)[0],1)
		self.path2caches(os.path.split(path)[0])
		return result


	def	rmdir(self, path):
		if self.verbosity>=3: print 'rmdir(%s)' % (path)
		result = self.agile.deleteDir(path)
		time.sleep(3)
		self.delcache(path,0)
		self.delcache(path,1)
		self.delcache(os.path.split(path)[0],0)
		self.delcache(os.path.split(path)[0],1)
		self.path2caches(os.path.split(path)[0])
		return 0

	def	unlink(self, path):
		if self.verbosity>=3: print 'unlink(%s)' % (path)
		result = self.agile.deleteFile(path)
		time.sleep(3)
		self.delcache(os.path.split(path)[0],0)
		self.delcache(os.path.split(path)[0],1)
		self.path2caches(os.path.split(path)[0])
		return 0

	def	write(self, path, data, offset, fh):
		if self.verbosity>=3: print 'write(%s)' % (path)
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
