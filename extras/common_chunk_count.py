#!/usr/bin/python
import sys
import hashlib
import collections

#Usage: common_chunk_count.py filepath1 filepath2 [chunksize]
CHUNK_SIZE = 1024

def getSize(path):
	fileobject = open(path, 'rb')
	fileobject.seek(0,2) # move the cursor to the end of the file
	size = fileobject.tell()
	fileobject.close()
	return size

def main():
	
	global CHUNK_SIZE
	
	if len(sys.argv) < 3:
		print "Usage: common_chunk_count.py filepath1 filepath2 [chunksize]"
		return

	f1_size = getSize(sys.argv[1])
	f2_size = getSize(sys.argv[2])
	
	f1 = open(sys.argv[1], 'rb')
	f2 = open(sys.argv[2], 'rb')

	if len(sys.argv) > 3:
		CHUNK_SIZE = int(sys.argv[3])

	l1 = [];
	l2 = [];
	
	f1_pos = 0
	f2_pos = 0

	f1_percent = 0
	f2_percent = 0

	try:
		while True:
			chunk = f1.read(CHUNK_SIZE)
			if not chunk:
				break
			l1.append(hashlib.sha1(chunk).hexdigest())
			f1_pos += CHUNK_SIZE
			percent = int((f1_pos / float(f1_size)) * 100)
			if f1_percent != percent:
				f1_percent = percent
				print sys.argv[1], ":", f1_percent, "%"
	finally:
		f1.close

	try:
		while True:
			chunk = f2.read(CHUNK_SIZE)
			if not chunk:
				break
			l2.append(hashlib.sha1(chunk).hexdigest())
			f2_pos += CHUNK_SIZE
			percent = int((f2_pos / float(f2_size)) * 100)
			if f2_percent != percent:
				f2_percent = percent
				print sys.argv[2], ":", f2_percent, "%"
	finally:
		f2.close
	
	print "No of chunks in", f1.name + ":" ,len(l1)
	print "No of chunks in", f2.name + ":" ,len(l2)

	#l1dup = [i for i, ct in collections.Counter(l1).items() if ct > 1]
	#l2dup = [i for i, ct in collections.Counter(l2).items() if ct > 1]
	
	l1_redundant = sum([ct-1 for i, ct in collections.Counter(l1).items() if ct > 1])
	l2_redundant = sum([ct-1 for i, ct in collections.Counter(l2).items() if ct > 1])
	
	l1uniq = [i for i, ct in collections.Counter(l1).items() if ct == 1]
	l2uniq = [i for i, ct in collections.Counter(l2).items() if ct == 1]

	l1filtered = set(l1)
	common_hashes = [i for i in l1filtered if i in l2]
	
	print "(Inter-file redundancy, file1 redundancy, file1 unique) - (%d , %d , %d)" %(len(common_hashes), l1_redundant, len(l1) - l1_redundant)

	#for i in range(len(l1)):
	#	if l1[i] in l2:
	#		sys.stdout.write('#')
	#	else:
	#		sys.stdout.write('_')
	#print 
	#for i in range(len(l2)):
	#	if l2[i] in l1:
	#		sys.stdout.write('#')
	#	else:
	#		sys.stdout.write('_')
	#print 
	#sys.stdout.flush()



if __name__ == "__main__":
    main()
