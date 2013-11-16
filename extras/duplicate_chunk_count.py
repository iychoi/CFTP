#!/usr/bin/python
import sys
import hashlib
import collections

#Usage: common_chunk_count.py filepath [chunksize]
CHUNK_SIZE = 1024

def getSize(path):
	fileobject = open(path, 'rb')
	fileobject.seek(0,2) # move the cursor to the end of the file
	size = fileobject.tell()
	fileobject.close()
	return size

def main():
	
	global CHUNK_SIZE
	
	if len(sys.argv) < 2:
		print "Usage: common_chunk_count.py filepath [chunksize]"
		return

	f1_size = getSize(sys.argv[1])
	
	f1 = open(sys.argv[1], 'rb')

	if len(sys.argv) > 2:
		CHUNK_SIZE = int(sys.argv[2])

	l1 = [];
	f1_pos = 0
	f1_percent = 0

	try:
		while True:
			chunk = f1.read(CHUNK_SIZE)
			if not chunk:
				break
			l1 = l1 + [hashlib.sha1(chunk).hexdigest(),]
			f1_pos += CHUNK_SIZE
			percent = int((f1_pos / float(f1_size)) * 100)
			if f1_percent != percent:
				f1_percent = percent
				print sys.argv[1], ":", f1_percent, "%"
	finally:
		f1.close
	
	print "No of chunks in", f1.name + ":" ,len(l1)
	
	l1dup = [i for i, ct in collections.Counter(l1).items() if ct > 1]
	print "No of duplicate chunks:", len(l1dup) 

if __name__ == "__main__":
    main()
