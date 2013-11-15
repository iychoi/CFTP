#!/usr/bin/python
import sys
import hashlib

#Usage: common_chunk_count.py filepath1 filepath2 [chunksize]

CHUNK_SIZE = 1024

f1 = open(sys.argv[1], 'rb')
f2 = open(sys.argv[2], 'rb')

if len(sys.argv) > 3:
	CHUNK_SIZE = int(sys.argv[3])

l1 = [];
l2 = [];

try:
	while True:
		chunk = f1.read(CHUNK_SIZE)
		if not chunk:
			break
		l1 = l1 + [hashlib.sha1(chunk).hexdigest(),]
finally:
	f1.close

try:
	while True:
		chunk = f2.read(CHUNK_SIZE)
		if not chunk:
			break
		l2 = l2 + [hashlib.sha1(chunk).hexdigest(),]
finally:
	f2.close
	
print "No of chunks in", f1.name + ":" ,len(l1)
print "No of chunks in", f2.name + ":" ,len(l2)

common_hashes = [i for i in l1 if i in l2]
print "No of common chunks:", len(common_hashes)

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
