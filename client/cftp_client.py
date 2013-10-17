#!/usr/bin/env python
import cftplib
import bytebuffer
import shutil
import os
import sys
import struct
import binascii

from ca import Chunk_Handler

CHUNK_SIZE = 1024

def gettext(ftp, filename, outfile=None):
    # fetch a text file
    if outfile is None:
        outfile = sys.stdout
    # use a lambda to add newlines to the lines read from the server
    ftp.retrlines("RETR " + filename, lambda s, w=outfile.write: w(s+"\n"))

def getbinary(ftp, filename, outfile=None):
    # fetch a binary file
    if outfile is None:
        outfile = sys.stdout
    ftp.retrbinary("RETR " + filename, outfile.write)

def getrecipe(ftp, filename):
    # fetch a binary buffer
    buffer = bytebuffer.ByteBuffer()
    ftp.retrbinary("RRTR " + filename, buffer.pushBytes)
    recipe = buffer.toBytes()
    strs = []
    if len(recipe) % 20 != 0:
        print "length of recipe is not correct"
        return None
    
    for x in range(0, len(recipe) / 20):
        strs += binascii.hexlify(bytearray(recipe[x * 20 : x * 20 + 20])),

    return strs

def collectchunks(ftp, ca, hashes, outfile=None):
    # get chunk data from hash
    if outfile is None:
        outfile = sys.stdout
    server_chunks = []
    chunks_num = len(hashes)
    req_hashes = ""
    
    # check server chunks
    for x in range(0, chunks_num):
        #hasLocal = ca.has_chunk(hashes[x])
        hasLocal = False
        if not hasLocal:
            print "read from server : ", hashes[x]
            server_chunks.append(x)
            if req_hashes != "":
                req_hashes += ","
            req_hashes += hashes[x]
    
    # request server chunks and store it as local temporary file
    if len(server_chunks) != 0:
        f = open('/tmp/server_chunks', 'wb')
        ftp.retrbinary("HRTR " + req_hashes, f.write)
        f.close()
    
    # build file
    if len(server_chunks) != 0:
        f = open('/tmp/server_chunks', 'rb')

    for x in range(0, chunks_num):
        chunk = None
        if x in server_chunks:
            chunk = f.read(CHUNK_SIZE)
        else:
            chunk = ca.get_chunk(hashes[x])
        
        outfile.write(chunk)

    if len(server_chunks) != 0:
        f.close()
        os.remove('/tmp/server_chunks')



ca = Chunk_Handler()
ftp = cftplib.FTP()
ftp.connect("localhost", 2121)
ftp.login("user", "12345")

hashes = getrecipe(ftp, "sample.dat")
dest = open('sample.dat', 'wb')
collectchunks(ftp, ca, hashes, dest)
dest.close()
