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
    buffer.clear()
    ftp.retrbinary("RRTR " + filename, buffer.pushBytes)
    return buffer.toBytes()

def gethashstrings(recipe):
    strs = []    
    if len(recipe) % 20 != 0:
        print "length of recipe is not correct"
        return None
    
    for x in range(0, len(recipe) / 20):
        strs += binascii.hexlify(bytearray(bytes[x * 20 : x * 20 + 20])),
    return strs

def collectchunks(ftp, ca, hashes, outfile=None):
    # get chunk data from hash
    if outfile is None:
        outfile = sys.stdout
    server_chunks = []
    chunks_num = len(hashes)
    req_hashes = ""
    for x in range(0, chunks_num):
        #chunk = ca.get_chunk(hashes[x])
        chunk = None
        if chunk == None:
            print "read from server : ", hashes[x]
            server_chunks.append(x)
            if req_hashes != "":
                req_hashes += ","
            req_hashes += hashes[x]
        else:
            print "read from local : ", hashes[x]
            f = open('chunk_' + str(x), 'wb')
            f.write(chunk)
            f.close()

    print "server chunks : ", server_chunks
    if len(server_chunks) != 0:
        buffer = bytebuffer.ByteBuffer()
        buffer.clear()
        ftp.retrbinary("HRTR " + req_hashes, buffer.pushBytes)
        bufferrem = buffer.length
        print "read size : ", bufferrem
        x = 0
        while bufferrem > 0:
            chunksize = CHUNK_SIZE
            if bufferrem < CHUNK_SIZE:
                chunksize = bufferrem

            print "chunk : ", x
            f = open('chunk_' + str(server_chunks[x]), 'wb')
            f.write(bytearray(buffer.readBytes(chunksize)))
            f.close()
            bufferrem -= chunksize
            x += 1

    for x in range(0, chunks_num):
        filename = 'chunk_' + str(x)
        shutil.copyfileobj(open(filename, 'rb'), outfile)
        os.remove(filename)

ca = Chunk_Handler()
ftp = cftplib.FTP()
ftp.connect("localhost", 2121)
ftp.login("user", "12345")

bytes = getrecipe(ftp, "sample.dat")
hashes = gethashstrings(bytes)
dest = open('sample.dat', 'wb')
collectchunks(ftp, ca, hashes, dest)
dest.close()
