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

def getserverhashes(ftp, ca, filename):
    ca.validate_all_cache()
    ca.build_cache(filename)
    recipe = ca.get_hashes(filename)
    strs = []
    for x in recipe:
        strs += ''.join(x),
    #print strs

    req_hashes = ""
    for x in range(0, len(strs)):
        if req_hashes != "":
            req_hashes += ","
        req_hashes += strs[x]

    buffer = bytebuffer.ByteBuffer()
    ftp.retrbinary("RSTR " + req_hashes, buffer.pushBytes)
    nhashes = buffer.toBytes()
    print nhashes
    nhashes_strs = ""
    for x in range(0, len(nhashes) / 20):
        nhashes_strs += binascii.hexlify(bytearray(nhashes[x * 20 : x * 20 + 20])),

    return nhashes_strs

def collectchunks(ftp, ca, hashes, outfile=None):
    ca.validate_all_cache()
    # get chunk data from hash
    if outfile is None:
        outfile = sys.stdout
    
    chunks_num = len(hashes)
    req_hashes = []
    req_hash_string = ""
    temp_server_chunk_file = '/tmp/server_chunks'
    
    # check server chunks
    for x in range(0, chunks_num):
        hasLocal = ca.has_chunk(hashes[x])
        if not hasLocal:
            if hashes[x] not in req_hashes:
                print "read from server : ", hashes[x]
                req_hashes.append(hashes[x])
                if req_hash_string != "":
                    req_hash_string += ","
                req_hash_string += hashes[x]
    
    # request server chunks and store it as local temporary file
    if len(req_hashes) != 0:
        f = open(temp_server_chunk_file, 'wb')
        ftp.retrbinary("HRTR " + req_hash_string, f.write)
        f.close()
        ca.build_cache(temp_server_chunk_file)

    # build file
    for x in range(0, chunks_num):
        chunk = ca.get_chunk(hashes[x])
        if chunk == None:
            print "chunk is not in CA"
            return
        
        outfile.write(chunk)

    if len(req_hashes) != 0:
        f.close()
        os.remove(temp_server_chunk_file)
        ca.validate_cache(temp_server_chunk_file)

def sendchunks(ftp, ca, hashes, file):
    if len(hashes) == 0:
        return

    req_hashes = ""
    for x in range(0, len(hashes)):
        if req_hashes != "":
            req_hashes += ","
        req_hashes += hashes[x]

    f = open('/tmp/server_chunks', 'wb')
    for x in range(0, len(hashes)):
        chunk = ca.get_chunk(hashes[x])
        if chunk == None:
            print "chunk is not in CA"
            return
        
        f.write(bytearray.fromhex(hashes[x]))
        f.write(chunk)
    f.close()

    f = open('/tmp/server_chunks', 'rb')
    ftp.storbinary("HSTR " + file, f)
    f.close()
    os.remove('/tmp/server_chunks')

def buildfile(ftp, ca, file):
    recipe = ca.get_hashes(file)
    strs = []
    for x in recipe:
        strs += ''.join(x),

    req_hashes = ""
    for x in range(0, len(strs)):
        if req_hashes != "":
            req_hashes += ","
        req_hashes += strs[x]

    ftp.voidcmd("BDRC " + file + "," + req_hashes)
    
def getmerkledepth(ftp, filename):
    ret = ftp.sendcmd("CMRT " + filename)
    retarr = ret.split(" ")
    if len(retarr) == 2:
        if retarr[0] == "200":
            return int(retarr[1])
    return -1
