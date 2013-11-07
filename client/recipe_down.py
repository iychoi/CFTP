#!/usr/bin/env python
import cftplib
import cftp_client
import bytebuffer
import shutil
import os
import sys
import struct
import binascii

from ca import Chunk_Handler

ca = Chunk_Handler()

ftp = cftplib.FTP()
ftp.connect("localhost", 2121)
ftp.login("user", "12345")

target_file = "sample_down.dat"
local_target_file = os.path.abspath(target_file)
temp_file = "/tmp/client_download.tmp"

if os.path.lexists(local_target_file):
    ca.build_cache(local_target_file)

hashes = cftp_client.getrecipe(ftp, target_file)
if hashes == None:
    print "file not exist"
else:
    dest = open(temp_file, 'wb')
    ret = cftp_client.collectchunks(ftp, ca, hashes, dest)
    dest.close()
    
    if ret:
        if os.path.lexists(local_target_file):
            os.remove(local_target_file)
        os.rename(temp_file, local_target_file)
        print "download complete"
        ca.build_cache(local_target_file)
    else:
        if os.path.lexists(temp_file):
            os.remove(temp_file)
        print "download failed"