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

merkle_info = cftp_client.getmerkleinfo(ftp, target_file)
if merkle_info == None:
    print "file not exist"
else:
    merkle_infoarr = merkle_info.split(",")
    height = int(merkle_infoarr[0])
    roothash = merkle_infoarr[1]
    print "retrieve merkle tree", "height:", height
    hashes = cftp_client.collectmerkletree(ftp, ca, target_file, height, roothash)
    
    if hashes == None:
        print "file not exist"
    else:
	print hashes
