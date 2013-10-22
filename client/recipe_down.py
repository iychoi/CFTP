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
temp_file = "/tmp/temp_download"

if os.path.lexists(target_file):
    ca.build_cache(target_file)
hashes = cftp_client.getrecipe(ftp, target_file)
dest = open(temp_file, 'wb')
cftp_client.collectchunks(ftp, ca, hashes, dest)
dest.close()
if os.path.lexists(target_file):
    os.remove(target_file)
os.rename(temp_file, target_file)
print "down complete"
ca.build_cache(target_file)