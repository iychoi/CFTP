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
temp_file = "/tmp/client_download.tmp"

if os.path.lexists(target_file):
    ca.build_cache(target_file)

depth = cftp_client.getmerkledepth(ftp, target_file)
for x in range(0, depth):
    print "depth :", x
    #hashes = cftp_client.getmerklenodes(ftp, x)
    #print hashes