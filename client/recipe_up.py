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

target_file = "sample_up.dat"

nhashes = cftp_client.getserverhashes(ftp, ca, target_file)
if nhashes == None:
    print "error while checking server hashes"
else:
    ret = cftp_client.sendchunks(ftp, ca, nhashes, target_file)
    if ret:
        cftp_client.buildfile(ftp, ca, target_file)
        print "upload complete"
    else:
        print "error while sending chunks"
