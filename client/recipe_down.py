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

hashes = cftp_client.getrecipe(ftp, "sample.dat")
dest = open('sample.dat', 'wb')
cftp_client.collectchunks(ftp, ca, hashes, dest)
dest.close()
