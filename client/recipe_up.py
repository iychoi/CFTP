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

nhashes = cftp_client.getserverhashes(ftp, ca, "sample.dat")
cftp_client.sendchunks(ftp, ca, nhashes, "sample.dat")
cftp_client.buildfile(ftp, ca, "sample.dat")
