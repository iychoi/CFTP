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

def check(ca, ftp, filename):
    target_file = filename
    local_target_file = os.path.abspath(target_file)
    temp_file = "/tmp/client_download.tmp"

    if os.path.lexists(local_target_file):
        ca.build_cache(local_target_file)

    hashes = cftp_client.getrecipe(ftp, target_file)
    if hashes == None:
        print "file not exist"
    else:
        cftp_client.testcollectchunks(ftp, ca, hashes, None)
        print "check succeed"

def main():
    if len(sys.argv) < 2:
        print "command : ./searchtime_recipe.py filename"
    else:
        filename = sys.argv[1]
        ca = Chunk_Handler()

        ftp = cftplib.FTP()
        ftp.connect("localhost", 2121)
        ftp.login("user", "12345")
        check(ca, ftp, filename)

if __name__ == "__main__":
    main()
