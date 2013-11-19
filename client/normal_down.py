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

def download(ftp, filename):
    target_file = filename
    local_target_file = os.path.abspath(target_file)
    temp_file = "/tmp/client_download.tmp"

    dest = open(temp_file, 'wb')
    cftp_client.getbinary(ftp, target_file, dest)
    dest.close()
    
    if os.path.lexists(local_target_file):
        os.remove(local_target_file)
    shutil.move(temp_file, local_target_file)
    print "download complete"

def main():
    if len(sys.argv) < 2:
        print "command : ./normal_down.py filename"
    else:
        filename = sys.argv[1]
        
        ftp = cftplib.FTP()
        ftp.connect("localhost", 2121)
        ftp.login("user", "12345")
        download(ftp, filename)

if __name__ == "__main__":
    main()
