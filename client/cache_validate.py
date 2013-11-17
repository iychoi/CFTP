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

def validate(ca, filename):
    target_file = filename
    local_target_file = os.path.abspath(target_file)

    if os.path.lexists(local_target_file):
        print "validate cache", local_target_file
        ca.validate_cache(local_target_file)
    else:
        print "file not exist", local_target_file

def main():
    if len(sys.argv) < 2:
        print "command : ./cache_validate.py filename"
    else:
        filename = sys.argv[1]
        ca = Chunk_Handler()
        validate(ca, filename)

if __name__ == "__main__":
    main()
