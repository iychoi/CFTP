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

def validate(ca):
    print "validate all cache"
    ca.validate_all_cache()

def main():
    ca = Chunk_Handler()
    validate(ca)

if __name__ == "__main__":
    main()
