# legume. Copyright 2009-2010 Dale Reidy. All rights reserved.
# See LICENSE for details.
import struct

class BufferError(Exception): pass
 
class ByteBuffer(object):
    '''
    Provides a simplified method of reading struct packed data from
    a string buffer.
    readBytes and readStruct remove the read data from the string buffer.
    '''
 
    def __init__(self):
        self._bytes = []

    def clear(self):
        self._bytes = []

    def close():
        self._bytes = None
 
    def readBytes(self, bytes_to_read):
        if bytes_to_read > len(self._bytes):
            raise BufferError, (
                'Cannot read %d bytes, buffer too small (%d bytes)' \
                % (bytes_to_read, len(self._bytes)))
 
        result = self._bytes[:bytes_to_read]
        self._bytes = self._bytes[bytes_to_read:]
        return result

    def toBytes(self):
        result = self._bytes
        return result
 
    def peekBytes(self, bytes_to_peek):
        if bytes_to_peek > len(self._bytes):
            raise BufferError, (
                'Cannot peek %d bytes, buffer too small (%d bytes)' \
                % (bytes_to_peek, len(self._bytes)))
 
        return self._bytes[:bytes_to_peek]
 
    def pushBytes(self, bytes):
        #self._bytes += bytes
        self._bytes.extend(bytes)

    def write(self, bytes):
        #self._bytes += bytes
        self._bytes.extend(bytes)

    def read(self, blocksize):
        return self.readBytes(blocksize)
 
    def readStruct(self, struct_format):
        struct_size = struct.calcsize('!'+struct_format)

        try:
            struct_bytes = self.readBytes(struct_size)
            bytes = struct.unpack('!'+struct_format, struct_bytes)
        except struct.error, e:
            raise BufferError, 'Unable to unpack data'
        except BufferError, e:
            raise BufferError(
                'Could not unpack using format %s' % struct_format, e)
 
        return bytes
 
    def peekStruct(self, struct_format):
        struct_size = struct.calcsize('!'+struct_format)
 
        try:
            struct_bytes = self.peekBytes(struct_size)
            bytes = struct.unpack('!'+struct_format, struct_bytes)
        except struct.error, e:
            raise BufferError, 'Unable to unpack data'
        except BufferError, e:
            raise BufferError(
                'Could not unpack using format %s' % struct_format, e)
 
        return bytes
 
    def isEmpty(self):
        return len(self._bytes) == 0
 
    @property
    def length(self):
        return len(self._bytes)

 
