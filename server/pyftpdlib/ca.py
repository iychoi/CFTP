import hashlib
import sqlite3
import os
import time

CHUNK_SIZE = 1024
 
class Chunk_Handler (object):
   
    table_name = 'caftp'
    
    def __init__(self):
        self.db = sqlite3.connect('/tmp/caftp.sqlite')
        self.cursor = self.db.cursor()
        self.cursor.execute('CREATE TABLE IF NOT EXISTS ' + self.table_name + 
                            ' (id INTEGER PRIMARY KEY AUTOINCREMENT, filepath TEXT UNIQUE, hashes TEXT, last_modified TEXT, file_size INTEGER)')
        
    def get_chunk(self, chunk_hash):
        chunk = None
        self.cursor.execute('SELECT * FROM ' + self.table_name + 
                            ' WHERE hashes LIKE ? OR hashes LIKE ? OR hashes LIKE ? OR hashes LIKE ?',
                            ('%:' + chunk_hash + ':%', chunk_hash + ':%', '%:' + chunk_hash, chunk_hash))
        row = self.cursor.fetchone()
        if row != None:
            f = open(row[1], 'rb')
            try:
                offset = row[2].split(':').index(chunk_hash) * CHUNK_SIZE
                f.seek(offset)
                chunk = f.read(CHUNK_SIZE)
            finally:
                f.close
        return chunk

    def has_chunk(self, chunk_hash):
        self.cursor.execute('SELECT * FROM ' + self.table_name + 
                            ' WHERE hashes LIKE ? OR hashes LIKE ? OR hashes LIKE ? OR hashes LIKE ?',
                            ('%:' + chunk_hash + ':%', chunk_hash + ':%', '%:' + chunk_hash, chunk_hash))
        row = self.cursor.fetchone()
        if row != None:
            return True
        return False
    
    def get_hashes(self, filepath):
        
        self.__validate_cache(filepath)
        
        t = ()  # tuple
        self.cursor.execute('SELECT * FROM ' + self.table_name + ' WHERE filepath = ?', (filepath,))
        row = self.cursor.fetchone()
        if row != None:
            t = tuple(row[2].split(':'))
        else:
            f = open(filepath, 'rb')
            try:
                while True:
                    chunk = f.read(CHUNK_SIZE)  # for now, chunk size is 1KB
                    if not chunk:
                        break
                    t = t + (hashlib.sha1(chunk).hexdigest(),)
                self.cursor.execute('INSERT INTO ' + self.table_name + 
                                    '(filepath, hashes, last_modified, file_size) VALUES (?,?,?,?)',
                                    (filepath, ":".join(t), time.ctime(os.path.getmtime(filepath)), os.path.getsize(filepath)))
                self.db.commit()
            finally:
                f.close

        return t

    def get_merkle_hashes(self, filepath, tree_level):
        
        t = self.get_hashes(filepath)
        no_of_hashes_to_return = pow(2, tree_level)
       
        if len(t) <= no_of_hashes_to_return:
                return t
        
        group_size = len(t) / no_of_hashes_to_return;
       
        if group_size == 1:
            return t
       
        list_of_hash_group = [ t[n:n + group_size] for n, item in enumerate(t) if n % group_size == 0 ]
        merkle_hashes = [hashlib.sha1(":".join(item)).hexdigest() for n, item in enumerate(list_of_hash_group)]
       
        return tuple(merkle_hashes);
    
    def __validate_cache(self, filepath):
        self.cursor.execute('SELECT * FROM ' + self.table_name + ' WHERE filepath = ? ', (filepath,))
        row = self.cursor.fetchone()
        
        if row != None:
            if os.path.exists(filepath) and row[3] == time.ctime(os.path.getmtime(filepath)) and row[4] == os.path.getsize(filepath):
                return
            else:
                self.cursor.execute('DELETE FROM ' + self.table_name + ' WHERE filepath = ? ', (filepath,))
                self.db.commit()
        
