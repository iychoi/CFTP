import hashlib
import sqlite3
import os
import time
import math

CHUNK_SIZE = 1024
 
class Chunk_Handler (object):
   
    table_name = 'caftp'
    
    def __init__(self):
        self.db = sqlite3.connect('/tmp/caftps.sqlite')
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
        t = ()  # tuple
        self.cursor.execute('SELECT * FROM ' + self.table_name + ' WHERE filepath = ?', (filepath,))
        row = self.cursor.fetchone()
        if row != None:
            t = tuple(row[2].split(':'))
        return t

    def get_merkle_depth(self, filepath):
        t = self.get_hashes(filepath)
        return math.ceil(math.log(len(t), 2))

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
    
    def validate_cache(self, filepath):
        self.cursor.execute('SELECT * FROM ' + self.table_name + ' WHERE filepath = ? ', (filepath,))
        row = self.cursor.fetchone()
        
        if row != None:
            if os.path.exists(filepath) and row[3] == time.ctime(os.path.getmtime(filepath)) and row[4] == os.path.getsize(filepath):
                return True
            else:
                self.cursor.execute('DELETE FROM ' + self.table_name + ' WHERE filepath = ? ', (filepath,))
                self.db.commit()
                return False
        return False

    def validate_all_cache(self):
        self.cursor.execute('SELECT * FROM ' + self.table_name)
        rows = self.cursor.fetchall()
        if rows != None:
            for row in rows:
                if os.path.exists(row[1]) and row[3] == time.ctime(os.path.getmtime(row[1])) and row[4] == os.path.getsize(row[1]):
                    pass
            else:
                self.cursor.execute('DELETE FROM ' + self.table_name + ' WHERE filepath = ? ', (row[1],))
                self.db.commit()
    
    def build_cache(self, filepath):
        if not self.validate_cache(filepath):
            f = open(filepath, 'rb')
            t = ()  # tuple
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
