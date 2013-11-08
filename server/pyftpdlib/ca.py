import hashlib
import sqlite3
import os
import time
import math
import json

CHUNK_SIZE = 1024
MERKLE_LOG_BASE = 4
 
class Chunk_Handler (object):
   
    table_name = 'caftp'
    
    def __init__(self):
        self.db = sqlite3.connect('/tmp/caftps.sqlite')
        self.cursor = self.db.cursor()
        self.cursor.execute('CREATE TABLE IF NOT EXISTS ' + self.table_name + 
                            ' (id INTEGER PRIMARY KEY AUTOINCREMENT, filepath TEXT UNIQUE, hashes TEXT, last_modified TEXT, file_size INTEGER, merkle_hashes TEXT)')
        
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

    ''' TODO: root's depth is 1 or 0?'''
    def get_merkle_depth(self, filepath):
        t = self.get_hashes(filepath)
        return int(math.ceil(math.log(len(t), MERKLE_LOG_BASE)))

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
            merkle_hashes = []
            try:
                while True:
                    chunk = f.read(CHUNK_SIZE)  # for now, chunk size is 1KB
                    if not chunk:
                        break
                    t = t + (hashlib.sha1(chunk).hexdigest(),)
                
                merkle_hashes = [t, ]
                intermediate_merkle_hashes = t
                for i in range(int(math.ceil(math.log(len(t), MERKLE_LOG_BASE))), 0, -1):
                    list_of_hash_group = (intermediate_merkle_hashes[n:n + MERKLE_LOG_BASE] for n, item in enumerate(intermediate_merkle_hashes) if n % MERKLE_LOG_BASE == 0)
                    intermediate_merkle_hashes = tuple(hashlib.sha1(":".join(item)).hexdigest() for n, item in enumerate(list_of_hash_group))
                    merkle_hashes = [intermediate_merkle_hashes, ] + merkle_hashes
                    
                self.cursor.execute('INSERT INTO ' + self.table_name + 
                                    '(filepath, hashes, last_modified, file_size, merkle_hashes) VALUES (?,?,?,?,?)',
                                    (filepath, ":".join(t), time.ctime(os.path.getmtime(filepath)), os.path.getsize(filepath), json.dumps(merkle_hashes)))
                self.db.commit()
            finally:
                f.close
        
    def get_merkle_children(self, filepath, parents):
        l = []
        self.cursor.execute('SELECT * FROM ' + self.table_name + ' WHERE filepath = ?', (filepath,))
        row = self.cursor.fetchone()

        if row != None:
            merkle_tree_list = json.loads(row[5])
          
            for parent in parents:
                for current_level in merkle_tree_list:
                    if parent in current_level and merkle_tree_list.index(current_level) != len(merkle_tree_list) - 1:
                        next_level = merkle_tree_list[merkle_tree_list.index(current_level) + 1]
                        index_of_parent = current_level.index(parent)
                        l.extend(next_level[index_of_parent * MERKLE_LOG_BASE: index_of_parent * MERKLE_LOG_BASE + MERKLE_LOG_BASE])
        return l

    def has_merkle_hash(self, merkle_hash):
        self.cursor.execute('SELECT * FROM ' + self.table_name + 
                            ' WHERE merkle_hashes like ?',
                             ('%"' + merkle_hash + '"%',))
        row = self.cursor.fetchone()

        if row != None:
            return True
        return False
        
