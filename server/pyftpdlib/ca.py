import hashlib
import sqlite3
import os
import time
import math
import json

CHUNK_SIZE = 65536
MERKLE_LOG_BASE = 4
 
class Chunk_Handler (object):
   
    table_name = 'caftp'
    
    def __init__(self):
        self.db = sqlite3.connect('/tmp/caftps.sqlite')
        self.cursor = self.db.cursor()
        self.cursor.execute('CREATE TABLE IF NOT EXISTS ' + self.table_name + 
                            ' (id INTEGER PRIMARY KEY AUTOINCREMENT, filepath TEXT UNIQUE, hashes TEXT, last_modified TEXT, file_size INTEGER, merkle_hashes TEXT)')
        self.build_hash_cache()
        self.lastread_hashlist = []
        self.lastread_file = None
    
    def build_hash_cache(self):
        self.hash_cache = {}
        self.merkle_cache = {}
        self.cursor.execute('SELECT hashes, merkle_hashes FROM ' + self.table_name)
        rows = self.cursor.fetchall()
        if rows != None:
            for row in rows:
                for item in row[0].split(':'):
                    self.hash_cache[item] = 1;
                mlist = json.loads(row[1])
                for sublist in mlist:
                    for item in sublist:
                        self.merkle_cache[item] = 1;
                
    def get_chunk(self, chunk_hash):
        chunk = None
        
        # read from cache if present
        if chunk_hash in self.lastread_hashlist:
            offset = self.lastread_hashlist.index(chunk_hash) * CHUNK_SIZE
            self.lastread_file.seek(offset)
            chunk = self.lastread_file.read(CHUNK_SIZE)
            return chunk
            
        self.cursor.execute('SELECT * FROM ' + self.table_name + 
                            ' WHERE hashes LIKE ? OR hashes LIKE ? OR hashes LIKE ? OR hashes LIKE ?',
                            ('%:' + chunk_hash + ':%', chunk_hash + ':%', '%:' + chunk_hash, chunk_hash))
        row = self.cursor.fetchone()
        if row != None:
            
            if self.lastread_file != None:
                self.lastread_file.close()
                
            self.lastread_file = open(row[1], 'rb')
            self.lastread_hashlist = row[2].split(':')
            offset = self.lastread_hashlist.index(chunk_hash) * CHUNK_SIZE
            self.lastread_file.seek(offset)
            chunk = self.lastread_file.read(CHUNK_SIZE)
        return chunk

    def has_chunk(self, chunk_hash):
        if chunk_hash in self.hash_cache:
            return True
        return False
    
    def get_hashes(self, filepath):
        t = ()  # tuple
        self.cursor.execute('SELECT * FROM ' + self.table_name + ' WHERE filepath = ?', (filepath,))
        row = self.cursor.fetchone()
        if row != None:
            t = tuple(row[2].split(':'))
        return t

    def get_merkle_height(self, filepath):
        t = self.get_hashes(filepath)
        return int(math.ceil(math.log(len(t), MERKLE_LOG_BASE)))

    def get_merkle_leaves_count(self, height):
        return int(math.pow(MERKLE_LOG_BASE, height))

    def get_merkle_base(self):
        return MERKLE_LOG_BASE

    def validate_cache(self, filepath):
        self.cursor.execute('SELECT * FROM ' + self.table_name + ' WHERE filepath = ? ', (filepath,))
        row = self.cursor.fetchone()
        
        if row != None:
            if os.path.exists(filepath) and row[3] == time.ctime(os.path.getmtime(filepath)) and row[4] == os.path.getsize(filepath):
                return True
            else:
                self.cursor.execute('DELETE FROM ' + self.table_name + ' WHERE filepath = ? ', (filepath,))
                self.db.commit()
                self.build_hash_cache()
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
                    self.build_hash_cache()
    
    def build_cache(self, filepath):
        if not self.validate_cache(filepath):
            f = open(filepath, 'rb')
            l = []
            merkle_hashes = []
            try:
                while True:
                    chunk = f.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    l.append(hashlib.sha1(chunk).hexdigest())
                
                merkle_hashes = [l, ]
                intermediate_merkle_hashes = l
                for i in range(int(math.ceil(math.log(len(l), MERKLE_LOG_BASE))), 0, -1):
                    list_of_hash_group = (intermediate_merkle_hashes[n:n + MERKLE_LOG_BASE] for n, item in enumerate(intermediate_merkle_hashes) if n % MERKLE_LOG_BASE == 0)
                    intermediate_merkle_hashes = list(hashlib.sha1(":".join(item)).hexdigest() for n, item in enumerate(list_of_hash_group))
                    merkle_hashes = [intermediate_merkle_hashes, ] + merkle_hashes
                    
                self.cursor.execute('INSERT INTO ' + self.table_name + 
                                    '(filepath, hashes, last_modified, file_size, merkle_hashes) VALUES (?,?,?,?,?)',
                                    (filepath, ":".join(l), time.ctime(os.path.getmtime(filepath)), os.path.getsize(filepath), json.dumps(merkle_hashes)))
                self.db.commit()
                for item in l:
                    self.hash_cache[item] = 1
                for sublist in merkle_hashes:
                    for item in sublist:
                        self.merkle_cache[item] = 1
            finally:
                f.close
        
    def get_merkle_children(self, filepath, parents):
        l = []
        self.cursor.execute('SELECT * FROM ' + self.table_name + ' WHERE filepath = ?', (filepath,))
        row = self.cursor.fetchone()

        if row != None:
            merkle_tree_list = json.loads(row[5])
          
            parent = parents[0]
            current_level_index = 0;
            for current_level in merkle_tree_list:
                    if parent in current_level and current_level_index != len(merkle_tree_list) - 1:
                        break
                    current_level_index += 1
                    
            if current_level_index != len(merkle_tree_list) - 1:
                current_level = merkle_tree_list[current_level_index]
                next_level = merkle_tree_list[current_level_index + 1]
                for parent in parents:
                    index_of_parent = current_level.index(parent)
                    l.extend(next_level[index_of_parent * MERKLE_LOG_BASE: index_of_parent * MERKLE_LOG_BASE + MERKLE_LOG_BASE])
        return l

    def get_merkle_root(self, filepath):
        self.cursor.execute('SELECT * FROM ' + self.table_name + ' WHERE filepath = ?', (filepath,))
        row = self.cursor.fetchone()
        
        if row != None:
            merkle_tree_list = json.loads(row[5])
            if len(merkle_tree_list) != 0:
                if len(merkle_tree_list[0]) != 0:
                    return merkle_tree_list[0][0]

        return None

    def has_merkle_hash(self, merkle_hash):
        if merkle_hash in self.merkle_cache:
            return True
        return False
        
    def get_merkle_leaves(self, parent_merkle_hash):
        self.cursor.execute('SELECT * FROM ' + self.table_name + 
                            ' WHERE merkle_hashes like ?',
                             ('%"' + parent_merkle_hash + '"%',))
        row = self.cursor.fetchone()
        
        is_input_leaf = True
        current_level_merkle_hashes = [parent_merkle_hash, ]
        
        if row != None:
            
            merkle_tree_list = json.loads(row[5])

            while True:
                l = []
                for parent in current_level_merkle_hashes:
                    current_level_index = 0
                    for current_level in merkle_tree_list:
                        if parent in current_level and current_level_index != len(merkle_tree_list) - 1:
                            next_level = merkle_tree_list[current_level_index + 1]
                            index_of_parent = current_level.index(parent)
                            l.extend(next_level[index_of_parent * MERKLE_LOG_BASE: index_of_parent * MERKLE_LOG_BASE + MERKLE_LOG_BASE])
                    current_level_index += 1
                
                next_level_merkle_hashes = l
                if not next_level_merkle_hashes:
                    break
                else:
                    is_input_leaf = False
                    current_level_merkle_hashes = next_level_merkle_hashes
                    
            if not is_input_leaf:
                return current_level_merkle_hashes
            else:
                return []
                
        return []
