import hashlib
import sqlite3

CHUNK_SIZE = 1024
 
class Chunk_Handler (object):
   
    table_name = 'caftp'
    
    def __init__(self):
        self.db = sqlite3.connect('/tmp/caftp.sqlite')
        self.cursor = self.db.cursor()
        self.cursor.execute('CREATE TABLE IF NOT EXISTS ' + self.table_name + 
                            ' (id INTEGER PRIMARY KEY AUTOINCREMENT, filepath TEXT UNIQUE, hashes TEXT)')
        
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
    
    def get_hashes(self, filepath):
        t = () # tuple
        self.cursor.execute('SELECT * FROM ' + self.table_name + ' WHERE filepath = ? ', (filepath,))
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
                                    '(filepath, hashes) VALUES (?,?)', (filepath, ":".join(t)))
                self.db.commit()
            finally:
                f.close

        return t
