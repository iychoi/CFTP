#!/usr/bin/env python
import cftplib
import bytebuffer
import shutil
import os
import sys
import struct
import binascii

from ca import Chunk_Handler

CHUNK_SIZE = 1024

def gettext(ftp, filename, outfile=None):
    # fetch a text file
    if outfile is None:
        outfile = sys.stdout
    # use a lambda to add newlines to the lines read from the server
    ftp.retrlines("RETR " + filename, lambda s, w=outfile.write: w(s+"\n"))

def getbinary(ftp, filename, outfile=None):
    # fetch a binary file
    if outfile is None:
        outfile = sys.stdout
    ftp.retrbinary("RETR " + filename, outfile.write)

def getrecipe(ftp, filename):
    ret = ftp.sendcmd("RRTR " + filename)
    retarr = ret.split(" ")
    if len(retarr) == 2:
        if retarr[0] == "200":
            recipe = retarr[1]
            strs = []
            if len(recipe) % 40 != 0:
                print "length of recipe is not correct"
                return None
    
            for x in range(0, len(recipe) / 40):
                #strs += recipe[x * 40 : x * 40 + 40],
                strs.append(recipe[x * 40 : x * 40 + 40],)
            print "Received a recipe of the file :", filename
            return strs

    return None

def collectchunks(ftp, ca, hashes, outfile=None):
    ca.validate_all_cache()
    # get chunk data from hash
    if outfile is None:
        outfile = sys.stdout
    
    chunks_num = len(hashes)
    req_hashes = []
    req_hash_string = ""
    temp_server_chunk_file = '/tmp/server_chunks'
    
    # check server chunks
    for x in range(0, chunks_num):
        hasLocal = ca.has_chunk(hashes[x])
        if not hasLocal:
            if hashes[x] not in req_hashes:
                print "read from server : ", hashes[x]
                req_hashes.append(hashes[x])
                if req_hash_string != "":
                    req_hash_string += ","
                req_hash_string += hashes[x]
    
    # request server chunks and store it as local temporary file
    if len(req_hashes) != 0:
        f = open(temp_server_chunk_file, 'wb')
        ftp.retrbinary("HRTR " + req_hash_string, f.write)
        f.close()
        ca.build_cache(temp_server_chunk_file)

    # build file
    for x in range(0, chunks_num):
        chunk = ca.get_chunk(hashes[x])
        if chunk == None:
            print "chunk is not in CA"
            return False
        outfile.write(chunk)

    if len(req_hashes) != 0:
        f.close()
        os.remove(temp_server_chunk_file)
        ca.validate_cache(temp_server_chunk_file)
    return True

def getserverhashes(ftp, ca, filename):
    local_file = os.path.abspath(filename)
    ca.build_cache(local_file)
    ca.validate_all_cache()
    recipe = ca.get_hashes(local_file)
    strs = []
    for x in recipe:
        hash = ''.join(x),
        if hash not in strs:
            #strs += ''.join(x),
            strs.append(''.join(x),)

    req_hashes = ""
    for x in range(0, len(strs)):
        if req_hashes != "":
            req_hashes += ","
        req_hashes += strs[x]

    print "check server hashes of the file :", filename
    ret = ftp.sendcmd("RSTR " + req_hashes)
    retarr = ret.split(" ")
    if len(retarr) == 2:
        if retarr[0] == "200":
            recipe = retarr[1]
            strs = []
            if len(recipe) % 40 != 0:
                print "length of recipe is not correct"
                return None
    
            for x in range(0, len(recipe) / 40):
                #strs += recipe[x * 40 : x * 40 + 40],
		strs.append(recipe[x * 40 : x * 40 + 40],)
            print "Received server hashes of the file :", filename
            return strs
    return None

def sendchunks(ftp, ca, hashes, file):
    if len(hashes) == 0:
        return True

    req_hashes = ""
    for x in range(0, len(hashes)):
        if req_hashes != "":
            req_hashes += ","
        req_hashes += hashes[x]

    f = open('/tmp/server_chunks', 'wb')
    for x in range(0, len(hashes)):
        chunk = ca.get_chunk(hashes[x])
        if chunk == None:
            print "chunk is not in CA"
            return False
        
        print "send to server : ", hashes[x]
        f.write(chunk)
    f.close()

    f = open('/tmp/server_chunks', 'rb')
    ftp.storbinary("HSTR " + file, f)
    f.close()
    os.remove('/tmp/server_chunks')
    return True

def buildfile(ftp, ca, file):
    local_file = os.path.abspath(file)
    recipe = ca.get_hashes(local_file)
    strs = []
    for x in recipe:
        #strs += ''.join(x),
        strs.append(''.join(x),)

    req_hashes = ""
    for x in range(0, len(strs)):
        if req_hashes != "":
            req_hashes += ","
        req_hashes += strs[x]

    ftp.voidcmd("BDRC " + file + "," + req_hashes)
    
def getmerkleinfo(ftp, filename):
    ret = ftp.sendcmd("MINF " + filename)
    retarr = ret.split(" ")
    if len(retarr) == 2:
        if retarr[0] == "200":
            return retarr[1]
    return None

def collectmerkletree(ftp, ca, file, height, roothash):
    hashes = []
    build_leaf = {}
    
    if not ca.has_merkle_hash(roothash):
        hashes += roothash,
    
    for depth in range(0, height):
        print "depth :", depth
        #print "request hash :", len(hashes), hashes
        req_hashes = ""
        if len(hashes) == 0:
            break

        for x in range(0, len(hashes)):
            if req_hashes != "":
                req_hashes += ","
            req_hashes += hashes[x]

        ret = ftp.sendcmd("MCRT " + file + "," + req_hashes)
        retarr = ret.split(" ")
        if len(retarr) == 2:
            if retarr[0] == "200":
                recipe = retarr[1]
                strs = []
                if len(recipe) % 40 != 0:
                    print "length of recipe is not correct"
                    return None

                for x in range(0, len(recipe) / 40):
                    merkle_node = recipe[x * 40 : x * 40 + 40]
                    #strs += merkle_node,
                    strs.append(merkle_node,)
                #print "received hash :", len(strs), strs

                for x in range(0, len(hashes)):
                    build_leaf[hashes[x]] = strs[x*ca.get_merkle_base() : x*ca.get_merkle_base()+ca.get_merkle_base()]
                
                print "Received server hashes of the file :", file, "depth :", depth
                hashes = []
                for x in range(0, len(strs)):
                    #check local merkle tree
                    if not ca.has_merkle_hash(strs[x]):
                        #hashes += strs[x],
                        hashes.append(strs[x],)
            else:
                print "error while searching children"
                return None
        else:
            print "error while searching children"
            return None

    leaf_recipe = build_leaf_recipe(ca, roothash, build_leaf, height)
    return leaf_recipe


def build_leaf_recipe(ca, roothash, merkle_tree, height):
    print "build leaf"

    hashes = __build_recipe_from_merkle(ca, roothash, merkle_tree, height)
    #print hashes
    return hashes

def __build_recipe_from_merkle(ca, hash, merkle_tree, height):
    if height <= 0:
        return [hash]

    if ca.has_merkle_hash(hash):
        return ca.get_merkle_leaves(hash)
    else:
        children = merkle_tree[hash]
        ret_hashes = []
        for x in range(0, len(children)):
            child = children[x]
            #ret_hashes += __build_recipe_from_merkle(ca, child, merkle_tree, height -1)
            ret_hashes.append(__build_recipe_from_merkle(ca, child, merkle_tree, height -1))

    return ret_hashes
