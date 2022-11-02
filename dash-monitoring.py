#!/usr/bin/env python
# Copyright (c) 2014-2016 The Bitcoin Core developers
# Copyright (c) 2019 thephez
# Distributed under the MIT software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.

import binascii
import zmq
import struct
import sqlite3
import datetime

port = 20003

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Exception as e:
        print(e)
 
    return conn


def create_table(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS blocks(Hash TEXT, ChainLock BOOL, BlockSeenTime TEXT, ChainLockSeenTime TEXT, PRIMARY KEY (Hash))")


def create_table2(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS transactions(Hash TEXT, InstantLock BOOL, BlockSeenTime TEXT, InstantLockSeenTime TEXT, PRIMARY KEY (Hash))")


def is_existing_block(conn, blockhash):
    cur = conn.cursor()
    with conn:
        for row in cur.execute("SELECT ChainLock FROM blocks WHERE Hash = ?", (blockhash,)):
            return True
        else:
            return False

def is_existing_tx(conn, txhash):
    cur = conn.cursor()
    with conn:
        for row in cur.execute("SELECT 1 FROM transactions WHERE Hash = ?", (txhash,)):
            return True
        else:
            return False


def insert_block_data(conn, data):
    cur = conn.cursor()
    with conn:
        try:
            cur.execute("INSERT OR IGNORE INTO blocks VALUES(?, ?, ?, ?)", data)
        except Exception as e:
            print(data)
            raise
    return True

def insert_tx_data(conn, data):
    cur = conn.cursor()
    with conn:
        try:
            cur.execute("INSERT OR IGNORE INTO transactions VALUES(?, ?, ?, ?)", data)
        except Exception as e:
            print(data)
            raise
    return True

def update_block_data(conn, data):
    cur = conn.cursor()
    with conn:
        try:
            cur.execute("UPDATE blocks SET Chainlock = ?, ChainLockSeenTime = ? WHERE Hash = ?", data)
        except Exception as e:
            print(data)
            raise
    return True

def update_tx_data_with_islock(conn, data):
    cur = conn.cursor()
    with conn:
        try:
            assert(data[1] is not None)
            cur.execute("UPDATE transactions SET InstantLock = ?, InstantLockSeenTime = ? WHERE Hash = ?", data)
        except Exception as e:
            print(data)
            raise
    return True

# def update_tx_data_without_islock(conn, data):
#     assert False
#     cur = conn.cursor()
#     with conn:
#         try:
#             cur.execute("UPDATE transactions SET BlockSeenTime = ? WHERE Hash = ?", data)
#         except Exception as e:
#             print(data)
#             raise
#     return True

def process_zmq_message(topic, blockhash):
    now_time = datetime.datetime.utcnow()
    print('{}\tTopic received: {}\tData: {}'.format(now_time, topic, blockhash))

    if topic in ["hashblock", "hashchainlock"]:
        chainlock_status = False
        chainlock_seen_time = None

        # Set ChainLock Status
        if topic == "hashchainlock":
            chainlock_status = True
            chainlock_seen_time = now_time

        existing_block = is_existing_block(conn_cl, blockhash)

        if existing_block:
            # Update Only
            data = (chainlock_status, chainlock_seen_time, blockhash)
            update_block_data(conn_cl, data)
        else:
            # Insert
            data = (blockhash, chainlock_status, now_time, chainlock_seen_time)
            insert_block_data(conn_cl, data)
    else:
        instantlock_status = False
        instantlock_seen_time = None

        # Set ChainLock Status
        if topic == "hashtxlock":
            instantlock_status = True
            instantlock_seen_time = now_time

        if is_existing_tx(conn_is, blockhash):
            # Update Only
            if instantlock_status:
                data = (instantlock_status, instantlock_seen_time, blockhash)
                update_tx_data_with_islock(conn_is, data)
            # else:
            #     data = (now_time, blockhash)
            #     update_tx_data_without_islock(conn, data)

        else:
            # Insert
            data = (blockhash, instantlock_status, now_time, instantlock_seen_time)
            insert_tx_data(conn_is, data)

class MaxSizedList():
    def __init__(self, max_size):
        self.max_size = max_size
        self.list = []
        self.index = 0

    def add_elem(self, elem):
        self.list.append(elem)
        self.index += 1


# Database connection setup
conn_cl = create_connection('dash-chainlock-data.db')
conn_is = create_connection('dash-islock-data.db')
create_table(conn_cl)
create_table2(conn_is)

accepted_zmq = [
    "hashblock",
    "hashchainlock",
    "hashtx",
    "hashtxlock"
]

# ZMQ Setup
zmqContext = zmq.Context()
zmqSubSocket = zmqContext.socket(zmq.SUB)

for type in accepted_zmq:
    zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE, type)
# zmqSubSocket.setsockopt(zmq.SUBSCRIBE, b"hashblock")
# zmqSubSocket.setsockopt(zmq.SUBSCRIBE, b"hashchainlock")
zmqSubSocket.connect("tcp://127.0.0.1:%i" % port)
procesed_transactions = MaxSizedList(10000)

try:
    while True:
        msg = zmqSubSocket.recv_multipart()
        topic = str(msg[0].decode("utf-8"))
        body = msg[1]
        sequence = "Unknown"

        if len(msg[-1]) == 4:
          msgSequence = struct.unpack('<I', msg[-1])[-1]
          sequence = str(msgSequence)

        if topic in accepted_zmq:
            blockhash = binascii.hexlify(body).decode("utf-8")
            if topic == "hashtx":
                if blockhash in procesed_transactions.list:
                    print("SKIPPING DUE TO PROCESSING PREVIOUSLY")
                    size = len(procesed_transactions.list)
                    print(size)
                    procesed_transactions.list.remove(blockhash)
                    if size > 20_000:
                        procesed_transactions.list.pop(0)  # remove the earliest index
                    continue
                else:
                    procesed_transactions.list.append(blockhash)
                    print(len(procesed_transactions.list))

            # == "hashblock" or topic == "hashchainlock":
            process_zmq_message(topic, blockhash)
except KeyboardInterrupt:
    zmqContext.destroy()
