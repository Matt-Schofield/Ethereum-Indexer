import sys
import json
import sqlite3
from web3 import Web3, EthereumTesterProvider
from eth_abi import decode_abi
from eth_utils import event_abi_to_log_topic

# -------------------- Database -------------------- #

def db_init():
    return sqlite3.connect("database.db")

def db_create(db_connection):
    db_connection.execute("""
        CREATE TABLE IF NOT EXISTS balances (
            wallet_address TEXT NOT NULL,
            token_address TEXT NOT NULL,
            balance TEXT NOT NULL,
            PRIMARY KEY (wallet_address, token_address)
        );
    """)

    db_connection.execute("""
        CREATE TABLE IF NOT EXISTS persistentMetadata (
            key TEXT PRIMARY KEY,
            value TEXT
        );
    """)

def update_db(val, f_add, t_add, tk_add):
    # Update sender wallet information
    db_connection.execute("""
        INSERT INTO balances
        VALUES (?,?,?)
        ON CONFLICT(wallet_address, token_address) DO UPDATE SET
            balance=?                          
    """, (f_add, tk_add, val, val))

    # Update receiver wallet information
    db_connection.execute("""
        INSERT INTO balances
        VALUES (?,?,?) 
        ON CONFLICT(wallet_address, token_address) DO UPDATE SET
            balance=?               
    """, (t_add, tk_add, val, val))

    db_connection.commit() 

def set_meta(key, value):
    db_connection.execute("""
        INSERT INTO persistentMetadata
        VALUES (?, ?)    
        ON CONFLICT(key) DO UPDATE SET
            value=?              
    """, (key, value, value))

    db_connection.commit() 

def get_meta(key):
    res = db_connection.execute("""
        SELECT value 
        FROM persistentMetadata
        WHERE key=?
    """, ([key]))

    return res.fetchone()[0]



# --------------------- Setup ---------------------- #

def provider_init(eth_rpc):
    web3 = Web3(Web3.HTTPProvider(eth_rpc))

    if web3.isConnected():
        print("[PROG] Connected to provider successfully!")
        return web3
    else:
        return False

def parse_abi():
    f = open("./erc20.abi.json")
    return json.load(f)


# -------------------- Indexer --------------------- #

def index():
    N = 1 # Change back to 2000
    startBlock = web3.eth.blockNumber - N
    endBlock = web3.eth.blockNumber

    # StartBlock < 0 protection
    if startBlock < 0:
        startBlock = 0;

    # Start from one after last indexed block if applicable
    last_indexed = int(get_meta("lastIndexed"))

    print(last_indexed)

    if last_indexed > startBlock:
        startBlock = last_indexed + 1

    print(startBlock, endBlock)

    # Get ABI transfer subset
    for sub_abi in abi:
        if sub_abi.get('name') == 'Transfer':
            transfer_abi = sub_abi

    transfer_topic = event_abi_to_log_topic(transfer_abi)

    # Filter blocks
    for block_number in range(startBlock, endBlock + 1):
        logs = web3.eth.filter({
            "fromBlock": block_number,
            "toBlock": block_number,
            "topics": [transfer_topic.hex()]
        }).get_all_entries()

        db_update_count = 0

        for log in logs:
            if log['data'] == "0x":
                # Skip invalid ERC20 transfers (no data value)
                # - Are "0x000...0000" wallet address considered valid?
                continue
            else:
                value = log['data']
                from_address = '0x' + log['topics'][1].hex()[26:]
                to_address   = '0x' + log['topics'][2].hex()[26:]
                token_address = log['address']

                update_db(value, from_address, to_address, token_address)
                db_update_count += 1

        set_meta("lastIndexed", block_number)

        print(f"[LOG] Indexed block {block_number}")
        print(f"[DATABASE] Upserted {db_update_count} DB entries")

    db_connection.execute("SELECT * FROM balances")
    

# ---------------------- Main ---------------------- #

# TODO:
# - Batch indexing?
# - Continuous indexing?
# - Interruption safety?
# - Wallet queries

# RPC: "http://etharchivebware.upnode.org:7545"

last_indexed = None

db_connection = db_init()
db_create(db_connection)

set_meta("lastIndexed", -1)

abi = parse_abi()

while True:
    cmd = input("> ")

    cmd = cmd.split(" ")

    if cmd[0] == "erc20index":
        if len(cmd) != 3:
            print(f"[PROG] Error: erc20index takes 3 parameters but only found only {len(cmd)}.")
                    
        if cmd[1] == "start":
            web3 = provider_init(cmd[2])

            if web3 == False:
                print(f"[PROG] Error: Invalid RPC parameter.")
            else:
                print(f"[PROG] Beginning indexing...")
                index()
        
        if cmd[1] == "query":
            None

    elif cmd[0] == "quit":
        res = input("Are you sure you want to quit? Y/N: ")
        if res == "y" or res == "Y":
            break

    elif cmd[0] == "help":
        print(
        """Commands:
        help                        - Displays this message.
        erc20index start [RPC]      - Begins indexing using given RPC.
        erc20index start [Wallet]   - Queries given wallet balance
        quit                        - Quits the programs
        """)
    else:
        print(f"[PROG] Error: Unrecognised command")