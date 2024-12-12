import json
import sqlite3
from web3 import Web3, EthereumTesterProvider
from eth_abi import decode_abi
from eth_utils import event_abi_to_log_topic

# RPC URL
ETH_RPC = "http://etharchivebware.upnode.org:7545"

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


# --------------------- Setup ---------------------- #

def provider_init():
    web3 = Web3(Web3.HTTPProvider(ETH_RPC))

    if web3.isConnected():
        print("Connected to provider successfully!")
    else:
        print("Error connecting to provider...")

    return web3

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
    if last_indexed > startBlock:
        startBlock = last_indexed + 1

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
                continue
            else:
                value = log['data']
                from_address = '0x' + log['topics'][1].hex()[26:]
                to_address   = '0x' + log['topics'][2].hex()[26:]
                token_address = log['address']

                update_db(value, from_address, to_address, token_address)
                db_update_count += 1

        last_indexed = block_number
        print(f"[LOG] Indexed block {block_number}")
        print(f"[DATABASE] Upserted {db_update_count} DB entries")

    db_connection.execute("SELECT * FROM balances")
    

# ---------------------- Main ---------------------- #
last_indexed = None

db_connection = db_init()
db_create(db_connection)

web3 = provider_init()
abi = parse_abi()

index()