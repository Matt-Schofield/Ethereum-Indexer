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
    db_connection.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        );
    """)


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
    N = 1
    startBlock = web3.eth.blockNumber - N
    endBlock = web3.eth.blockNumber

    # StartBlock < 0 protection
    #

    # Last indexed block
    #

    # Get ABI transfer subset
    for sub_abi in abi:
        if sub_abi.get('name') == 'Transfer':
            transferAbi = sub_abi

    transfer_topic = event_abi_to_log_topic(transferAbi)

    # Filter blocks
    for block_number in range(startBlock, endBlock + 1):
        logs = web3.eth.filter({
            "fromBlock": block_number,
            "toBlock": block_number,
            "topics": [transfer_topic.hex()]
        }).get_all_entries()

        for log in logs:
            if log['data'] == "0x":
                # Skip invalid ERC20 transfers (no data value)
                continue
            else:
                value = log['data']
                from_address = '0x' + log['topics'][1].hex()[26:]
                to_address   = '0x' + log['topics'][2].hex()[26:]
                token_address = log['address']

    

# ---------------------- Main ---------------------- #

# TODO:
# - Add indexing for last 2000 blocks
# - Make it index from the last indexed block + 1

db_connection = db_init()
web3 = provider_init()
abi = parse_abi()

index()