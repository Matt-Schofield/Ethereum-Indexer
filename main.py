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

    print("Connected to provider:", web3.isConnected())

    # Handle any connection exceptions here...

    return web3

def parse_abi():
    f = open("./erc20.abi.json")
    return json.load(f)


# -------------------- Indexer --------------------- #

def index():
    block_range = range(web3.eth.blockNumber - 2000, web3.eth.blockNumber)


# ---------------------- Main ---------------------- #

db_connection = db_init()
web3 = provider_init()
abi = parse_abi()