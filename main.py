import sqlite3
from web3 import Web3, EthereumTesterProvider
from eth_abi import decode_abi
from eth_utils import event_abi_to_log_topic

# RPC URL
ETH_RPC = "http://etharchivebware.upnode.org:7545"

web3 = Web3(Web3.HTTPProvider(ETH_RPC))
print(web3.isConnected())
# print(web3.eth.getBlock(web3.eth.blockNumber))

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

db_connection = db_init()