import sys
import json
import sqlite3
from web3 import Web3
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
        print("[PROG] Error: Failed to connect to provider.")
        return None

def parse_abi():
    f = open("./erc20.abi.json")
    return json.load(f)

def check_params(cmd, n):
    if len(cmd) != n:
        # If the incorrect number of params was given skip
        print(f"[PROG] Error: erc20index {cmd[1]} takes {n} parameters but found {len(cmd)}.")   
        return False
    else:
        return True


# -------------------- Indexer --------------------- #
def index():
    N = 10 # Change back to 2000
    startBlock = web3.eth.blockNumber - N
    endBlock = web3.eth.blockNumber

    # StartBlock < 0 protection
    if startBlock < 0:
        startBlock = 0;

    # Start from one after last indexed block if applicable
    last_indexed = int(get_meta("lastIndexed"))

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

    # Filter request batching (Web3.py v7.x.x only)
    #
    # batch = web3.eth.make_batch()
    #
    #     filter = web3.eth.filter({
    #         "fromBlock": block_number,
    #         "toBlock": block_number,
    #         "topics": [transfer_topic.hex()]
    #     })
    #
    #     batch.add(filter.get_all_entries())
    #
    # logBatch = batch.execute()
    # # assert...
    #
    #
    # for logs in logBatch:
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

        set_meta("lastIndexed", block_number)

        print(f"[LOG] Indexed block {block_number}")

    db_connection.execute("SELECT * FROM balances")
    

# -------------------- Queryer --------------------- #
def query_wallet(web3, wallet_address):
    res = db_connection.execute("""
        SELECT token_address, balance 
        FROM balances
        WHERE wallet_address=?
    """, ([wallet_address]))

    res = res.fetchall()

    # Counter for formatting output nicely
    count = 0

    print("=" * 60)
    print(f"In Wallet ID: {wallet_address}")
    print("-" * 60)

    for row in res:
        count += 1

        # Fetch token info
        tokenContract = web3.eth.contract(row[0], abi=abi)

        name     = tokenContract.functions.name().call()
        symbol   = tokenContract.functions.symbol().call()
        decimals = tokenContract.functions.decimals().call()

        # Contract query batching (Web3.py v7.x.x only)
        #
        # batch = web3.batch_requests()
        #
        # batch.add(tokenContract.functions.name())
        # batch.add(tokenContract.functions.symbol())
        # batch.add(tokenContract.functions.decimals())
        #
        # responses = batch.execute()
        #
        # name     = responses[0]
        # symbol   = responses[1]
        # decimals = responses[2]

        # Balance number formatting
        balance = str(int(row[1], 16))
        if len(balance) <= decimals:
            # Voodoo magic to handle display in case the balance is less than 1 unit
            balance = ("0" * (decimals - len(balance))) + "0" + balance
        balance = balance[:len(balance)-decimals] + "." + balance[-decimals:]

        print(f"Coin #{count}")
        print(f"- Name: {name} \n- Symbol: {symbol} \n- Balance: {balance}")

    if count == 0:
        print("No tokens in this wallet.")

    print("-" * 60)


# ---------------------- Main ---------------------- #
db_connection = db_init()
db_create(db_connection)

# Fetch last indexed block if it exists
last_indexed = int(get_meta("lastIndexed"))
# try:
#     last_indexed = int(get_meta("lastIndexed"))
# except:
#     last_indexed = -1

# Connect to default provider
DEFAULT_PROVIDER = "http://etharchivebware.upnode.org:7545"
print("[PROG] Connecting to default provider...")
web3 = provider_init(DEFAULT_PROVIDER)

abi = parse_abi()

while True:
    cmd = input("> ")

    cmd = cmd.split(" ")

    # ======== erc20index ========
    if cmd[0] == "erc20index":
        if cmd[1] == "start":
            # Check number of params
            if not check_params(cmd, 3):
                continue
    
            # Change the provider if not connected already or if a different provider was passed to the CLI
            if web3 == None or (cmd[2] != DEFAULT_PROVIDER and cmd[2] != "default"):
                web3 = provider_init(cmd[2])

                # If the connection failed skip
                if web3 == None:
                    continue

            if input("Index continuously? Y/N: ").lower().startswith("y"):
                print("[PROG] Beginning continuous indexing... (CLI provides no interrupt. Use Ctrl+C to stop program.)")
                while True:
                    index()
            else:   
                print("[PROG] Beginning one-time indexing...")
                index()

        elif cmd[1] == "query":
            # Check number of params
            if not check_params(cmd, 3):
                continue

            query_wallet(web3, cmd[2])
   
        elif cmd[1] == "last":
            # Check number of params
            if not check_params(cmd, 2):
                continue

            print(f"Last indexed block: {get_meta('lastIndexed')}")
    # ========== quit ==========
    elif cmd[0] == "quit":
        res = input("Are you sure you want to quit? Y/N: ")
        if res == "y" or res == "Y":
            break
    # ========== help ==========
    elif cmd[0] == "help":
        print(
        """Commands:
        help                        - Displays this message.
        erc20index start [RPC]      - Begins indexing using given RPC.
        erc20index query [Wallet]   - Queries given wallet balance
        erc20index last             - Prints the block number of the last indexed block
        quit                        - Quits the programs
        """)
    # ======== DEFAULT =========
    else:
        print(f"[PROG] Error: Unrecognised command")