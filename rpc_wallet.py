from web3 import HTTPProvider, Web3
from erc20_abi import abi

def get_wallet_balance(wallet_address, token_address, token_decimals):
    w3 = Web3(HTTPProvider('https://eth.llamarpc.com'))
    wallet_addr = w3.toChecksumAddress(wallet_address)
    token_addr = w3.toChecksumAddress(token_address)
    contract = w3.eth.contract(address=token_addr, abi=abi)
    raw_balance = contract.functions.balanceOf(wallet_addr).call()

    return raw_balance/10**token_decimals