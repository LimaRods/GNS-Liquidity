from web3 import Web3

infura_url = "https://mainnet.infura.io/v3/69dd01c0890246e09bdbcf9fb85a81c1"
arb_rpc = "https://arb1.arbitrum.io/rpc"
rinkeby = "https://rinkeby.arbitrum.io/rpc"

def get_web3_client(rpc=arb_rpc):
    return Web3(Web3.HTTPProvider(rpc))

def get_contract(web3_client, address, abi):
    return web3_client.eth.contract(address=address, abi=abi)

def buildTransaction(contract, func_name, *args):
    func = contract.functions[func_name]
    print(*args)
    return func(*args).buildTransaction({
        'chainId': 421611,
        'gas': 70000,
        'maxFeePerGas': Web3.toWei('2', 'gwei'),     
        'maxPriorityFeePerGas': Web3.toWei('1', 'gwei'),
        'nonce': 1,
    })

def get_block(web3_client):
    return web3_client.eth.get_block('latest')

def checksum_list(addresses):
    _addresses =[]
    for address in addresses:
        _addresses.append(Web3.toChecksumAddress(address))
    return _addresses