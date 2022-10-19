import Web3 from 'web3'
import postgres from 'postgres'
import {writeFile, readFileSync} from 'fs'

import {db_config, bnb_config} from './config.js'

const db = postgres(db_config)
const web3 = new Web3(new Web3.providers.HttpProvider(bnb_config.rpc_url))

const path = 'binance_block.txt'

const blockNumberTrack = (curr) => parseInt(curr) + 1
const commit_pre_req = (object) => object?.balance >= 0.5
const delay = async () => new Promise((resolve) => setTimeout(resolve, 1000))

const saveLastBlock = (blockNumber) => {
  try {
    writeFile(path, blockNumber.toString(), (error) => {
      if (error) console.log(error)
    })
  } catch (e) {
    console.log(e)
  }
}

const readInitalBlock = () => {
  try {
    const data = readFileSync(path, 'utf8')
    return parseInt(data)
  } catch {
    return bnb_config.startBlock
  }
}

const commit_to_db = async (object, blockNumber) => {
  const data = db`
    insert into etherAddress (address) 
    values (${object?.address})
  `
  try {
    await data.execute()
  } catch {
    data.cancel()
  }
}

const processAddress = async (address, blockNumber) => {
  try {
    const balance = await web3.eth.getBalance(address)
    const object = {
      address: address,
      balance: web3.utils.fromWei(balance, 'ether'),
      blockNumber: blockNumber,
    }
    if (commit_pre_req(object)) await commit_to_db(object, blockNumber)
  } catch {}
}

const txProcessor = (txs) => {
  return [].concat(...txs?.map((item) => [item?.from, item?.to]))
}

const getBlock = async (blockNumber) => {
  console.log('request for block: --> ', blockNumber)
  return new Promise(async (resolve) => {
    const block = await web3.eth.getBlock(blockNumber, true)
    return block
      ? resolve(block)
      : (async () => {
          await delay()
          getBlock(blockNumber).then((new_block) => resolve(new_block))
        })()
  })
}

const main = async () => {
  let blockNumber = readInitalBlock()

  while (true) {
    const data = await getBlock(blockNumber)
    const addresses = txProcessor(data?.transactions)
    console.log(
      `Addresses found in Block ${blockNumber}: --> `,
      addresses?.length
    )
    addresses.forEach((address) => processAddress(address))
    saveLastBlock(blockNumber)
    blockNumber = blockNumberTrack(blockNumber)
  }
}

await main()
