import Web3 from 'web3'
import postgres from 'postgres'
import {writeFile, readFileSync} from 'fs'

import {db_config, ether_config} from './config.js'

const db = postgres(db_config)
const web3 = new Web3(new Web3.providers.HttpProvider(ether_config.rpc_url))

const queryKeys = {
  balance: 'balance',
  txCount: 'count',
  address: 'address',
}

const path = 'ether_block.txt'

const blockNumberTrack = (curr) => parseInt(curr) + 1
const delay = async () => new Promise((resolve) => setTimeout(resolve, 1000))
const commit_pre_req = (object) => {
  return web3.utils.fromWei(object?.balance, 'ether') >= 0.5
}

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
    return parseInt(data) + 1
  } catch {
    return parseInt(ether_config.startBlock) + 1
  }
}

const validate = (items, validateKey) => {
  if (!validateKey)
    return items.filter((item) => item && web3.utils.checkAddressChecksum(item))

  return items.filter(
    (item) =>
      item?.[validateKey] &&
      web3.utils.checkAddressChecksum(item?.[validateKey])
  )
}

const commit_to_db = async (object, blockNumber) => {
  const data = db`
      insert into etherAddress (address) 
      values (${object?.address})
    `

  try {
    if (commit_pre_req(object)) await data.execute()
  } catch {
    data.cancel()
  }
}

const makeBatchRequest = (queryKey, queryFn, lists) => {
  const batch = new web3.BatchRequest()

  let promises = lists.map((item) => {
    const params = item?.address
    return new Promise((resolve) => {
      let req = queryFn.request(params, (error, res) => {
        if (error) return resolve()
        else return resolve({...item, [queryKey]: res})
      })
      batch.add(req)
    })
  })
  batch.execute()

  return Promise.all(promises)
}

const txProcessor = (txs) => {
  return [].concat(
    ...txs?.map((item) => [{address: item?.from}, {address: item?.to}])
  )
}

const addressesProcess = async (addresses, blockNumber) => {
  console.log('request balances for total address: --> ', addresses?.length)
  const getBalances = await makeBatchRequest(
    queryKeys.balance,
    web3.eth.getBalance,
    validate(addresses, queryKeys.address)
  )

  const addressWithBalance = getBalances.filter(Boolean)
  console.log('processed addresses: --> ', addressWithBalance?.length, '\n')
  addressWithBalance.forEach((item) => commit_to_db(item, blockNumber))
}

const getBlock = (blockNumber) => {
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
  let addresses = []
  while (true) {
    const data = await getBlock(blockNumber)

    console.log('Block Found: --> ', blockNumber)
    console.log(
      `Transaction found in Block ${blockNumber}: --> `,
      data?.transactions?.length
    )

    const addressInTxs = txProcessor(data?.transactions)
    console.log(
      `Address found in Block ${blockNumber}: --> `,
      addressInTxs?.length
    )

    addresses.push(...addressInTxs)

    if (addresses.length >= ether_config.batchSize) {
      const addressForProcess = addresses.splice(0, ether_config.batchSize)
      await addressesProcess(addressForProcess, blockNumber)
      saveLastBlock(blockNumber)
    }

    blockNumber = blockNumberTrack(blockNumber)
  }
}

await main()
