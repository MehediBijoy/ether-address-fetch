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
    const params = queryKey === queryKeys.balance ? item : item?.address
    return new Promise((resolve) => {
      let req = queryFn.request(params, (error, res) => {
        if (error) resolve()
        if (queryKey === queryKeys.balance) {
          resolve({
            address: params,
            [queryKey]: web3.utils.fromWei(res, 'ether'),
          })
        } else {
          resolve({...item, [queryKey]: res})
        }
      })
      batch.add(req)
    })
  })
  batch.execute()

  return Promise.all(promises)
}

const txProcessor = (txs) => {
  return [].concat(...txs?.map((item) => [item?.from, item?.to]))
}

const addressesProcess = async (addresses, blockNumber) => {
  console.log('request balances for total address: --> ', addresses?.length)
  const addressWithBalance = await makeBatchRequest(
    queryKeys.balance,
    web3.eth.getBalance,
    validate(addresses)
  )

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

    if (addresses.length >= 1000) {
      await addressesProcess(addresses, blockNumber)
      saveLastBlock(blockNumber)
      addresses = []
    }

    blockNumber = blockNumberTrack(blockNumber)
  }
}

await main()
