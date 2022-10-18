import Web3 from "web3";
import postgres from "postgres";
import { db_config, ether_config } from "./config.js";

const db = postgres(db_config);
const web3 = new Web3(new Web3.providers.HttpProvider(ether_config.rpc_url));

const queryKeys = {
  balance: "balance",
  txCount: "count",
  address: "address",
};

const delay = async () => new Promise((resolve) => setTimeout(resolve, 1000));
const blockNumberTrack = (pre, curr) => {
  return typeof pre === "string" ? curr + 1 : pre + 1;
};

const commit_pre_req = (object) => {
  return object?.balance >= 0.5;
};

const validate = (items, validateKey) => {
  return items.filter((item) => {
    if (!validateKey) return item && web3.utils.checkAddressChecksum(item);
    return (
      item?.[validateKey] &&
      web3.utils.checkAddressChecksum(item?.[validateKey])
    );
  });
};

const commit_to_db = async (object, blockNumber) => {
  const data = db`
      insert into etherAddress (address) 
      values (${object?.address})
    `;

  try {
    if (commit_pre_req(object)) await data.execute();
  } catch {
    data.cancel();
  }
};

const makeBatchRequest = async (queryKey, queryFn, lists) => {
  const batch = new web3.BatchRequest();

  let promises = lists.map((item) => {
    const params = queryKey === queryKeys.balance ? item : item?.address;
    return new Promise((resolve) => {
      let req = queryFn.request(params, (error, res) => {
        if (!error) {
          if (queryKey === queryKeys.balance) {
            resolve({
              address: params,
              [queryKey]: web3.utils.fromWei(res, "ether"),
            });
          } else {
            resolve({ ...item, [queryKey]: res });
          }
        }
      });
      batch.add(req);
    });
  });
  batch.execute();

  return Promise.all(promises);
};

const txProcessor = (txs) => {
  return [].concat(...txs?.map((item) => [item?.from, item?.to]));
};

const addressesProcess = async (addresses, blockNumber) => {
  console.log("request for balances...");
  const addressWithBalance = await makeBatchRequest(
    queryKeys.balance,
    web3.eth.getBalance,
    validate(addresses)
  );

  console.log("processed addresses: --> ", addressWithBalance?.length, "\n");
  addressWithBalance.forEach((item) => commit_to_db(item, blockNumber));
};

const getBlock = (blockNumber) => {
  console.log("request for block: --> ", blockNumber);
  return new Promise(async (resolve) => {
    const data = await web3.eth.getBlock(blockNumber, true);
    return await (data
      ? resolve(data)
      : (async () => {
          await delay(),
            await getBlock(blockNumber).then((new_data) => resolve(new_data));
        })());
  });
};

const main = async () => {
  let blockNumber = ether_config.startBlock;
  while (true) {
    const data = await getBlock(blockNumber);

    console.log("Block Found: --> ", blockNumber);
    console.log(
      `Transaction found in Block ${blockNumber}: --> `,
      data?.transactions?.length
    );

    const addresses = txProcessor(data?.transactions);

    console.log(
      `Address found in Block ${blockNumber}: --> `,
      addresses?.length
    );

    await addressesProcess(addresses, blockNumber);
    blockNumber = blockNumberTrack(blockNumber, data?.number);
  }
};

await main();
