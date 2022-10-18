import Web3 from "web3";
import postgres from "postgres";
import { db_config, ether_config } from "./config.js";

const db = postgres(db_config);
const web3 = new Web3(new Web3.providers.HttpProvider(ether_config.rpc_url));

const queryKeys = {
  tx: "transactions",
  balance: "balance",
  txCount: "count",
  address: "address",
};

const delay = async () => new Promise((resolve) => setTimeout(resolve, 1000));
const blockNumberTrack = (pre, curr) => {
  return typeof pre === "string" ? curr + 1 : pre + 1;
};

const commit_pre_req = (object) => {
  return object?.balance >= 0.5 && object?.count >= 5;
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
      insert into etherAddress (address) values (${object?.address})
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
    const params =
      queryKey === queryKeys.tx || queryKey === queryKeys.balance
        ? item
        : item?.address;
    return new Promise((resolve) => {
      let req = queryFn.request(params, (error, res) => {
        if (!error) {
          if (queryKey === queryKeys.tx) {
            resolve([res?.from, res?.to]);
          } else if (queryKey === queryKeys.balance) {
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

const addressesProcess = async (addresses, blockNumber) => {
  console.log("request for balances...");
  const addressWithBalance = await makeBatchRequest(
    queryKeys.balance,
    web3.eth.getBalance,
    validate(addresses)
  );

  console.log("request for transaction count...");
  const address = await makeBatchRequest(
    queryKeys.txCount,
    web3.eth.getTransactionCount,
    validate(addressWithBalance, queryKeys.address)
  );

  console.log("processed addresses: --> ", address?.length, "\n");
  address.forEach(async (item) => await commit_to_db(item, blockNumber));
};

const getBlock = async (blockNumber) => {
  console.log("request for block: --> ", blockNumber);
  return new Promise(async (resolve) => {
    return await web3.eth.getBlock(blockNumber).then(async (data) => {
      return data
        ? resolve(data)
        : (async () => {
            await delay(),
              await getBlock(blockNumber).then((data) => resolve(data));
          })();
    });
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

    const result = await makeBatchRequest(
      queryKeys.tx,
      web3.eth.getTransaction,
      data?.transactions
    );
    const flatAddresses = [].concat(...result);

    console.log(
      `Address found in Block ${blockNumber}: --> `,
      flatAddresses?.length
    );

    await addressesProcess(flatAddresses);
    blockNumber = blockNumberTrack(blockNumber, data?.number);
  }
};

await main();
