import Web3 from "web3";
import postgres from "postgres";
import { db_config, rpc_config } from "./config.js";

const db = postgres(db_config);

const url = "https://mainnet.infura.io/v3/d9fadc9580654b62ab44cf0b5d289c53";
const web3 = new Web3(new Web3.providers.HttpProvider(url));

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

const commitPreReq = (object) => {
  return (
    web3.utils.fromWei(object?.balance, "ether") >= 0.5 && object?.count >= 10
  );
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

const commit_to_db = async (object) => {
  const data = db`
      insert into etherAddress (address) values (${object?.address})
    `;

  try {
    if (commitPreReq(object)) await data.execute();
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
            resolve({ address: params, [queryKey]: res });
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

const addressesProcess = async (addresses) => {
  const addressWithBalance = await makeBatchRequest(
    queryKeys.balance,
    web3.eth.getBalance,
    validate(addresses)
  );

  const address = await makeBatchRequest(
    queryKeys.txCount,
    web3.eth.getTransactionCount,
    validate(addressWithBalance, queryKeys.address)
  );
  console.log("processed addresses: --> ", address?.length);
  address.map(commit_to_db);
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
  let blockNumber = rpc_config.startBlock;
  while (true) {
    const data = await getBlock(blockNumber);
    const result = await makeBatchRequest(
      queryKeys.tx,
      web3.eth.getTransaction,
      data?.transactions
    );
    const flatAddresses = [].concat(...result);

    console.log(
      `Transaction found in Block ${blockNumber}: --> `,
      data?.transactions?.length
    );

    console.log(
      `Address found in Block ${blockNumber}: --> `,
      flatAddresses?.length
    );

    await addressesProcess(flatAddresses);
    blockNumber = blockNumberTrack(blockNumber, data?.number);
  }
};

await main();
