import Web3 from "web3";
import postgres from "postgres";
import { db_config, bnb_config } from "./config.js";

const db = postgres(db_config);
const web3 = new Web3(new Web3.providers.HttpProvider(bnb_config.rpc_url));

const delay = async () => new Promise((resolve) => setTimeout(resolve, 1000));
const blockNumberTrack = (pre, curr) => {
  return typeof pre === "string" ? curr + 1 : pre + 1;
};

const validate = (address) => {
  return address && web3.utils.checkAddressChecksum(address);
};

const commit_pre_req = (object) => {
  return (
    web3.utils.fromWei(object?.balance, "ether") >= 0.5 && object?.count >= 5
  );
};

const commit_to_db = async (object) => {
  const data = db`
    insert into etherAddress (address) values (${object?.address})
  `;
  try {
    await data.execute();
  } catch {
    data.cancel();
  }
};

const processAddress = async (address, blockNumber) => {
  try {
    const balance = await web3.eth.getBalance(address);
    const count = await web3.eth.getTransactionCount(address);
    const object = {
      address: address,
      balance: web3.utils.fromWei(balance, "ether"),
      count: count,
      blockNumber: blockNumber,
    };
    if (commit_pre_req(object)) await commit_to_db(object);
  } catch {}
};

const processTransaction = async (tx, blockNumber) => {
  try {
    const data = await web3.eth.getTransaction(tx);

    if (validate(data?.to)) {
      await processAddress(data?.to, blockNumber);
    }

    if (validate(data?.from)) {
      await processAddress(data?.from, blockNumber);
    }
  } catch {}
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
  let blockNumber = bnb_config.startBlock;
  while (true) {
    const data = await getBlock(blockNumber);
    const transactions = data?.transactions?.map(
      async (tx) => await processTransaction(tx, blockNumber)
    );
    console.log(
      `Transaction found in Block ${blockNumber}: --> `,
      data?.transactions?.length
    );
    Promise.all([...transactions]);
    blockNumber = blockNumberTrack(blockNumber, data?.number);
  }
};

await main();
