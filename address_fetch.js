import Web3 from "web3";
import postgres from "postgres";
import { db_config, rpc_config } from "./config.js";

const db = postgres(db_config);

const web3 = new Web3(new Web3.providers.HttpProvider(rpc_config.rpc_url));

const delay = async () => new Promise((resolve) => setTimeout(resolve, 1000));
const blockNumberTrack = (pre, curr) => {
  return typeof pre === "string" ? curr + 1 : pre + 1;
};

const commit_to_db = async (address) => {
  const data = db`
    insert into etherAddress (address) values (${address})
  `;
  try {
    await data.execute();
  } catch {
    data.cancel();
  }
};

const processAddress = async (address) => {
  await commit_to_db(address);
};

const processTransaction = async (tx, blockNumber) => {
  try {
    console.log("TX --> : ", tx, " Block Number: -->: ", blockNumber);
    const data = await web3.eth.getTransaction(tx);

    if (data?.to && web3.utils.checkAddressChecksum(data?.to)) {
      await processAddress(data.to);
    }

    if (data?.from && web3.utils.checkAddressChecksum(data?.from)) {
      await processAddress(data?.from);
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
  let blockNumber = rpc_config.startBlock;
  while (true) {
    const data = await getBlock(blockNumber);
    const transactions = data?.transactions?.map(
      async (tx) => await processTransaction(tx, blockNumber)
    );
    Promise.all([...transactions]);
    blockNumber = blockNumberTrack(blockNumber, data?.number);
  }
};

await main();
