import { parentPort } from "worker_threads";
import { ProcessedCaseRecord } from "./CaseRecord.js";
import { BlobServiceClient } from '@azure/storage-blob';
import { getCaseFromPermanentJSON, MultiThreadProcessMessageCMD } from "./utils.js";

if (!parentPort) {
    throw new Error("Must be run in a thread context");
}

const azureStorageConnectionString = process.env.AZURE_STORAGE_CONNECTION_STRING;
const containerName = process.env.AZURE_STORAGE_CONTAINER;

if(!azureStorageConnectionString || !containerName) {
    throw new Error(`You must set AZURE_STORAGE_CONNECTION_STRING and AZURE_STORAGE_CONTAINER environment variables`)
}

// Create the BlobServiceClient object which will be used to create a container client
const blobServiceClient = BlobServiceClient.fromConnectionString(azureStorageConnectionString);
// Get a reference to a container
const containerClient = blobServiceClient.getContainerClient(containerName);

parentPort.on("message", (async (records: string[]) => {
    try {

        await Promise.all(records.map(async recordLink => {

            const record: ProcessedCaseRecord = getCaseFromPermanentJSON(recordLink);

            const blockBlobClient = containerClient.getBlockBlobClient(record.fileKey + ".json");
            const uploadData = JSON.stringify(record);
            try {
                await blockBlobClient.upload(uploadData, uploadData.length);
            } catch(ex) {
                throw ex
            }

        }));
    } catch (ex) {
        parentPort!.postMessage({
            cmdType: "error",
            data: ex
        });
    }

    parentPort!.postMessage({
        cmdType: "finish",
        data: records
    } satisfies MultiThreadProcessMessageCMD<string>);

}))
