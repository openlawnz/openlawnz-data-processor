import { parentPort } from "worker_threads";
import CaseRecord from "./CaseRecord.js";
import { BlobServiceClient } from '@azure/storage-blob';

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

parentPort.on("message", (async (records: CaseRecord[]) => {
    try {

        await Promise.all(records.map(async record => {

            const blockBlobClient = containerClient.getBlockBlobClient(record.fileKey + ".json");
            const uploadData = JSON.stringify(record);
            try {
                await blockBlobClient.upload(uploadData, uploadData.length);
            } catch(ex: any) {
            }

        }));
    } catch (ex) {
        console.log("syncAzure fail");
    }

    parentPort!.postMessage(records);

}))
