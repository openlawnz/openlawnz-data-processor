import { parentPort, workerData } from "worker_threads";
import { existsSync, writeFileSync, readFileSync } from "fs";
import path from "path";
import CaseRecord from "./CaseRecord.js";
import { getS3Client, MultiThreadProcessMessageCMD } from "./utils.js";
import { S3 } from "@aws-sdk/client-s3";

if (!parentPort) {
    throw new Error("Must be run in a thread context");
}

const { localCasePath, S3CaseBucket } = workerData;

let S3Client: S3;

try {
    S3Client = getS3Client();
} catch {

}

parentPort.on("message", (async (records: CaseRecord[]) => {
    let results: CaseRecord[][] = [];
    try {

        results = await Promise.all(records.map(async record => {

            // TODO: Probably shouldn't be an array
            const unsynced = [];

            const fileLocation = path.join(localCasePath, record.fileKey);

            var existsInLocalCache = existsSync(fileLocation);

            // If there's no S3 client, only care about downloading locally
            if(!S3Client) {
                if(!existsInLocalCache) {
                    unsynced.push(record);
                }
                return unsynced;
            }

            // Do the sync between S3 and local cache automatically

            const S3Location = {
                Bucket: S3CaseBucket,
                Key: record.fileKey
            };
            
            var existsInS3Cache = true;
            try {
                await S3Client.headObject(S3Location);
            } catch (ex) {
                existsInS3Cache = false;
            }
            // If does not exist in local cache and does exist in S3 cache, download to local
            if (!existsInLocalCache && existsInS3Cache) {
                try {
                    const data = await S3Client.getObject(S3Location);
                    if (data.Body) {
                        const body = await data.Body.transformToByteArray();
                        writeFileSync(fileLocation, body);
                    }
                } catch (ex) {
                    throw("Fail writing locally");
                }
                // If it doees not exist in S3, and does exist in local cache, upload
            } else if (existsInLocalCache && !existsInS3Cache) {
                try {
                    var data = readFileSync(fileLocation);
                    await S3Client.putObject({
                        ...S3Location,
                        Body: data
                    })
                } catch (ex) {
                    throw ("Fail uploading to S3");
                }
            } else if (!existsInLocalCache && !existsInS3Cache) {
                unsynced.push(record);
            }

            return unsynced;

        }));
    } catch (ex) {
        parentPort!.postMessage({
            cmdType: "error",
            data: ex
        });
    }

    parentPort!.postMessage({
        cmdType: "finish",
        data: results
    } satisfies MultiThreadProcessMessageCMD<CaseRecord[]>);

}))
