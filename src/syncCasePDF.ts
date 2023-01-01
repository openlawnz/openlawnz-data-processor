import { parentPort, workerData } from "worker_threads";
import { existsSync, writeFileSync, readFileSync } from "fs";
import path from "path";
import { S3 } from "@aws-sdk/client-s3";
import CaseRecord from "./CaseRecord.js";

if (!parentPort) {
    throw new Error("Must be run in a thread context");
}

const { localCasePath, S3CaseBucket } = workerData;

let client: S3;

if (process.env.AWS_ACCESS_KEY_ID && process.env.AWS_SECRET_ACCESS_KEY) {
    client = new S3({
        region: "ap-southeast-2",
        credentials: {
            accessKeyId: process.env.AWS_ACCESS_KEY_ID,
            secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
        }
    });
} else {
    // TODO: Handle so that it works on just local
    throw Error("No AWS credentials set")
}

parentPort.on("message", (async (records: CaseRecord[]) => {

    const results = await Promise.all(records.map(async record => {

        const unsynced = [];

        const fileLocation = path.join(localCasePath, record.fileKey);
        const S3Location = {
            Bucket: S3CaseBucket,
            Key: record.fileKey
        };

        // Do the sync between S3 and local cache automatically

        var existsInLocalCache = existsSync(fileLocation);
        var existsInS3Cache = true;
        try {
            await client.headObject(S3Location);
        } catch (ex) {
            existsInS3Cache = false;
        }
        // If does not exist in local cache and does exist in S3 cache, download to local
        if (!existsInLocalCache && existsInS3Cache) {
            const data = await client.getObject(S3Location);
            if (data.Body) {
                const body = await data.Body.transformToString();
                writeFileSync(fileLocation, body);
            }
            // If it doees not exist in S3, and does exist in local cache, upload
        } else if (existsInLocalCache && !existsInS3Cache) {
            var data = readFileSync(fileLocation);
            await client.putObject({
                ...S3Location,
                Body: data
            })
        } else if(!existsInLocalCache && !existsInS3Cache) {
            unsynced.push(record);
        }

        return unsynced;

    }));

    parentPort!.postMessage(results);
    
}))
