import { Worker } from "worker_threads";
import { setTimeout as setTimeoutP } from "timers/promises";
import crypto from 'crypto';
import { S3 } from "@aws-sdk/client-s3";
import { appendFileSync, readFileSync } from "fs";
import { createFileSync } from "fs-extra";
import { ProcessedCaseRecord } from "./CaseRecord.js";

export function chunkArrayInGroups<T>(arr: Array<T>, size: number) {
    var myArray = [];
    for (var i = 0; i < arr.length; i += size) {
        myArray.push(arr.slice(i, i + size));
    }
    return myArray;
}

export type MultiThreadProcessMessageCMD<U> = { cmdType: "finish", data: Array<U> } | { cmdType: "log", data: any } | { cmdType: "error", data: any }

export async function multithreadProcess<T, U>(
    logger: FileLogger,
    threads: number,
    records: Array<T>,
    workerScript: string,
    globalWorkerData?: object,
    initCallback: (totalChunks: number) => void = () => { },
    updateCallback: (totalProcessed: number) => void = () => { }): Promise<Array<U>> {

    return new Promise(async (resolve, reject) => {
        let runner: NodeJS.Timeout;
        let allResults: Array<U> = [];
        var tasks: {
            id: string,
            worker: Worker,
            isProcessing: boolean
        }[] = [];
        const recordChunks = chunkArrayInGroups(records, threads);
        const totalRecords = records.length;
        initCallback(totalRecords);
        let totalWorkers = Math.min(threads, recordChunks.length);
        for (var i = 0; i < totalWorkers; i++) {
            (() => {
                var task: {
                    id: string,
                    worker: Worker,
                    isProcessing: boolean
                };
                const worker = new Worker(workerScript, { workerData: globalWorkerData });
                worker.on('message', (cmd: MultiThreadProcessMessageCMD<U>) => {

                    switch (cmd.cmdType) {
                        case "log":
                            logger.log(cmd.data, false) // Don't log to console because it will be noisy
                            break;
                        case "error":
                            logger.error(cmd.data, false) // Don't log to console because it will be noisy
                            break;
                        case "finish":
                            allResults = allResults.concat(cmd.data);
                            task.isProcessing = false;
                            break;

                    }


                });
                worker.on('error', (err: any) => {
                    logger.error("MAJOR WORKER ERROR")
                    logger.error(err);
                    task.isProcessing = false;
                });
                worker.on('exit', () => {
                    tasks.splice(tasks.findIndex(t => t.id == task.id), 1);
                    totalWorkers--;
                    logger.log(`Worker exit - total workers: ${totalWorkers}`, false);
                });

                task = {
                    id: crypto.randomUUID(),
                    worker,
                    isProcessing: false
                }
                tasks.push(task);
            })();
        }
        async function run() {
            var freeTasksFilter = tasks.filter(x => x.isProcessing == false);
            if (recordChunks.length == 0 && freeTasksFilter.length == totalWorkers) {
                for (var i = 0; i < freeTasksFilter.length; i++) {
                    await freeTasksFilter[i].worker.terminate();
                }
                resolve(allResults);
                clearTimeout(runner);
                updateCallback(100)
                return;

            } else if (recordChunks.length > 0 && freeTasksFilter.length > 0) {
                for (let i = 0; i < freeTasksFilter.length; i++) {
                    var records = recordChunks.shift();
                    if (records) {
                        freeTasksFilter[i].isProcessing = true
                        freeTasksFilter[i].worker.postMessage(records)
                    }
                }
            }
            updateCallback(totalRecords - (recordChunks.length * threads));
            runner = await setTimeoutP(100);
            await run();

        }
        await run();
    });
}

export function getCitation(str: string) {
    const regCite = /(\[?\d{4}\]?)(\s*?)NZ(D|F|H|C|S|L)(A|C|R)(\s.*?)(\d+)*/;
    // try for neutral citation
    const match = str.match(regCite);
    if (match && match.length > 0) {
        return match[0];
    } else {
        // try for other types of citation
        const otherCite = /((\[\d{4}\])(\s*)NZ(D|F|H|C|S|L)(A|C|R)(\s.*?)(\d+))|((HC|DC|FC) (\w{2,4} (\w{3,4}).*)(?=\s\d{1,2} ))|(COA)(\s.{5,10}\/\d{4})|(SC\s\d{0,5}\/\d{0,4})/;
        const otherMatch = str.match(otherCite);
        if (otherMatch && otherMatch.length > 0) {
            return otherMatch[0];
        } else {
            return null;
        }
    }
};

export function getS3Client(): S3 {

    if (process.env.AWS_ACCESS_KEY_ID && process.env.AWS_SECRET_ACCESS_KEY) {
        return new S3({
            region: "ap-southeast-2",
            credentials: {
                accessKeyId: process.env.AWS_ACCESS_KEY_ID,
                secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
            }
        });
    } else {
        throw Error("No AWS credentials set")
    }
}

export const getCaseFromPermanentJSON = (filePath: string): ProcessedCaseRecord => {
    return JSON.parse(readFileSync(filePath).toString());
}

export class FileLogger {
    private runPath: string;
    constructor(runPath: string) {
        this.runPath = `${runPath}.txt`;
        createFileSync(this.runPath);
    }
    private _log(message: string | object, consoleLog: boolean = true) {
        let fileLogMessage;
        if (typeof message === "object") {
            fileLogMessage = JSON.stringify(message, null, 4);
        } else {
            fileLogMessage = message;
        }
        appendFileSync(this.runPath, fileLogMessage + "\n");
        if (consoleLog) {
            console.log(message);
        }
    }
    public log(message: string | object, consoleLog: boolean = true) {
        this._log(`[LOG] ` + message, consoleLog)
    }
    public error(message: string | object, consoleLog: boolean = true) {
        this._log(`[ERROR] ` + message, consoleLog)
    }
}