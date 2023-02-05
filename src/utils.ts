import { Worker } from "worker_threads";
import { setTimeout as setTimeoutP } from "timers/promises";
import crypto from 'crypto';
import { S3 } from "@aws-sdk/client-s3";

export function chunkArrayInGroups(arr: Array<any>, size: number) {
    var myArray = [];
    for (var i = 0; i < arr.length; i += size) {
        myArray.push(arr.slice(i, i + size));
    }
    return myArray;
}
export async function multithreadProcess<T, U>(threads: number, records: Array<T>, workerScript: string, globalWorkerData?: object): Promise<Array<U>> {

    return new Promise(async (resolve, reject) => {
        let runner: NodeJS.Timeout;
        let allResults: Array<U> = [];
        var tasks: {
            id: string,
            worker: Worker,
            isProcessing: boolean
        }[] = [];
        const recordChunks = chunkArrayInGroups(records, threads);
        let totalWorkers = Math.min(threads, recordChunks.length);
        for (var i = 0; i < totalWorkers; i++) {
            (() => {
                var task: {
                    id: string,
                    worker: Worker,
                    isProcessing: boolean
                };
                const worker = new Worker(workerScript, { workerData: globalWorkerData });
                worker.on('message', (results: Array<U>) => {
                    allResults = allResults.concat(results);
                    task.isProcessing = false;
                });
                worker.on('error', (err: U) => {
                    task.isProcessing = false;
                });
                worker.on('exit', () => {
                    // If exit for some reason. TODO: Investigate
                    tasks.splice(tasks.findIndex(t => t.id == task.id), 1);
                    totalWorkers--;
                    console.log("- Worker exited")
                });

                task = {
                    id: crypto.randomUUID(),
                    worker,
                    isProcessing: false
                }
                console.log("+ Worker created")
                tasks.push(task);
            })();
        }
        async function run() {
            var freeTasksFilter = tasks.filter(x => x.isProcessing == false);
            if (recordChunks.length == 0 && freeTasksFilter.length == totalWorkers) {
                console.log('Workers finished')
                for (var i = 0; i < freeTasksFilter.length; i++) {
                    await freeTasksFilter[i].worker.terminate();
                }
                resolve(allResults);
                clearTimeout(runner);
                return;

            } else if (recordChunks.length > 0 && freeTasksFilter.length > 0) {
                for (let i = 0; i < freeTasksFilter.length; i++) {
                    console.log("--------------------------------")
                    console.log("freeTasksFilter", freeTasksFilter.length);
                    console.log("recordChunks.length", recordChunks.length);
                    console.log("totalWorkers", totalWorkers);
                    console.log("--------------------------------")
                    var records = recordChunks.shift();
                    if (records) {
                        console.log('Send records to worker id:' + freeTasksFilter[i].id)
                        freeTasksFilter[i].isProcessing = true
                        freeTasksFilter[i].worker.postMessage(records)
                    }
                }


            }
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