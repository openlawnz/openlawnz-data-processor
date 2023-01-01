import { Worker } from "worker_threads";
import crypto from 'crypto';

export function chunkArrayInGroups(arr: Array<any>, size: number) {
    var myArray = [];
    for (var i = 0; i < arr.length; i += size) {
        myArray.push(arr.slice(i, i + size));
    }
    return myArray;
}
export async function multithreadProcess<T, U>(threads: number, records: Array<T>, workerScript: string, globalWorkerData?: object): Promise<Array<U>> {

    return new Promise((resolve, reject) => {
        let allResults: Array<U> = [];
        var tasks: {
            id: string,
            worker: Worker,
            isProcessing: boolean
        }[] = [];
        const recordChunks = chunkArrayInGroups(records, threads);
        const totalWorkers = Math.min(threads, recordChunks.length);
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

                task = {
                    id: crypto.randomUUID(),
                    worker,
                    isProcessing: false
                }
                tasks.push(task);
            })();
        }
        function run() {
            var freeTasksFilter = tasks.filter(x => x.isProcessing == false);
            if (recordChunks.length > 0 && freeTasksFilter.length > 0) {
                var records = recordChunks.shift();
                freeTasksFilter[0].isProcessing = true
                freeTasksFilter[0].worker.postMessage(records)
            }
            else if (recordChunks.length == 0 && freeTasksFilter.length == totalWorkers) {
                resolve(allResults);
                for(var i = 0; i < freeTasksFilter.length; i++) {
                    freeTasksFilter[i].worker.terminate();
                }
            }
            setImmediate(run);
        }
        setImmediate(run)
    });
}