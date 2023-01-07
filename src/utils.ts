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
                worker.on('exit', () => {
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
        if (otherMatch  && otherMatch.length > 0) {
			return otherMatch[0];
		} else {
			return null;
		}
	}
};