import { readFileSync, writeFileSync } from "fs";
import path from "path";
import { setTimeout as setTimeoutP } from "timers/promises";
import { multithreadProcess } from "./utils.js";
import CaseRecord from "./CaseRecord.js"

const MAX_THREADS = 4;

const allLegislation = JSON.parse(readFileSync("./legislation.json").toString());

var sampleCaseRecords: CaseRecord[] = []

if(!process.env.LOCAL_CASE_PATH || !process.env.S3_CASE_BUCKET) {
  throw Error("Missing LOCAL_CASE_PATH or S3_CASE_BUCKET env vars");
}
var localCasePath = process.env.LOCAL_CASE_PATH;
var S3CaseBucket = process.env.S3_CASE_BUCKET;

async function syncCasePDFs(caseRecords: CaseRecord[]): Promise<CaseRecord[]> {
  const caseRecordsNotInCaches: CaseRecord[] = await multithreadProcess(MAX_THREADS, caseRecords, './syncCasePDF.js', {
    localCasePath,
    S3CaseBucket
  });
  return caseRecordsNotInCaches.flat();
}

async function sequentiallyDownloadFilesWithDelays(caseRecords: CaseRecord[]) {
  for (var i = 0; i < caseRecords.length; i++) {
    const caseRecord = caseRecords[i];
    const res = await fetch(caseRecord.PDFUrl);
    if (res.body) {
      var data = await res.text();
      writeFileSync(path.join(localCasePath, caseRecord.fileKey), data)
    }
    await setTimeoutP(5000)
  }
}

async function processCases(caseRecords: CaseRecord[]) {
  return multithreadProcess(MAX_THREADS, caseRecords, './processCase.js', allLegislation);
}

var caseRecordsNotInCaches = await syncCasePDFs(sampleCaseRecords);

if(caseRecordsNotInCaches.length > 0) {
  // TODO: prompt
  await sequentiallyDownloadFilesWithDelays(caseRecordsNotInCaches)
}

const processedCases = await processCases(sampleCaseRecords);

process.exit();