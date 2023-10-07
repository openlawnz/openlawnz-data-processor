import { parentPort, workerData } from "worker_threads";
import CaseRecord, { ProcessedCaseRecord } from "./CaseRecord.js"

if (!parentPort) {
    throw new Error("Must be run in a thread context");
}

import {
    parseFromPDFJSConversion,
    convertPDFURLWithPDFJS,
    parseLocation,
    parseNeutralCitation,
    parseCourt,
    parseLawReport,
    parseCategory,
    parseCourtFilingNumber,
    parseRepresentation,
    parseLegislation,
    parseJudges,
    courts,
    lawReports,
    judgeTitles,
    getVersion,
    parseFromAzureOCRConversion
} from '@openlawnz/openlawnz-parsers';
import path from "path";
import { existsSync, readFileSync, writeFileSync } from "fs";
import { getS3Client, MultiThreadProcessMessageCMD } from "./utils.js";
import { S3 } from "@aws-sdk/client-s3";

let conversionEngine: string;
const parsersVersion = getVersion();

let S3Client: S3;

try {
    S3Client = getS3Client();
} catch {

}

// Suppress pdfjs warnings
console.log = function() {};

const { 
    localCasePath, 
    allLegislation, 
    OCRBucket, 
    reprocessOCR, 
    savePermanentJSONPath, 
    permamentOCR } = workerData;

parentPort.on("message", (async (records: CaseRecord[]) => {
    let results: string[] = [];
    try {
        results = await Promise.all(records.map(async (caseRecord): Promise<string> => {

            let caseText;
            let footnoteContexts;
            let footnotes;
            let isValid;

            const fileKey = caseRecord.fileKey;
            const filePath = path.join(localCasePath, caseRecord.fileKey);

            if (!existsSync(filePath)) {
                throw (`processCase: ${filePath} does not exist`);
            }

            if (caseRecord.fromOCR && !reprocessOCR) {
                conversionEngine = 'azure';
                try {
                    const ocrLocalPath = path.join(permamentOCR, fileKey);
                    let pages;

                    if (existsSync(ocrLocalPath)) {
                        pages = readFileSync(ocrLocalPath).toJSON();
                    } else {
                        const data = await S3Client.getObject({
                            Bucket: OCRBucket,
                            Key: fileKey
                        });
                        const jsonData = await data.Body?.transformToString() || "";
                        writeFileSync(ocrLocalPath, jsonData)
                        pages = JSON.parse(jsonData);
                    }

                    ({ caseText, footnoteContexts, footnotes, isValid } = parseFromAzureOCRConversion(pages));
                } catch (ex) {
                    throw (`processCase: ${filePath} OCR error`);
                }

            } else {
                conversionEngine = 'pdfjs'
                const pages = await convertPDFURLWithPDFJS("file://" + filePath);

                ({ caseText, footnoteContexts, footnotes, isValid } = parseFromPDFJSConversion(pages));
            }

            if (!caseText) {
                throw (`processCase: ${filePath} No case text (and processPDF didn't throw Error)`);
            }

            parentPort!.postMessage({
                cmdType: "log",
                data: `Process case: ${caseRecord.fileKey}`
            });
            
            const fileProvider = caseRecord.fileProvider;
            const caseLocation = parseLocation(caseText);
            const caseCitations = parseNeutralCitation({
                caseCitations: caseRecord.caseCitations,
                fileKey: caseRecord.fileKey,
                caseDate: caseRecord.caseDate,
                caseText: caseText,
            });
            const court = parseCourt({
                caseText: caseText,
                caseCitations,
                courts,
            });
            const lawReport = parseLawReport(lawReports, caseCitations);
            const category = parseCategory(fileProvider, court, lawReport);
            const filingNumber = parseCourtFilingNumber(caseText);
            const representation = parseRepresentation(caseText);
            const legislation = parseLegislation({
                allLegislation,
                caseText,
                footnoteContexts,
                footnotes,
                fileKey,
                isValid,
            });
            const judges = parseJudges({ judgeTitles, fileKey, caseText });
            const saveFilePath = path.join(savePermanentJSONPath, caseRecord.fileKey + ".json");

            writeFileSync(saveFilePath, JSON.stringify(new ProcessedCaseRecord(
                caseRecord.fileURL,
                caseRecord.fileKey,
                caseRecord.fileProvider,
                caseRecord.caseDate,
                caseRecord.caseNames,
                caseRecord.dateAccessed,
                isValid,
                caseText,
                caseCitations,
                caseLocation,
                representation,
                category,
                court,
                filingNumber,
                lawReport,
                legislation,
                judges,
                conversionEngine,
                '',
                parsersVersion

            ), null, 4))

            return saveFilePath;

        }))

    } catch (ex) {
        parentPort!.postMessage({
            cmdType: "error",
            data: ex
        });
    }

    parentPort!.postMessage({
        cmdType: "finish",
        data: results
    } satisfies MultiThreadProcessMessageCMD<string>);

}));
