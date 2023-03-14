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
import { existsSync } from "fs";
import { getS3Client } from "./utils.js";
import { S3 } from "@aws-sdk/client-s3";

let conversionEngine: string;
const parsersVersion = getVersion();

let S3Client: S3;

try {
    S3Client = getS3Client();
} catch {

}


const { localCasePath, allLegislation, OCRBucket, reprocessOCR } = workerData;

parentPort.on("message", (async (records: CaseRecord[]) => {
    let results: ProcessedCaseRecord[] = [];
    try {
        results = await Promise.all(records.map(async (caseRecord): Promise<ProcessedCaseRecord> => {

            let caseText;
            let footnoteContexts;
            let footnotes;
            let isValid;

            const fileKey = caseRecord.fileKey;
            const filePath = path.join(localCasePath, caseRecord.fileKey);

            if (!existsSync(filePath)) {
                console.log(`[!] processCase: ${filePath} does not exist`)
                throw ("Error"); // TODO: handle better
            }

            if (caseRecord.fromOCR && !reprocessOCR) {
                conversionEngine = 'azure';
                try {
                    const data = await S3Client.getObject({
                        Bucket: OCRBucket,
                        Key: fileKey
                    });
                    const pages = JSON.parse(await data.Body?.transformToString() || "");

                    ({ caseText, footnoteContexts, footnotes, isValid } = parseFromAzureOCRConversion(pages));
                } catch (ex) {
                    throw new Error();
                }



            } else {
                conversionEngine = 'pdfjs'
                const pages = await convertPDFURLWithPDFJS("file://" + filePath);

                ({ caseText, footnoteContexts, footnotes, isValid } = parseFromPDFJSConversion(pages));
            }

            if (!caseText) {
                console.log(`[!] processCase: ${filePath} No case text (and processPDF didn't throw Error)`);
                throw ("Error"); // TODO: handle better
            }


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

            return new ProcessedCaseRecord(
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

            );

        }))

    } catch (ex) {
        console.log("processCase fail");
    }

    parentPort!.postMessage(results);

}));
