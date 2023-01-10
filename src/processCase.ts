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
    getVersion
} from '@openlawnz/openlawnz-parsers';
import path from "path";

const conversionEngine = 'pdfjs';
const parsersVersion = getVersion();

const { localCasePath, allLegislation } = workerData;

parentPort.on("message", (async (records: CaseRecord[]) => {

    const results = await Promise.all(records.map(async (caseRecord): Promise<ProcessedCaseRecord> => {

        let pages;
        let caseText;
        let footnoteContexts;
        let footnotes;
        let isValid;

        const filePath = "file://" + path.join(localCasePath, caseRecord.fileKey);
        
        pages = await convertPDFURLWithPDFJS(filePath);

        ({ caseText, footnoteContexts, footnotes, isValid } = parseFromPDFJSConversion(pages));

        if (!caseText) {
            throw new Error("No case text (and processPDF didn't throw Error)");
        }

        const fileKey = caseRecord.fileKey;
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

    parentPort!.postMessage(results);

}));
