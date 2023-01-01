import { parentPort, workerData } from "worker_threads";
import CaseRecord from "./CaseRecord.js"

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

const conversionEngine = 'pdfjs';
const parsersVersion = getVersion();

const allLegislation = workerData;

parentPort.on("message", (async (records: CaseRecord[]) => {

    const results = await Promise.all(records.map(async caseRecord => {

        let pages;
        let caseText;
        let footnoteContexts;
        let footnotes;
        let isValid;

        pages = await convertPDFURLWithPDFJS(caseRecord.PDFUrl);

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

        let obj: {
            [index: string]: any
        } = {
            isValid,
            caseText,
            caseCitations,
            caseLocation,
            representation,
            category,
            fileProvider,
            court,
            fileKey,
            filingNumber,
            lawReport,
            legislation,
            judges,
            conversionEngine,
            pdfChecksum: '',
            parsersVersion,
        };

        Object.keys(obj).forEach((key) => obj[key] === undefined && delete obj[key]);

        return obj;

    }))

    parentPort!.postMessage(results);

}));
