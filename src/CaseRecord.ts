import { CaseCitation } from "@openlawnz/openlawnz-parsers/dist/types/CaseCitation.js"
import { Category } from "@openlawnz/openlawnz-parsers/dist/types/Category.js"
import { Court } from "@openlawnz/openlawnz-parsers/dist/types/Court.js"
import { Judge } from "@openlawnz/openlawnz-parsers/dist/types/Judge.js"
import { LawReport } from "@openlawnz/openlawnz-parsers/dist/types/LawReport.js"
import { Legislation } from "@openlawnz/openlawnz-parsers/dist/types/Legislation.js"
import { Representation } from "@openlawnz/openlawnz-parsers/dist/types/Representation.js"

export default class CaseRecord {
    public constructor(
        public fileURL: string,
        public fileKey: string,
        public fileProvider: string,
        public caseCitations: string[],
        public caseDate: string,
        public caseNames: string[],
        public dateAccessed: Date) {
    }
}

export class ProcessedCaseRecord {
    public constructor(
        public fileURL: string,
        public fileKey: string,
        public fileProvider: string,
        public caseDate: string,
        public caseNames: string[],
        public dateAccessed: Date,
        public isValid: boolean,
        public caseText: string,
        public caseCitations: CaseCitation[],
        public caseLocation: string,
        public representation: Representation,
        public category: Category | null,
        public court: Court,
        public filingNumber: string,
        public lawReport: LawReport,
        public legislation: Legislation,
        public judges: Judge[],
        public conversionEngine: string,
        public pdfChecksum: string,
        public parsersVersion: string) {
           
        }
}