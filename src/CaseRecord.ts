export default class CaseRecord {
    public constructor(
        public PDFUrl:string,
        public fileKey:string,
        public fileProvider:string,
        public caseCitations: string[],
        public caseDate:string,
        public caseNames:string[],
        public dateAccessed:string) {
    }
}
