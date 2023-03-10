import os from 'os';
import { writeFileSync } from "fs";
import path from "path";
import { setTimeout as setTimeoutP } from "timers/promises";
import { chunkArrayInGroups, getCitation, multithreadProcess } from "./utils.js";
import yargs from 'yargs'
import { hideBin } from "yargs/helpers";
import pkg from 'pg';
import crypto from 'crypto';
import inquirer from 'inquirer';
import { courts, lawReports, judgeTitles, parseCaseCitations, parseCaseToCase } from "@openlawnz/openlawnz-parsers";
import { CaseCitation } from "@openlawnz/openlawnz-parsers/dist/types/CaseCitation.js"
import CaseRecord, { ProcessedCaseRecord } from "./CaseRecord.js"

const argv = await yargs(hideBin(process.argv)).argv
const { Pool } = pkg;

const MAX_THREADS = os.cpus().length * 2;

console.log("MAX_THREADS", MAX_THREADS);

const pool = new Pool({
	user: 'postgres',
	host: 'db',
	database: 'dev',
	password: 'postgres',
	port: 5432
})

//---------------------------------------
// Legislation
//---------------------------------------
if (argv.importLegislation) {

	const allLegislation = await (await fetch("https://openlawnz-legislation.s3.ap-southeast-2.amazonaws.com/legislation/legislation.json")).json();

	for (let a in allLegislation) {
		const legislationItem = allLegislation[a];
		legislationItem.id = crypto.randomUUID();

		try {
			await pool.query(`
				INSERT INTO main.legislation (
						id,
						title,
						link,
						year,
						alerts)
				VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`, [
				legislationItem.id,
				legislationItem.title,
				legislationItem.link,
				legislationItem.year,
				legislationItem.alerts
			])
		}
		catch (ex) {
			console.log("Error writing legislation to db: " + ex);
		}

	}

	process.exit();

}

//---------------------------------------
// Static
// - Courts
// - Law Reports
// - Judge Titles
//---------------------------------------
if (argv.importStatic) {

	await Promise.all([
		(async () => {
			await pool.query(`TRUNCATE TABLE main.courts RESTART IDENTITY CASCADE;`)

			// Insert rows
			for (let a in courts) {
				const courtsItem = courts[a];

				try {
					await pool.query(`
					INSERT INTO main.courts (
						id,
						acronyms,
						name)
					VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, [
						courtsItem.id,
						courtsItem.acronyms,
						courtsItem.name,
					])
				}
				catch (ex) {
					console.log("Error writing courts to db: " + ex);
				}
			}

		})(),
		(async () => {
			await pool.query(`TRUNCATE TABLE main.law_reports RESTART IDENTITY CASCADE;`)

			for (let a in lawReports) {
				const lawReportItem = lawReports[a];

				try {
					await pool.query(`
					INSERT INTO main.law_reports (
						id,
						acronym,
						name)
					VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, [
						lawReportItem.id,
						lawReportItem.acronym,
						lawReportItem.name,
					])
				}
				catch (ex) {
					console.log("Error writing law report to db: " + ex);
				}
			}
		})(),
		(async () => {
			await pool.query(`TRUNCATE TABLE main.judge_titles RESTART IDENTITY CASCADE;`)

			type judgeTitle = {
				[key: string]: {
					short_title: string,
					long_titles: string[]
				}
			}

			for (let a in judgeTitles) {
				const judge_title = (judgeTitles as any)[a] as judgeTitle;
				await pool.query(`
				INSERT INTO main.judge_titles (id, short_title, long_titles)
				VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, [
					a,
					judge_title.short_title,
					judge_title.long_titles,
				]);
			}
		})()
	])

	process.exit();

}

//---------------------------------------
// Cases
//---------------------------------------
if (argv.importCases) {
	// Get from provider
	if (argv.casesProvider) {

		let recordsToProcess: CaseRecord[] = []

		if (argv.casesProvider == "jdo") {

			console.log("Get cases from JDO");

			const data = await fetch("https://forms.justice.govt.nz/solr/jdo/select?q=*&facet=true&facet.field=Location&facet.field=Jurisdiction&facet.limit=-1&facet.mincount=1&rows=200&json.nl=map&fq=JudgmentDate%3A%5B*%20TO%202019-2-1T23%3A59%3A59Z%5D&sort=JudgmentDate%20desc&fl=CaseName%2C%20JudgmentDate%2C%20DocumentName%2C%20id%2C%20score&wt=json")
			const json = await data.json();
			const docs = json.response.docs;
			recordsToProcess = docs.map((doc: any) => {

				const fileKey = `jdo_` + (+new Date(doc.JudgmentDate)) + "_" + doc.DocumentName;
				const neutralCitation = getCitation(doc.CaseName);

				return new CaseRecord(
					"https://openlawnz-pdfs-prod.s3-ap-southeast-2.amazonaws.com/" + fileKey,
					fileKey,
					"jdo",
					neutralCitation ? [neutralCitation] : [],
					doc.JudgmentDate,
					[doc.CaseName],
					new Date()
				)

			});

		}

		if (recordsToProcess.length > 0) {

			console.log(`recordsToProcess ${recordsToProcess.length}`)

			const allLegislation = (await pool.query("SELECT * FROM main.legislation")).rows;

			if (!process.env.LOCAL_CASE_PATH || !process.env.S3_CASE_BUCKET) {
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

			async function sequentiallyDownloadFilesWithDelays(caseRecords: CaseRecord[]): Promise<{ casesToExclude: CaseRecord[] }> {
				console.log(`sequentiallyDownloadFilesWithDelays ${caseRecords.length}`)
				var casesToExclude: CaseRecord[] = [];
				for (var i = 0; i < caseRecords.length; i++) {
					const caseRecord = caseRecords[i];
					try {
						const res = await fetch(caseRecord.fileURL, {

						});
						if (res.status == 200 && res.body) {
							var data = await res.arrayBuffer()
							const filePath = path.join(localCasePath, caseRecord.fileKey);
							writeFileSync(filePath, Buffer.from(data), 'binary')
							
						} else {
							casesToExclude.push(caseRecord);
							console.log(`Cannot download file ${caseRecord.fileURL}`);
						}
					} finally {
						if (!caseRecord.fileURL.startsWith("https://openlawnz-pdfs-prod.s3-ap-southeast-2.amazonaws.com")) {
							await setTimeoutP(5000)
						}
					}
				}

				await syncCasePDFs(caseRecords.filter(x => {
					return !casesToExclude.find(y => y.fileKey == x.fileKey)
				}))
				return {
					casesToExclude
				};
			}

			async function processCases(caseRecords: CaseRecord[]) {
				return multithreadProcess(MAX_THREADS, caseRecords, './processCase.js', { localCasePath, allLegislation });
			}

			console.log(`syncCasePDFs: ${recordsToProcess.length}`)
			var caseRecordsNotInCaches = await syncCasePDFs(recordsToProcess);

			console.log("caseRecordsNotInCaches: " + caseRecordsNotInCaches.length);

			if (caseRecordsNotInCaches.length > 0) {

				let answer = false;

				if (!argv.dangerouslySkipConfirmDownloadPDFsInOrderToHaveDebuggerWorkInVSCode) {

					const response = await inquirer.prompt([
						{
							name: "answer",
							type: "confirm",
							message: `There are ${caseRecordsNotInCaches.length} cases to download from source. Continue?`,
							default: false
						}

					]);

					answer = response["answer"]

				} else {
					answer = true;
				}


				if (answer) {

					const { casesToExclude } = await sequentiallyDownloadFilesWithDelays(caseRecordsNotInCaches);

					recordsToProcess = recordsToProcess.filter(x => {
						return !casesToExclude.find(y => y.fileKey == x.fileKey)
					})

				} else {
					process.exit();
				}
			}

			console.log(`Process ${recordsToProcess.length} cases`);

			const startProcessedCases = +new Date();

			const processedCases = await processCases(recordsToProcess);

			const endProcessedCases = +new Date();

			const timeToProcessCases = ((endProcessedCases - startProcessedCases) / 60000).toFixed(2);

			const chunkedProcessedCases: Array<ProcessedCaseRecord[]> = chunkArrayInGroups(processedCases, 10);

			console.log("Truncating previous dataset")
			//====================================================================
			// TRUNCATE everything since we reprocess the whole dataset
			//====================================================================

			await pool.query(`
				TRUNCATE TABLE 
					main.case_citations, 
					main.case_pdfs, 
					main.cases, 
					main.cases_cited, 
					main.category_to_cases,
					main.judge_to_cases,
					main.legislation_to_cases,
					main.party_and_representative_to_cases
				RESTART IDENTITY CASCADE
			`);

			//====================================================================
			// Put case and related info into DB
			//====================================================================

			console.log("Put in DB");

			for (var i = 0; i < chunkedProcessedCases.length; i++) {

				const cases = chunkedProcessedCases[i]

				await Promise.all(cases.map(caseRecord => (async () => {

					const casePDFsValues = [
						caseRecord.fileKey,
						caseRecord.caseDate,
						caseRecord.fileProvider,
						caseRecord.fileKey,
						caseRecord.fileURL,
						caseRecord.pdfChecksum,
						caseRecord.parsersVersion
					];

					const caseValues = [
						caseRecord.fileKey,
						caseRecord.lawReport ? (caseRecord.lawReport as any).id : null, // TODO: Update parsers to add id to type
						caseRecord.court ? caseRecord.court.id : null,
						caseRecord.fileKey,
						caseRecord.caseDate,
						caseRecord.caseText,
						caseRecord.caseNames[0],
						caseRecord.isValid,
						caseRecord.caseLocation,
						caseRecord.conversionEngine,
						caseRecord.filingNumber,
						caseRecord.parsersVersion
					]

					const existsResult = await pool.query(`SELECT COUNT(*) FROM main.cases WHERE id = $1`, [caseRecord.fileKey]);

					if (existsResult.rows[0].count == 0) {

						try {
							await pool.query(`
								INSERT INTO main.case_pdfs (
									pdf_id,
									fetch_date,
									pdf_provider,
									pdf_db_key,
									pdf_url,
									pdf_checksum,
									parsers_version
									)
								VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING`, casePDFsValues);
						}
						catch (ex) {
							console.error("Error writing case pdf records " + ex);
							console.error(caseRecord);
							return;
						}

						try {
							await pool.query(`
								INSERT INTO main.cases (
									id,
									lawreport_id,
									court_id,
									pdf_id,
									case_date,
									case_text,
									case_name,
									is_valid,
									location,
									conversion_engine,
									court_filing_number,
									parsers_version)
								VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) ON CONFLICT DO NOTHING`, caseValues);
						}
						catch (ex) {
							console.error("Error writing case record " + ex);
							console.error(caseRecord);
							return;
						}

					}
					else {

						try {
							await pool.query(`
								UPDATE main.case_pdfs
								SET
									fetch_date = $2,
									pdf_provider = $3,
									pdf_db_key = $4,
									pdf_url = $5,
									pdf_checksum = $6,
									parsers_version = $7
								WHERE pdf_id = $1`, casePDFsValues);
						}
						catch (ex) {
							console.error("Error writing case pdf records update " + ex);
							console.error(caseRecord);
							return;
						}

						try {
							await pool.query(`
								UPDATE main.cases
								SET
									lawreport_id = $2,
									court_id = $3,
									pdf_id = $4,
									case_date = $5,
									case_text = $6,
									case_name = $7,
									is_valid = $8,
									location = $9,
									conversion_engine = $10,
									court_filing_number = $11,
									parsers_version = $12
								WHERE id = $1`, caseValues);
						}
						catch (ex) {
							console.error("Error writing case record update " + ex);
							console.error(caseRecord);
							return;
						}


					}

					//---------------------------------------------------------------------
					// Representation
					//---------------------------------------------------------------------

					try {

						// Delete relationships
						// await pool.query(`DELETE FROM main.party_and_representative_to_cases WHERE case_id = $1`, [caseRecord.fileKey]);

						const representationRecord = await pool.query(`
						INSERT INTO main.party_and_representative_to_cases (case_id, names, party_type, appearance, parsers_version)
						VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`, [
							caseRecord.fileKey,
							caseRecord.representation.initiation.names,
							caseRecord.representation.initiation.party_type,
							caseRecord.representation.initiation.appearance,
							caseRecord.parsersVersion
						]);

						if ((representationRecord as any).response) { // TODO: Check this

							await pool.query(`
						INSERT INTO main.party_and_representative_to_cases (case_id, names, party_type, appearance, parsers_version)
						VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`, [
								caseRecord.fileKey,
								caseRecord.representation.response.names,
								caseRecord.representation.response.party_type,
								caseRecord.representation.response.appearance,
								caseRecord.parsersVersion
							]);
						}

					}
					catch (ex) {
						console.error("Error writing representation to db" + ex)
						console.error(caseRecord.representation)
					}

					//---------------------------------------------------------------------
					// Judges to Cases
					//---------------------------------------------------------------------

					try {
						// Delete relationships
						// await pool.query(`DELETE FROM main.judge_to_cases WHERE case_id = $1`, [caseRecord.fileKey]);

						for (let jindex = 0; jindex < caseRecord.judges.length; jindex++) {

							const judge = caseRecord.judges[jindex];
							// Write to DB

							await pool.query(`
						INSERT INTO main.judge_to_cases (title_id, name, case_id, parsers_version)
						VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`, [
								judge.title_id,
								judge.name,
								caseRecord.fileKey,
								caseRecord.parsersVersion
							]);


						}

					}
					catch (ex) {
						console.error("Error writing judges to cases to db" + ex)
					}

					//---------------------------------------------------------------------
					// Category
					//---------------------------------------------------------------------

					try {

						if (caseRecord.category) {

							// Delete relationships
							// await pool.query(`DELETE FROM main.category_to_cases WHERE case_id = $1`, [caseRecord.fileKey]);

							await pool.query(`
							INSERT INTO main.categories (id, category)
							VALUES ($1, $2) ON CONFLICT DO NOTHING`, [
								caseRecord.category.id,
								caseRecord.category.name,
							]);

							await pool.query(`
							INSERT INTO main.category_to_cases (case_id, category_id)
							VALUES ($1, $2) ON CONFLICT DO NOTHING`, [
								caseRecord.fileKey,
								caseRecord.category.id
							]);

						}
					}
					catch (ex) {
						console.error("Error writing categories to db" + ex)
					}

					//---------------------------------------------------------------------
					// Legislation References
					//---------------------------------------------------------------------

					try {

						// Delete relationships
						// await pool.query(`DELETE FROM main.legislation_to_cases WHERE case_id = $1`, [caseRecord.fileKey]);

						// Loop

						if (caseRecord.legislation) {

							const lrefs = caseRecord.legislation.legislationReferences;

							for (let k = 0; k < lrefs.length; k++) {

								const legislationReference = lrefs[k];

								const groupedSections = Object.values(legislationReference.groupedSections);

								for (let groupedSectionKey in groupedSections) {

									const groupedSection = groupedSections[groupedSectionKey];

									await pool.query(`
								INSERT INTO main.legislation_to_cases (legislation_id, extraction_confidence, section, case_id, count, parsers_version)
								VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING`, [
										legislationReference.legislationId,
										caseRecord.legislation.extractionConfidence,
										groupedSection.id,
										caseRecord.fileKey,
										groupedSection.count,
										caseRecord.parsersVersion
									]);

								}

							}

						}

					}
					catch (ex) {
						console.error("Error writing legislation to db" + ex);
						console.error(JSON.stringify(caseRecord, null, 4));
					}

					//---------------------------------------------------------------------
					// Citations
					//---------------------------------------------------------------------

					try {

						// Delete relationships
						// await pool.query(`DELETE FROM main.case_citations WHERE case_id = $1`, [caseRecord.fileKey]);

						// Loop

						if (caseRecord.caseCitations) {

							for (let cindex = 0; cindex < caseRecord.caseCitations.length; cindex++) {

								const citationRecord = caseRecord.caseCitations[cindex];

								await pool.query(`
								INSERT INTO main.case_citations (case_id, citation, id, year, parsers_version)
								VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`, [
									citationRecord.fileKey,
									citationRecord.citation,
									citationRecord.id,
									citationRecord.year,
									caseRecord.parsersVersion
									// citationRecord.parsersVersion // TODO: Investigate
								]);


							}

						}

					}
					catch (ex) {
						console.error("Error writing legislation to db" + ex);
						console.error(JSON.stringify(caseRecord, null, 4));
					}


				})())


				)

			}


			//====================================================================
			// Double Citations
			//====================================================================

			await (async () => {

				const allCitationsRaw: CaseCitation[] = (await pool.query("SELECT * FROM main.case_citations")).rows;
				// TODO: Fix case_id / fileKey
				const allCitations = allCitationsRaw.map(x => ({
					...x,
					fileKey: (x as any).case_id
				}));

				for (var i = 0; i < chunkedProcessedCases.length; i++) {

					const cases = chunkedProcessedCases[i]

					await Promise.all(cases.map(caseRecord => (async () => {

						const citationRecordsToCreate = parseCaseCitations(caseRecord.caseText, allCitations);

						if (citationRecordsToCreate.length > 0) {

							for (let crindex = 0; crindex < citationRecordsToCreate.length; crindex++) {

								const citationRecord = citationRecordsToCreate[crindex];

								await pool.query(`
							INSERT INTO main.case_citations (case_id, citation, id, year, parsers_version)
							VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`, [
									citationRecord.fileKey,
									citationRecord.citation,
									citationRecord.id,
									citationRecord.year,
									caseRecord.parsersVersion
									// citationRecord.parsersVersion // TODO: Investigate
								]);

							}


						}


					})())


					)

				}

			})()


			//====================================================================
			// Case to Case
			//====================================================================

			await (async () => {

				const allCitationsRaw: CaseCitation[] = (await pool.query("SELECT * FROM main.case_citations")).rows;
				// TODO: Fix case_id / fileKey
				const allCitations = allCitationsRaw.map(x => ({
					...x,
					fileKey: (x as any).case_id
				}));

				for (var i = 0; i < chunkedProcessedCases.length; i++) {

					const cases = chunkedProcessedCases[i]

					await Promise.all(cases.map(caseRecord => (async () => {

						const casesCitedRecord = parseCaseToCase(caseRecord.caseText, allCitations, caseRecord.fileKey);

						if (casesCitedRecord) {

							for (let x = 0; x < casesCitedRecord.case_cites.length; x++) {

								await pool.query(`
									INSERT INTO main.cases_cited (case_origin, case_cited, citation_count, parsers_version)
									VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`, [
									casesCitedRecord.case_origin,
									casesCitedRecord.case_cites[x].fileKey,
									casesCitedRecord.case_cites[x].count,
									caseRecord.parsersVersion
									// casesCitedRecord.parsersVersion // TODO: Investigate
								]);

							}


						}


					})())


					)

				}

			})();

			console.log(`Process Cases took ${timeToProcessCases} minutes`);

			console.log("Done");

		} else {
			console.log("No records to process")
		}

	} else {
		console.log("No case provider");
	}

	process.exit();

}