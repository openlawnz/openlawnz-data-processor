import os from 'os';
import { ensureDirSync } from "fs-extra/esm";
import { createWriteStream, unlink } from "fs";
import path from "path";
import { setTimeout as setTimeoutP } from "timers/promises";
import { chunkArrayInGroups, FileLogger, getCaseFromPermanentJSON, getCitation, getS3Client, multithreadProcess } from "./utils.js";
import yargs from 'yargs'
import { hideBin } from "yargs/helpers";
import pkg from 'pg';
import crypto from 'crypto';
import inquirer from 'inquirer';
import fs, { ensureFileSync } from "fs-extra";
import { courts, lawReports, judgeTitles, parseCaseCitations, parseCaseToCase } from "@openlawnz/openlawnz-parsers";
import { CaseCitation } from "@openlawnz/openlawnz-parsers/dist/types/CaseCitation.js"
import CaseRecord, { ProcessedCaseRecord } from "./CaseRecord.js"
import { ListObjectsV2Request, S3 } from '@aws-sdk/client-s3';
import cliProgress from 'cli-progress';
import got from 'got';
import {pipeline as streamPipeline} from 'node:stream/promises';

const argv = await yargs(hideBin(process.argv)).argv
const { Pool } = pkg;

const MAX_THREADS = Math.max(1, os.cpus().length - 4);

const RUN_KEY = Date.now() + "";

const localImportCasesRunsPath = process.env.LOCAL_IMPORT_CASES_RUNS_PATH;

if (!localImportCasesRunsPath) {
	throw new Error(`You must set LOCAL_IMPORT_CASES_RUNS_PATH environment variable`)
}

ensureDirSync(localImportCasesRunsPath);

const logger = new FileLogger(path.join(localImportCasesRunsPath, RUN_KEY));

logger.log("MAX_THREADS: " + MAX_THREADS);

const connectionString = process.env.CONNECTION_STRING;

if (!connectionString) {
	logger.error("No connection string")
	throw new Error("No connection string");
}

const pool = new Pool({
	connectionString
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

	const localImportCasesRunsPath = process.env.LOCAL_IMPORT_CASES_RUNS_PATH;
	const savePermanentJSONPath = process.env.PERMANENT_JSON;
	const s3IndexFilePath = process.env.S3_INDEX_FILE;

	if (!localImportCasesRunsPath || !savePermanentJSONPath) {
		throw new Error(`You must set LOCAL_IMPORT_CASES_RUNS_PATH and PERMANENT_JSON environment variables`)
	}

	if(!s3IndexFilePath) {
		throw new Error("Missing " + s3IndexFilePath);
	}

	ensureFileSync(s3IndexFilePath);

	ensureDirSync(localImportCasesRunsPath);

	const S3Index = fs.readJSONSync(s3IndexFilePath) || [];

	let S3Client: S3 = getS3Client();

	let recordsToProcess: CaseRecord[] = []

	const fetchLocations = [];

	// TODO: Add && S3Client
	
	if (process.env.ACC_BUCKET && process.env.ACC_FILE_KEY) {

		fetchLocations.push(async () => {

			logger.log("Get ACC cases");

			const data = await S3Client.getObject({
				Bucket: process.env.ACC_BUCKET,
				Key: process.env.ACC_FILE_KEY
			});

			const json = JSON.parse(await data.Body?.transformToString() || "");

			if (json) {
				return json.map((record: any) => new CaseRecord(
					"https://openlawnz-pdfs-prod.s3-ap-southeast-2.amazonaws.com/" + record.file_key_openlaw,
					record.file_key_openlaw,
					record.file_provider,
					record.citations || [],
					record.case_date,
					[record.case_name],
					new Date(),
					true
				)
				)

			}
			return []

		});

	}
	
	fetchLocations.push(async () => {
		logger.log("Get JDO cases");

		type JDOCase = {
			JudgmentDate: string,
			DocumentName: string,
			CaseName: string
		};

		const data = await fetch("https://openlawnz-jsons.s3.ap-southeast-2.amazonaws.com/allCases.json")
		const json: JDOCase[] = await data.json();
		return json.map((doc) => {

			const fileKey = `jdo_doc_` + doc.DocumentName;
			const neutralCitation = getCitation(doc.CaseName);

			return new CaseRecord(
				"https://www.justice.govt.nz/jdo_documents/workspace___SpacesStore_" + doc.DocumentName,
				fileKey,
				"jdo",
				neutralCitation ? [neutralCitation] : [],
				doc.JudgmentDate,
				[doc.CaseName],
				new Date()
			)

		});
	})

	const getRecordsResult = await Promise.all(fetchLocations.map(fetchLocation => fetchLocation()));

	recordsToProcess = getRecordsResult.reduce((prev, cur) => {
		return prev.concat(cur);
	}, [])

	//recordsToProcess = recordsToProcess.slice(0, 10000);

	if (recordsToProcess.length > 0) {

		logger.log(`recordsToProcess ${recordsToProcess.length}`)

		// TODO: Tie into S3 check above	
		if (!process.env.LOCAL_CASE_PATH || !process.env.S3_CASE_BUCKET) {
			throw Error("Missing LOCAL_CASE_PATH or S3_CASE_BUCKET env vars");
		}

		if (!argv.reprocessOCR && !process.env.OCR_BUCKET) {
			throw Error("reprocessOCR is not set and missing OCR_BUCKET variable");
		}

		if (!process.env.PERMANENT_OCR) {
			throw Error("Missing PERMANENT_OCR variable");
		}

		const localCasePath = process.env.LOCAL_CASE_PATH;
		const S3CaseBucket = process.env.S3_CASE_BUCKET;
		const OCRBucket = process.env.OCR_BUCKET;
		const permamentOCR = process.env.PERMANENT_OCR;
		const reprocessOCR = !!argv.reprocessOCR;

		const allLegislation = (await pool.query("SELECT * FROM main.legislation")).rows;

		const progressBar = new cliProgress.SingleBar({
			forceRedraw: true,
			autopadding: true,
			clearOnComplete: true
		}, cliProgress.Presets.shades_classic);
		
		async function syncCasePDFs(caseRecords: CaseRecord[]): Promise<CaseRecord[]> {
			logger.log(`syncCasePDFs - caseRecords: ` + caseRecords.length);
			// Cannot have too many threads doing fetching
			const caseRecordsNotInCaches: CaseRecord[] = await multithreadProcess(logger, MAX_THREADS, caseRecords, './syncCasePDF.js', {
				localCasePath,
				S3CaseBucket,
				s3Index: S3Index
			}, (totalChunks: number) => {
				progressBar.start(totalChunks, 0);
			}, (totalProcessed: number) => {
				progressBar.update(totalProcessed);
			});
			progressBar.stop();
			return caseRecordsNotInCaches.flat();
		}

		/*
		THIS IS A CRITICAL FUNCTION AS IT STOPS THE PROGRAM FROM DOS'ING PROVIDERS. DO NOT OVERRIDE.
		*/
		async function sequentiallyDownloadFilesWithDelays(caseRecords: CaseRecord[]): Promise<{ casesToExclude: CaseRecord[] }> {
			logger.log(`sequentiallyDownloadFilesWithDelays - caseRecords: ${caseRecords.length}`)
			progressBar.start(caseRecords.length, 0);
			var casesToExclude: CaseRecord[] = [];
			for (const caseRecord of caseRecords) {

				const filePath = path.join(localCasePath, caseRecord.fileKey);
				const file = createWriteStream(filePath);

				try {
					logger.log(`Download ${caseRecord.fileURL}`, false);
					const gotStream = got.stream.get(caseRecord.fileURL);

					try {
						await streamPipeline(gotStream, file);
					} catch (error) {
						logger.log(`Error with stream ${caseRecord.fileKey} ${error}`, false);
					}

					file.close();
				} catch (error) {
					logger.log(`Error with ${caseRecord.fileKey} ${error}`, false);
					await new Promise(resolve => {
						file.close(() => {
							unlink(filePath, resolve);
						});
					});
					casesToExclude.push(caseRecord);
				}
				finally {
					progressBar.increment();
					if (!caseRecord.fileURL.startsWith("https://openlawnz-pdfs-prod.s3-ap-southeast-2.amazonaws.com")) {
						await setTimeoutP(1000)
					}
				}

			}

			progressBar.stop();

			await syncCasePDFs(caseRecords.filter(x => {
				return !casesToExclude.find(y => y.fileKey == x.fileKey)
			}))
			return {
				casesToExclude
			};
		}

		async function processCases(caseRecords: CaseRecord[]) {
			logger.log(`processCases - caseRecords: ` + caseRecords.length);
			const result = await multithreadProcess<CaseRecord, string>(
				logger,
				MAX_THREADS,
				caseRecords,
				'./processCase.js',
				{
					localCasePath,
					allLegislation,
					OCRBucket,
					reprocessOCR,
					savePermanentJSONPath,
					permamentOCR
				}, (totalChunks: number) => {
					progressBar.start(totalChunks, 0);
				}, (totalProcessed: number) => {
					progressBar.update(totalProcessed);
				});
				progressBar.stop();
			return result;
		}

		var caseRecordsNotInCaches = await syncCasePDFs(recordsToProcess);

		logger.log("caseRecordsNotInCaches: " + caseRecordsNotInCaches.length);

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

		logger.log(`Process ${recordsToProcess.length} cases`);

		const startProcessedCases = +new Date();

		logger.log("Truncating previous dataset")
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

		const processedCases: string[] = await processCases(recordsToProcess);

		const endProcessedCases = +new Date();

		const timeToProcessCases = ((endProcessedCases - startProcessedCases) / 60000).toFixed(2);

		const chunkedProcessedCases: string[][] = chunkArrayInGroups<string>(processedCases, 10);


		//====================================================================
		// Put case and related info into DB
		//====================================================================

		logger.log("Put in DB");

		progressBar.start(3, 0);
		for (const cases of chunkedProcessedCases) {

			await Promise.all(cases.map(caseRecordLink => (async () => {

				const caseRecord: ProcessedCaseRecord = getCaseFromPermanentJSON(caseRecordLink);

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
						logger.error("Error writing case pdf records", false);
						logger.error(JSON.stringify(ex, null, 2), false);
						logger.error(caseRecord.fileKey, false);
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
						logger.error("Error writing case record", false);
						logger.error(JSON.stringify(ex, null, 2), false);
						logger.error(caseRecord.fileKey, false);
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
						logger.error("Error writing case pdf records update", false);
						logger.error(JSON.stringify(ex, null, 2), false);
						logger.error(caseRecord.fileKey, false);
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
						logger.error("Error writing case record update", false);
						logger.error(JSON.stringify(ex, null, 2), false);
						logger.error(caseRecord.fileKey, false);
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
					logger.error("Error writing representation to db", false);
					logger.error(JSON.stringify(ex, null, 2), false);
					logger.error(caseRecord.fileKey, false);
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
					logger.error("Error writing judges to cases to db", false)
					logger.error(JSON.stringify(ex, null, 2), false);
					logger.error(caseRecord.fileKey, false);
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
					logger.error("Error writing categories to db", false)
					logger.error(JSON.stringify(ex, null, 2), false);
					logger.error(caseRecord.fileKey, false);
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
					logger.error("Error writing legislation to db", false);
					logger.error(JSON.stringify(ex, null, 2), false);
					logger.error(caseRecord.fileKey, false);
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
					logger.error("Error writing legislation to db", false);
					logger.error(JSON.stringify(ex, null, 2), false);
					logger.error(caseRecord.fileKey, false);
				}


			})())


			)

		}


		//====================================================================
		// Double Citations
		//====================================================================
		progressBar.increment();
		await (async () => {

			const allCitationsRaw: CaseCitation[] = (await pool.query("SELECT * FROM main.case_citations")).rows;
			// TODO: Fix case_id / fileKey
			const allCitations = allCitationsRaw.map(x => ({
				...x,
				fileKey: (x as any).case_id
			}));

			for (const cases of chunkedProcessedCases) {

				await Promise.all(cases.map(caseRecordLink => (async () => {

					const caseRecord: ProcessedCaseRecord = getCaseFromPermanentJSON(caseRecordLink);

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
		progressBar.increment();
		await (async () => {

			const allCitationsRaw: CaseCitation[] = (await pool.query("SELECT * FROM main.case_citations")).rows;
			// TODO: Fix case_id / fileKey
			const allCitations = allCitationsRaw.map(x => ({
				...x,
				fileKey: (x as any).case_id
			}));

			for (const cases of chunkedProcessedCases) {

				await Promise.all(cases.map(caseRecordLink => (async () => {

					const caseRecord: ProcessedCaseRecord = getCaseFromPermanentJSON(caseRecordLink);

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

		progressBar.stop();

		//====================================================================
		// Upload to Azure
		//====================================================================
		
		await (async () => {
			logger.log("Sync to Azure");

			await multithreadProcess(logger, MAX_THREADS, processedCases, './syncAzure.js', undefined, (totalChunks: number) => {
				progressBar.start(totalChunks, 0);
			}, (totalProcessed: number) => {
				progressBar.update(totalProcessed);
			});

		})();

		logger.log(`Process Cases took ${timeToProcessCases} minutes`);

		logger.log("Done");		

	} else {
		logger.log("No records to process")
	}

	process.exit();

}

if(argv.rebuildS3Index) {
	
	if (!process.env.S3_INDEX_FILE) {
		throw Error("Missing LOCAL_CASE_PATH or S3_CASE_BUCKET env vars");
	}

	let S3Client: S3;

	try {
		S3Client = getS3Client();
	} catch {

	}

	if(!process.env.S3_INDEX_FILE) {
		throw new Error("Missing " + process.env.S3_INDEX_FILE);
	}

	async function listObjectNames(): Promise<string[]> {
		try {
			// Create an array to store the object names
			let objectNames: string[] = [];
	
			// Define the parameters for the listObjectsV2 call
			let params: ListObjectsV2Request = {
				Bucket: process.env.S3_CASE_BUCKET,
			};
	
			do {
				// Call S3 to list the current batch of objects
				let response = await S3Client.listObjectsV2(params);
	
				// Get the names from the returned objects and push them to the objectNames array
				response.Contents?.forEach((obj: any) => obj.Key && objectNames.push(obj.Key));
	
				// If the response is truncated, set the marker to get the next batch of objects
				params.ContinuationToken = response.NextContinuationToken;
			} while (params.ContinuationToken);
	
			// Write the object names to a file
			fs.writeFileSync(process.env.S3_INDEX_FILE as string, JSON.stringify(objectNames, null, 2), 'utf8');
			console.log(`Object names written to ${process.env.S3_INDEX_FILE}`);
	
			return objectNames;
		} catch (error) {
			console.error('Error occurred while listing objects', error);
			throw error;
		}
	}
	
	await listObjectNames();

}