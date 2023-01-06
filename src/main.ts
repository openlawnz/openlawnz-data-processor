import { readFileSync, writeFileSync } from "fs";
import path from "path";
import { setTimeout as setTimeoutP } from "timers/promises";
import { multithreadProcess } from "./utils.js";
import yargs from 'yargs'
import { hideBin } from "yargs/helpers";
import pkg from 'pg';
import crypto from 'crypto';
import { courts, lawReports, judgeTitles } from "@openlawnz/openlawnz-parsers";
import CaseRecord from "./CaseRecord.js"

const argv = await yargs(hideBin(process.argv)).argv
const { Pool } = pkg;

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
if (argv.importlegislation) {

	const allLegislation = JSON.parse(readFileSync("./legislation.json").toString());

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
if(argv.importCases) {

	// Get from provider
	if(argv.casesProvider) {

		const allLegislation = (await pool.query("SELECT * FROM main.legislation")).rows;

		const MAX_THREADS = 4;

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

		var caseRecordsNotInCaches = await syncCasePDFs([]);

		if (caseRecordsNotInCaches.length > 0) {
			// TODO: prompt
			await sequentiallyDownloadFilesWithDelays(caseRecordsNotInCaches)
		}

		const processedCases = await processCases([]);


	} else {
		console.log("No case provider");
	}

	process.exit();

}