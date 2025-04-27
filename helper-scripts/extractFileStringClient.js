require("dotenv").config("../.env");
global.projectDir = "/root/secondary-sales-extraction";
global.log = {
	d: (e) => console.log(e),
	e: (e) => console.log(e),
	l: (e) => console.log(e),
};
global.config = {
	CONSTANTS: {
		idHeaderCleanerRegex: () =>
			/(\b(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sept|Sep|Oct|Nov|Dec)(\d{0,4})?(SalesQty|SalesValue|TaxValue|NetValue)?\b)|(\b\d{4}Ilas\b)|(\b\d{4}Ila\b)|(\b\d{4,6}(SalesQty|SalesValue|TaxValue|NetValue)\b)/gi,
	},
};
const downloadFilePath = projectDir + "/data/downloaded";
let pdfParse = require(projectDir + "/resources/pdf");
let xlsxParse = require(projectDir + "/resources/xlsx");
let txtParse = require(projectDir + "/resources/txt");
let csvParse = require(projectDir + "/resources/csv");
let htmlParse = require(projectDir + "/resources/html");
let fs = require("fs");
const EXTENSIONS = {
	__all: ["pdf", "xlsx", "txt", "csv", "html"],
	pdf: ["pdf"],
	html: ["html", "htm"],
	htm: ["html", "htm"],
	xlsx: ["xlsx", "xls", "xlsb"],
	xls: ["xlsx", "xls", "xlsb"],
	xlsb: ["xlsx", "xls", "xlsb"],
	csv: ["csv"],
	txt: ["txt"],
	mht: ["mht"],
	doc: ["doc"],
	docx: ["docx"],
	rtf: ["rtf"],
	jpeg: ["jpeg"],
	jpg: ["jpg"],
	png: ["png"],
};

const mongo5 = require("@orderstack/mongo-connector").mongo;
let sseConf = (conf) => {
	return {
		primary: true,
		dbms: "mongodb",
		host: conf.split(",")[0],
		dbName: conf.split(",")[1],
		un: process.env.SSE_DB_USERNAME,
		pw: process.env.SSE_DB_PASSWORD,
	};
};

let sseHQ = {
	primary: false,
	dbms: "mongodb",
	host: "138.197.228.132:5050",
	dbName: "sseHQ",
	un: process.env.SSE_DB_USERNAME,
	pw: process.env.SSE_DB_PASSWORD,
};
let DBConn = {
	sanofi: "10.116.0.5:27017,sanofi",
	indoco: "10.116.0.3:27017,indoco",
	oaknet: "10.116.0.6:27017,oaknet",
	gufic: "10.116.0.4:27017,gufic",
};

async function runScript() {
	var client = "indoco";
	let files = await DBConn[client]
		.collection("mail_meta")
		.find({
			$and: [
				{
					date: {
						$gte: new Date("2022-10-24T18:30:00.000Z"),
						$lt: new Date("2022-11-24T18:30:00.000Z"),
					},
				},
				{
					$or: [{ remarks: /EXTRACTED_DATA/ }, { metaUsed: { $exists: true } }],
				},
				{
					reintroduced: { $exists: false },
				},
			],
		})
		.toArray();

	let count = 0;
	console.log("total files", files.length);
	await DBConn[client].collection(client + "_fileStrings_v0").deleteMany({});
	for (let file of files) {
		count++;
		console.log(count);
		//get the file parser(helper) for the file extension of the file object provided
		let parser = await getParser(file.extension);

		if (!parser) {
			console.log("Parser not found");
			continue;
		}

		let transformArray = null;

		//if the transformed form of the file isn't available
		if (!fs.existsSync(projectDir + `/data/${EXTENSIONS[file.extension][0]}/${file.uuid}_transformed.json`)) {
			log.d("Transform not found, Asserting transform file....");

			try {
				// try to read the file data in UTF-8
				var readDetails = await parser.read({
					filepath: downloadFilePath + "/" + file.filename,
					timestamp: String(file.uuid),
				});
			} catch (e) {
				console.error(e);
			}

			// try creating the transformed file from the file data that has been read.
			try {
				await parser.transform(readDetails);
			} catch (e) {
				console.error(e);
			}
		}
		//load the transform file and load it in the transformArray variable
		transformArray = JSON.parse(
			fs.readFileSync(projectDir + `/data/${EXTENSIONS[file.extension][0]}/${file.uuid}_transformed.json`, {
				encoding: "utf-8",
			})
		);

		let meta = await DBConn.sseHQ.collection("file_meta").findOne({ _id: file.metaUsed });
		if (!meta) continue;
		let extractedMeta = await parser.detectMeta(
			{
				originalFilePath: downloadFilePath + "/" + file.filename,
				transformArray,
				uuid: file.uuid,
				execMode: "extract",
			},
			[meta]
		);

		if (extractedMeta) {
			await DBConn[client].collection(client + "_fileStrings_v0").insertOne({
				uuid: file.uuid,
				stockistId: file.stockistId,
				extractedStartDate: file.extractedStartDate,
				extractedEndDate: file.extractedEndDate,
				// startIndex: extractedMeta.index,
				metaUsed: file.metaUsed,
				// transformArray: arr,
				// fileString: transformArray.reduce((acc, cv) => {
				fileString: transformArray.slice(0, extractedMeta.index).reduce((acc, cv) => {
					if (Array.isArray(cv)) {
						for (let elem of cv) {
							acc += elem.str + " ";
						}
					} else if (typeof cv == "object") {
						acc += cv.str + " ";
					} else if (typeof cv == "string") {
						acc += cv + " ";
					} else {
						console.log(cv);
					}
					return acc + "\n";
				}, ""),
			});
		}
		// if (count == 100) break
	}
	console.log("processed file");

	for (let conf in DBConn) {
		await DBConn[conf].close();
	}
	console.log("done all");
}

async function init() {
	for (let conf in DBConn) {
		console.log(conf);
		DBConn[conf] = await mongo5(sseConf(DBConn[conf]));
	}
	console.log("sseHQ");
	DBConn["sseHQ"] = await mongo5(sseHQ);
	console.log("load done");
	await runScript();
}

init()
	.then(() => {})
	.catch((e) => {
		console.error(e);
	});

function getParser(extension) {
	let parser = null;
	// prettier-ignore
	switch (extension) {
	case "pdf": {
		parser = pdfParse;
		break;
	}
	case "xls":
	case "xlsx":
	case "xlsb": {
		parser = xlsxParse;
		break;
	}
	case "txt": {
		parser = txtParse;
		break;
	}
	case "csv": {
		parser = csvParse;
		break;
	}
	case "htm":
	case "html": {
		parser = htmlParse;
		break;
	}
	case "mht": {
		break;
	}
	default: {
		break;
	}
	}
	return parser;
}
