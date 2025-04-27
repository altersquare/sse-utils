require("dotenv").config("../.env");
const mongo5 = require("@orderstack/mongo-connector").mongo;
const moment = require("moment-timezone");
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

let fieldUpload = {
	primary: false,
	dbms: "mongodb",
	host: "138.197.228.132:5050",
	dbName: "field_upload",
	un: process.env.SSE_DB_USERNAME,
	pw: process.env.SSE_DB_PASSWORD,
};
let DBConn = {
	sanofi: "10.116.0.5:27017,sanofi",
	indoco: "10.116.0.3:27017,indoco",
	oaknet: "10.116.0.6:27017,oaknet",
};

async function runScript() {
	for (const client of Object.keys(DBConn).filter((elem) => elem != "fieldUpload")) {
		await DBConn[client]
			.collection("core_prod_mapping_" + client)
			.drop()
			.catch((err) => console.error(err.message));
		await DBConn[client].collection("core_prod_mapping_" + client).insertMany(
			await DBConn.fieldUpload
				.collection("core_prod_mapping_" + client)
				.find({ product_code: { $nin: ["null", "", "IGNORE", null] } })
				.toArray()
		);
		await DBConn[client].collection("core_prod_mapping_" + client).createIndex({ preCompute: 1 });
		await DBConn[client].collection("core_prod_mapping_" + client).createIndex({ product_code: 1 });

		await DBConn[client]
			.collection("brand_master_" + client)
			.drop()
			.catch((err) => console.error(err.message));
		await DBConn[client]
			.collection("missing_from_brand_master_" + client)
			.drop()
			.catch((err) => console.error(err.message));
		await DBConn[client].createCollection("brand_master_" + client).catch((err) => console.error(err.message));
		await DBConn[client]
			.createCollection("missing_from_brand_master_" + client)
			.catch((err) => console.error(err.message));

		await DBConn[client].collection("brand_master_" + client).createIndex({ product_name: 1 }, { unique: true });
		console.log(client);

		const current = () => moment().subtract(1, "months");
		let lookupColl = current().format("MMMMYYYY").toUpperCase();

		let count = 0;
		while (count != 10) {
			console.log("trying", lookupColl);
			await DBConn[client].collection(lookupColl).createIndex({ preCompute: 1 });
			await DBConn[client]
				.collection("core_prod_mapping_" + client)
				.aggregate([
					{
						$lookup: {
							from: lookupColl,
							localField: "preCompute",
							foreignField: "preCompute",
							as: "lookup",
						},
					},
					{
						$match: {
							"lookup.0": { $exists: true },
						},
					},
					{
						$addFields: {
							lookup: { $arrayElemAt: ["$lookup", 0] },
						},
					},
					{
						$project: {
							_id: 0,
							preCompute: 1,
							product_code: 1,
							product_name: "$lookup.PRODUCT_NAME",
						},
					},
					{
						$group: {
							_id: {
								product_code: "$product_code",
								product_name: "$product_name",
							},
						},
					},
					{
						$project: {
							_id: 0,
							product_code: "$_id.product_code",
							product_name: "$_id.product_name",
						},
					},
					{
						$merge: {
							into: "brand_master_" + client,
							on: "product_name",
							whenMatched: "keepExisting",
							whenNotMatched: "insert",
						},
					},
				])
				.toArray();
			count++;
			lookupColl = current().subtract(count, "months").format("MMMMYYYY").toUpperCase();
		}

		let missing = await DBConn[client]
			.collection("core_prod_mapping_" + client)
			.find({
				product_code: {
					$nin: await DBConn[client].collection("brand_master_" + client).distinct("product_code"),
				},
			})
			.toArray();

		if (missing.length) {
			await DBConn[client].collection("missing_from_brand_master_" + client).insertMany(missing);
		}

		await DBConn[client]
			.collection("core_prod_mapping_" + client)
			.drop()
			.catch((err) => console.error(err.message));

		console.log("done", client);
	}
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
	console.log("fieldUpload");
	DBConn["fieldUpload"] = await mongo5(fieldUpload);
	console.log("load done");
	await runScript();
}

init()
	.then(() => {})
	.catch((e) => {
		console.error(e);
	});
