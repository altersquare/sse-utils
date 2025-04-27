require("dotenv").config("../.env");
const mongo5 = require("@orderstack/mongo-connector").mongo;
let ObjectID = require("mongodb").ObjectID;
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
	guficDB: "10.116.0.4:27017,gufic",
	indocoDB: "10.116.0.3:27017,indoco",
	oaknetDB: "10.116.0.6:27017,oaknet",
	sanofiDB: "10.116.0.5:27017,sanofi",
};

async function runScript() {
	let usedMetasGufic = await DBConn.guficDB.collection("mail_meta").distinct("metaUsed", { metaUsed: { $ne: null } });
	console.log("usedMetasGufic", usedMetasGufic.length);

	let usedMetasIndoco = await DBConn.indocoDB
		.collection("mail_meta")
		.distinct("metaUsed", { metaUsed: { $ne: null } });
	console.log("usedMetasIndoco", usedMetasIndoco.length);

	let usedMetasOaknet = await DBConn.oaknetDB
		.collection("mail_meta")
		.distinct("metaUsed", { metaUsed: { $ne: null } });
	console.log("usedMetasOaknet", usedMetasOaknet.length);

	let usedMetasSanofi = await DBConn.sanofiDB
		.collection("mail_meta")
		.distinct("metaUsed", { metaUsed: { $ne: null } });
	console.log("usedMetasSanofi", usedMetasSanofi.length);

	let usedFinalThresholdsGufic = await DBConn.guficDB
		.collection("mail_meta")
		.distinct("finalThresholdId.metaUsed", { finalThresholdId: { $ne: null } });
	console.log("usedFinalThresholdsGufic", usedFinalThresholdsGufic.length);

	let usedFinalThresholdsIndoco = await DBConn.indocoDB
		.collection("mail_meta")
		.distinct("finalThresholdId.metaUsed", { finalThresholdId: { $ne: null } });
	console.log("usedFinalThresholdsIndoco", usedFinalThresholdsIndoco.length);

	let usedFinalThresholdsOaknet = await DBConn.oaknetDB
		.collection("mail_meta")
		.distinct("finalThresholdId.metaUsed", { finalThresholdId: { $ne: null } });
	console.log("usedFinalThresholdsOaknet", usedFinalThresholdsOaknet.length);

	let usedFinalThresholdsSanofi = await DBConn.sanofiDB
		.collection("mail_meta")
		.distinct("finalThresholdId.metaUsed", { finalThresholdId: { $ne: null } });
	console.log("usedFinalThresholdsSanofi", usedFinalThresholdsSanofi.length);

	let allUsedMetas = usedMetasGufic
		.concat(usedMetasIndoco)
		.concat(usedMetasOaknet)
		.concat(usedMetasSanofi)
		.concat(usedFinalThresholdsGufic)
		.concat(usedFinalThresholdsIndoco)
		.concat(usedFinalThresholdsOaknet)
		.concat(usedFinalThresholdsSanofi)
		.map((e) => ObjectID(e));

	await DBConn.sseHQ
		.collection("file_meta")
		.aggregate([
			{
				$match: {
					_id: { $nin: allUsedMetas },
				},
			},
			{
				$out: "unused_metas",
			},
		])
		.toArray();

	await DBConn.sseHQ
		.collection("analytics_new")
		.aggregate([
			{
				$match: {
					$and: [{ "_id.metaUsed": { $nin: allUsedMetas } }, { "_id.metaUsed": { $exists: true } }],
				},
			},
			{
				$out: "unused_thresholds",
			},
		])
		.toArray();

	for (let conf in DBConn) {
		await DBConn[conf].close();
	}
	console.log("done");
	/*
	db.file_meta.deleteMany({ _id: { $in: db.unused_metas.distinct("_id") } })
	db.analytics_new.deleteMany({ _id: { $in: db.unused_thresholds.distinct("_id") } })
	*/
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
