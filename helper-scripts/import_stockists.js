// db.cust_master.find({}).forEach(e => {
// 	e["Customer Id"] = ("0".repeat(10 - String(e["Customer Id"]).length)) + String(e["Customer Id"])
// 	db.cust_master.save(e)
// })

// ``.split("\n").map(e => {
// 	e = ("0".repeat(10 - String(e).length)) + String(e)
// 	return e
// })

// db.alkem_cust_master.aggregate([
// 	{
// 		$addFields: {
// 			stockist_code: {$toString: "$Customer Id"},
// 			_len: {$subtract: [10, {$strLenCP: {$toString: "$Customer Id"}}]}
// 		}
// 	},
// 	{
// 		$addFields: {
// 			stockist_code: {$concat: [{$substr: [{$toString: {$toLong: {$pow: [10, "$_len"]}}}, 1, "$_len"]}, "$stockist_code"]}
// 		}
// 	},
// 	{
// 		$project: {
// 			_id: 0,
// 			"stockist_code": "$stockist_code",
// 			"name": "$Custome Name",
// 			"email": {$ifNull: ["$Email-ID", ""]},
//			"additional_emails": [],
// 			"gst_no": {$ifNull: ["$Gstn No", ""]},
// 			"pan_no": {$ifNull: ["$PAN NO", ""]},
// 			"line_1": {$ifNull: ["$Address", ""]},
// 			"line_2": {$ifNull: ["$City", ""]},
// 			"city": {$ifNull: ["$City", ""]},
// 			"state": {$ifNull: ["$Region", ""]},
// 			"custom": [],
// 			"valid": {$literal: 1},
// 			"_do_not_extract": {$literal: false},
// 			_valid_email: {
// 					$cond: [
// 					{
// 						$eq: [
// 							{ $trim: { input: { $ifNull: ["$email", ""] } } },
// 							"",
// 						],
// 					},
// 					false,
// 					true,
// 				],
// 			},
// 		}
// 	}
// ])

db.new_customer_indoco_07_08_2020.aggregate([
	{
		$project: {
			_id: 0,
			stockist_code: "$customer_code",
			name: "$customer_name",
			email: { $trim: { input: { $ifNull: ["$email", ""] } } },
			gst_no: { $trim: { input: { $ifNull: ["$gst_no", ""] } } },
			line_1: {
				$trim: { input: { $ifNull: ["$customer_address", ""] } },
			},
			line_2: { $trim: { input: { $ifNull: ["$ADD2", ""] } } },
			city: { $trim: { input: { $ifNull: ["$city_name", ""] } } },
			state: { $trim: { input: { $ifNull: ["$state_name", ""] } } },
			custom: [],
			valid: { $literal: 1 },
			_do_not_extract: { $literal: false },
			_valid_email: {
				$cond: [
					{
						$eq: [{ $trim: { input: { $ifNull: ["$email", ""] } } }, ""],
					},
					false,
					true,
				],
			},
		},
	},
]);

db.gufic_cust_master
	.aggregate([
		{
			$project: {
				_id: 0,
				stockist_code: "$ACCODE",
				name: "$ACNAME",
				email: { $trim: { input: { $ifNull: ["$EMAIL", ""] } } },
				gst_no: { $trim: { input: { $ifNull: ["$GSTIN", ""] } } },
				pan_no: { $trim: { input: { $ifNull: ["$PANNO", ""] } } },
				line_1: { $trim: { input: { $ifNull: ["$ADD1", ""] } } },
				line_2: { $trim: { input: { $ifNull: ["$ADD2", ""] } } },
				city: { $trim: { input: { $ifNull: ["$CITY", ""] } } },
				state: { $trim: { input: { $ifNull: ["$STATE", ""] } } },
				custom: [],
				valid: { $literal: 1 },
				_do_not_extract: { $literal: false },
				_valid_email: {
					$cond: [
						{
							$eq: [
								{
									$trim: {
										input: { $ifNull: ["$email", ""] },
									},
								},
								"",
							],
						},
						false,
						true,
					],
				},
			},
		},
		{
			$out: "stockist_meta",
		},
	])
	.toArray();

db.getCollection("stockist_meta").aggregate([
	{
		$lookup: {
			from: "alkem_stockist_meta",
			as: "alk",
			let: { gst_no: "$gst_no", email: "$email" },
			pipeline: [
				{
					$match: {
						$expr: {
							$and: [{ $ne: ["$gst_no", ""] }, { $ne: ["$email", ""] }],
						},
					},
				},
				{
					$match: {
						$expr: {
							$and: [
								{
									$or: [{ $eq: ["$gst_no", "$$gst_no"] }, { $eq: ["$email", "$$email"] }],
								},
							],
						},
					},
				},
				{
					$project: {
						email: 1,
						gst_no: 1,
						custom: 1,
						_valid_email: 1,
						_id: 0,
					},
				},
			],
		},
	},
	{
		$match: {
			"alk.0": { $exists: true },
		},
	},
	{
		$count: "sum",
	},
]);
