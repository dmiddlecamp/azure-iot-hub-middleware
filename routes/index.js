var express = require('express');
var router = express.Router();

var AzureHelper = require('../lib/AzureHelper.js');

/* GET home page. */
router.get('/', function(req, res, next) {
	res.send({ok: false, error: "Nothing to see here!"});
});

router.get('/test', function(req, res, next) {

	var deviceID = "test_device",
		published_at = new Date() + "",
		event_topic = "a_fake_request",
		event_contents = "some_fake_data";

	AzureHelper.sendEvent(deviceID, published_at, event_topic, event_contents)
		.then(function() {
			res.send({ ok: true });
		},
		function(err) {
			res.send({ ok: false, error: err });
		});
});
router.post('/push', function(req, res, next) {
	try {
		// the default request
		//{
		//    "event": [event-name],
		//    "data": [event-data],
		//    "published_at": [timestamp],
		//    "coreid": [device-id]
		//}

		var getParam = function(req, name) {
			if (!req) { return; }
			else if (req.body && req.body[name]) {     return  req.body[name]; }
			else if (req.query && req.query[name]) {   return req.query[name]; }
			else if (req.params && req.params[name]) { return  req.params[name]; }
		};

		var deviceID = getParam(req, "coreid");
		var published_at = getParam(req, "published_at") || (new Date() + "");
		var event_topic = getParam(req, "event");
		var event_contents = getParam(req, "data");

//
//			published_at = req.body.published_at || req.query.published_at || req.params.published_at,
//			event_topic = req.body.event || req.query.event || req.params.event,
//			event_contents = req.body.data || req.query.data || req.params.data;

		//	var product_id = ,
		//		product_version = ,
		//		firmware_version = ;

		AzureHelper.sendEvent(deviceID, published_at, event_topic, event_contents)
			.then(function() {
				res.send({ ok: true });
			},
			function(err) {
				res.send({ ok: false, error: err });
			});
	}
	catch(ex) {
		console.log("push request failed, ", ex);
		res.send({ok: false, error: ex, error_data: JSON.stringify(ex, null, 2) });
	}
});

module.exports = router;
