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

	// the default request
	//{
	//    "event": [event-name],
	//    "data": [event-data],
	//    "published_at": [timestamp],
	//    "coreid": [device-id]
	//}

	var deviceID = req.params.coreid,
		published_at = req.params.published_at,
		event_topic = req.params.event,
		event_contents = req.params.data;

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
});

module.exports = router;
