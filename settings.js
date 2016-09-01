/**
 * Created by middleca on 9/22/15.
 */
var fs = require('fs');
var path = require('path');
var extend = require('xtend');

var settings = {
	particle_base_url: "https://api.particle.io",
	mongo_databaseUrl: null,


	particle_org_slug: null,
	particle_product_slug: null,
	particle_product_id: null,

	particle_client_id: null,
	particle_client_secret: null,

	azure_device_connString: null,

	_: null
};


var overridesFile = "./overrides.js";

if (!fs.existsSync(overridesFile)) {
	overridesFile = path.join(__dirname, overridesFile);
}

if (fs.existsSync(overridesFile)) {
	try {
		var overridesObj = require(overridesFile);
		settings = extend(settings, overridesObj);
	}
	catch(ex) {
		console.error("error opening overrides ", ex);
	}
}
else
{
	//hmm, can we just enumerate the settings obj and look for those?

	// expected settings in the environment?
	var envSettings = [
		{ name: "some_property", key: "some_env_var" },
		{ name: "mongo_databaseUrl", key: "MONGO_URL" },
		{ name: "particle_client_id", key: "particle_client_id" },
		{ name: "particle_client_secret", key: "particle_client_secret" },

		//TODO: REPLACE ME
		{ name: "azure_device_connString", key: "azure_device_connString" },
	];


	//
	//	Iterate over any environmental settings we're expecting
	//

	var result = {};
	for(var i=0;i<envSettings.length;i++) {
		var obj = envSettings[i];

		var name = obj.name;
		var value = process.env[obj.key];

		if (value) {
			result[name] = value;
		}
	}
	settings = extend(settings, result);
}


module.exports = settings;
