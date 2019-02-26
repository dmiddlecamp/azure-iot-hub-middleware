var when = require('when');
var pipeline = require('when/pipeline');
var MongoClient = require('mongodb').MongoClient;
var ObjectID = require('mongodb').ObjectID;
var settings = require('../settings.js');
var logger = require('./logger.js');
//var utilities = require("./utilities.js");


/**
 * Database implementation, v1
 * @constructor
 */
var Database = function () {

};

Database.prototype = {
    classname: "Database",

    _conn: null,
    _collections: {},

    open: function(databaseURL) {
        if (this._conn) {
            return when.resolve(this._conn);
        }

        var dfd = when.defer();
		this.connect(databaseURL, function(err, conn) {
            if (err) {
				console.log("connection failed.", err);
                dfd.reject(err);
            }
            else {
				console.log("connection succeeded.");
                dfd.resolve(conn);
            }
        });

        return dfd.promise;
    },




    connect: function (databaseURL, callback) {
        var that = this;


		databaseURL = databaseURL || settings.mongo_databaseUrl;
		if (typeof databaseURL == "function") {
			callback = databaseURL;
			databaseURL =  settings.mongo_databaseUrl;
			//console.log("url is " + databaseURL);
		}

        if (!callback) {
            return;
        }

        //  Really we're just sharing one connection pool for the server.
        if (that._conn) {
            process.nextTick(function () {
                callback(null, that._conn);
            });
        }
        else {
            if (!databaseURL) {
                logger.error("mongo_databaseUrl setting is MISSING");
            }


            MongoClient.connect(databaseURL, {
                //auto_reconnect: true,
               // poolSize: settings.database_pool_size || 100,
//				server: {
//					sslValidate: false,
//				},
				sslValidate: false,
				//useNewUrlParser: true,



//                replset: { socketOptions: { keepAlive: 1, connectTimeoutMS: 5000 } },
//                server: { socketOptions: { keepAlive: 1, connectTimeoutMS: 5000 } }

            }, function (err, db) {
                if (err) {
                    console.error("Database: couldn't connect to Mongo ", err);
                }

                that._conn = db;
                callback(err, that._conn);
            });
        }
    },

    getCollection: function (name, callback) {
        if (!name || !callback) {
            logger.log("getCollection called with bad arguments");
            return false;
        }

        var col = this._collections[name];
        if (col) {
            callback(null, col);
            return;
        }

        var that = this;
        this.connect(function (err, db) {
            if (!db) {
                console.log("Error connecting to database", err);
            }
            else {
                col = db.collection(name);
            }

            that._collections[name] = col;
            callback(err, col);
        });

    },

    delete: function (colName, obj, callback) {
        var defer = when.defer();
        this.getCollection(colName, function (err, collection) {
            if (err || !collection) {
                logger.error("Delete error (couldn't get collection) " + err);
                defer.reject(err);
                return;
            }

            collection.remove(obj, null, function (err, count) {
                if (err) {
                    logger.error("mongodb delete error: ", err);
                    defer.reject(err);
                }
                else {
                    defer.resolve(count);
                }

                if (callback) {
                    try {
                        callback(err, count);
                    }
                    catch (ex) {
                        logger.error("delete callback error: ", ex);
                    }
                }


            });
        });

        return defer.promise;
    },


    upsert: function (colName, obj, callback) {
        var defer = when.defer();
        this.getCollection(colName, function (err, collection) {
            collection.insert(obj, { upsert: true, safe: true },
                function (err, docs) {
                    if (err) {
                        defer.reject(err);
                    }
                    else {
                        defer.resolve(docs);
                    }

                    if (callback) {
                        try {
                            callback(err, docs);
                        }
                        catch (ex) {
                            logger.error("upsert callback error: ", ex);
                        }
                    }
                });
        });
        return defer.promise;
    },

    find: function (collection_name, criteria, callback) {
		var dfd = when.defer();
        this.getCollection(collection_name, function (err, collection) {
            var cursor = (collection) ? collection.find(criteria, null, { safe: true }) : null;
			dfd.resolve(cursor);

			if (callback) {
				callback(err, cursor);
			}
        });
		return dfd.promise;
    },
//
//    getCoreAttributes: function (deviceID) {
//        if (!deviceID || (deviceID === '') || !sanityChecker.isString(deviceID, 8, 32)) {
//            return when.reject(["getCoreAttributes - deviceID must not be empty", 500]);
//        }
//
//        var dfd = when.defer();
//        this.find('cores', { deviceID: deviceID }, function (err, docs) {
//
//            if (err) {
//                logger.error("get_core_attributes - error - ", err);
//                dfd.reject(["get_core_attributes database error ", 500]);
//            }
//
//            if (docs) {
//                docs.nextObject(function (err, doc) {
//                    if (doc) {
//                        dfd.resolve(doc);
//                    }
//                    else {
//                        dfd.reject(["Sorry, I couldn't find that for you. ", 404]);
//                    }
//                });
//            }
//            else {
//                logger.error("get_core_attributes - docs was empty", err);
//                dfd.reject("docs was empty");
//            }
//        });
//
//        return dfd.promise;
//    },

    forEach: function (collection, filter, handler) {
        var dfd = when.defer();

        this.find(collection, filter, function (err, docs) {
            if (err) {
                logger.error("forEach - Error ", err);
                dfd.reject("Error " + err);
                return;
            }

            if (!docs) {
                dfd.reject("Nothing found");
                return;
            }

            docs.each(handler);
        });

        return dfd.promise;
    },

    list_devices: function (user_id, callback) {
        if (!user_id || (user_id === "")) {
            if (callback) {
                logger.error("list_devices: user_id was empty");
                process.nextTick(function () {
                    callback("user_id was empty");
                });
            }
            return;
        }
        this.find('cores', { user_id: user_id }, callback);
    },

    update: function (colName, criteria, attrs, multi) {
        var deferred = when.defer();
        var options = { safe: true, upsert: true };
        if (multi) {
            options.multi = true;
        }

        this.getCollection(colName, function (err, collection) {
            collection.update(criteria, { $set: attrs }, options, function (err, docs) {
                if (err) {
                    deferred.reject(err);
                }
                else {
                    deferred.resolve(docs);
                }
            });
        });
        return deferred.promise;
    },

    updateCores: function (deviceID, attrs) {
        if (!deviceID || (deviceID == "")) {
            return when.reject("deviceID was empty");
        }

        var deferred = when.defer();

        this.getCollection('cores', function (err, collection) {
            var criteria = { deviceID: deviceID },
                set_attrs = { $set: attrs };

            collection.update(criteria, set_attrs, { safe: true, upsert: true }, function (err) {
                if (err) {
                    deferred.reject(err);
                } else {
                    attrs.id = deviceID;
                    deferred.resolve(attrs);
                }
            });
        });

        return deferred.promise;
    },

    set_core_name: function (deviceID, name, userid) {
        if (typeof userid == "string") {
            userid = new ObjectID(userid);
        }

        var that = this;
        return pipeline([
            function() {
                return that.count("cores", { user_id: userid, name: name });
            },
            function(count) {
                if (count > 0) {
                    logger.error("Cannot rename user core - already in use",
                        { coreID: deviceID, userID: userid, name: name });

                    return when.reject("Name already in use");
                }
                else {
                    return when.resolve();
                }
            },
            function() {
                return that.updateCores(deviceID, { name: name });
            }
        ]);
    },

    claim_device: function (deviceID, user_id) {
        return this.updateCores(deviceID, { user_id: user_id });
    },

    userOwnsCore: function (userID, deviceID) {
        if (!deviceID || !userID) {
            return when.reject("userID and deviceID may not be empty");
        }
        else {
            return this.getUserCore(userID, deviceID);
        }
    },

    userOwnsApp: function (userID, appID) {
        if (!appID || !userID) {
            return when.reject("userID and appID may not be empty");
        }
        else {
            return this.exists('apps', { _id: new ObjectID(appID), user_id: userID });
        }
    },

    userOwnsHook: function (userID, hookID) {
        if (!hookID || !userID) {
            return when.reject("userID and hookID may not be empty");
        }
        else {
            return this.exists('hooks', { _id: new ObjectID(hookID), user_id: userID });
        }
    },

    /**
     * Checks to see if the user is allowed to create webhooks
     * @param userID
     */
    isHookingUser: function (userID) {
        var result = when.defer();

        this.query("users", { _id: userID }, function (err, arr) {
            var userObj = (arr && (arr.length > 0)) ? arr[0] : null;

            if (userObj && userObj.mayHook) {
                logger.log("isHookingUser - user may hook ", { userID: userID });
                result.resolve();
            }
            else {
                logger.log("isHookingUser - permission denied");
                result.reject();
            }
        });
        return result.promise;
    },

    isEventingUser: function (userID) {
        var result = when.defer();

        this.query("users", { _id: userID }, function (err, arr) {
            var userObj = (arr && (arr.length > 0)) ? arr[0] : null;

            if (userObj && userObj.mayEvent) {
                logger.log("isEventingUser - user may event ", { userID: userID });
                result.resolve();
            }
            else {
                logger.log("isEventingUser - permission denied");
                result.reject();
            }
        });
        return result.promise;
    },

    /**
     * Checks to see if the user is allowed to create new coreIDs.
     * @param userID
     */
    isProvisioningUser: function (userID, deviceID) {
        var result = when.defer();
        this.query("users", { _id: userID }, function (err, arr) {
            var userObj = (arr && (arr.length > 0)) ? arr[0] : null;
            if (userObj && userObj.mayProvision) {
                //TODO: check and see how many cores they've provisioned first...?
                logger.log("userMayProvision - user may provision ", { userID: userID, deviceID: deviceID });
                result.resolve();
            }
            else {
                logger.log("userMayProvision - user does not exist or does not have permissions");
                result.reject();
            }
        });
        return result.promise;
    },


//    userMayProvision: function (userID, deviceID) {
//        if (!userID) {
//            return when.reject("userID may not be empty");
//        }
//
//        var that = this;
//
//        //if we match any of these tests, then they're allowed.
//        return utilities.deferredAny([
//
//            //if the user owns it, then they can add keys for it.
//            function () {
//                return that.userOwnsCore(userID, deviceID);
//            },
//
//            //if nobody owns it, then we'll let them add a public key, for ease of fixing issues.
//            function () {
//                return that.nobodyOwnsCore(deviceID);
//            },
//
//            //if the user has special permissions allowing them to create a new core ID.
//            function () {
//                return that.isProvisioningUser(userID, deviceID);
//            }
//        ]);
//
//        //TODO: how to best log outcomes in this format?
//        //logger.error("userMayProvision - user not provision empty id", { userID: userID, deviceID: deviceID });
//        //logger.log("userMayProvision - user's core: ", { userID: userID, deviceID: deviceID });
//        //logger.error("userMayProvision - no permissions", { userID: userID, deviceID: deviceID });
//    },

    getUserCore: function (userID, deviceID) {
        var dfd = when.defer();

        var query = {
            deviceID: deviceID,
            user_id: userID
        };

        this.find('cores', query, function (err, cursor) {
            if (!cursor) {
                logger.error("getUserCore: ", err);
                dfd.reject(err);
            }
            else {
                cursor.nextObject(function (err, doc) {
                    if (err || !doc) {
                        dfd.reject(err);
                    }
                    else {
                        dfd.resolve(doc);
                    }
                });
            }
        });

        return dfd.promise;
    },

    getUserCoreByName: function(userID, coreName) {
        return this.query("cores", { user_id: userID, name: coreName });
    },

    nobodyOwnsCore: function (deviceID) {
        //device ID must exist, obv.
        if (!deviceID) {
            return when.reject("device ID may not be empty");
        }

        //core must EXIST, and be UNOWNED.
        //pull any 'cores' collection records for this core
        //  if no results, then core is not owned
        //  if a result but user_id is empty, then not owned / else owned

        //pull any core_keys records for this core
        // if any results, then core exists
        // else not exists.


        var that = this;
        return pipeline([
            //is the core owned?
            function () {
                return that.query("cores", { deviceID: deviceID  })
            },
            //handle the "cores" row(s) for this core,
            function (arr) {
                var allEmpty = true;
                if (arr && (arr.length > 0)) {
                    for (var i = 0; i < arr.length; i++) {
                        allEmpty &= !!(!arr[i] || !arr[i].user_id)
                    }
                }

                if (allEmpty) {
                    //nobody has claimed it or it's been released
                    return when.resolve();
                }
                else {
                    //somebody has claimed this core
                    return when.reject();
                }
            },

            //does the coreid even exist?
            function () {
                return that.query("core_keys", { deviceID: deviceID  });
            },
            //handle the core_keys response for this core.
            function (arr) {
                var coreExists = false;
                if (arr && (arr.length > 0)) {
                    for (var i = 0; i < arr.length; i++) {
                        coreExists |= !!(arr[i] && arr[i].deviceID);
                    }
                }

                if (coreExists) {
                    //core exists
                    return when.resolve();
                }
                else {
                    //core doesn't exist.
                    return when.reject();
                }
            }
        ]);
    },

    latest_app_revision: function (app_id, callback) {
        this.getCollection('revisions', function (err, revisions) {
            if (err) {
                callback(err, null);
            }
            else {
                if (revisions) {
                    var cursor = revisions.find({ app_id: new ObjectID(app_id) })
                        .sort({ _id: -1 }).limit(1);
                    cursor.nextObject(callback);
                }
                else {
                    //collection didn't exist.
                    callback(err, null);
                }
            }
        });
    },

    count_properties: function(colName, filter, propName, callback) {
        if (!callback) {
            throw new Error("No You.");
        }

        var sum = 0;
        this.find(colName, filter, function(err, cursor) {
            if (err) { callback(0); }

            cursor.each(function(err, doc) {
                if (doc == null) {
                    callback(sum)
                }
                else {
                    sum += parseInt(doc[propName]);
                }
            });
        });
    },

    insert_firmware_binary: function (options, callback) {
        var tmp = when.defer();
        var that = this;
        this.getCollection(settings.binaries_table, function (err, col) {
            if (err || !col) {
                if (callback) {
                    callback(err, null);
                }
                tmp.reject(err);
                return;
            }

            var doc = { binary_content: options.binary_content };
            if (options.user_id) {
                doc.user_id = options.user_id;
            }
            if (options.expires_at) {
                doc.expires_at = options.expires_at;
            }

            col.insert(doc, function (err, docs) {
                if (err || (!(docs && docs.length >= 1))) {
                    if (callback) {
                        callback(err, null);
                    }
                    tmp.reject(err);
                    return;
                }

                var binary_id = docs[0]._id;
                if (!options.revision_id) {
                    if (callback) {
                        callback(err, binary_id);
                    }
                    tmp.resolve(binary_id);
                }
                else {
                    that.getCollection('revisions', function (err, revisions) {
                        if (err) {
                            if (callback) {
                                callback(err, binary_id);
                            }
                            tmp.reject(err);
                            return;
                        }

                        var criteria = { _id: options.revision_id };
                        var changes = { $set: { firmware_binary_id: binary_id } };
                        revisions.update(criteria, changes, { safe: true }, function (err) {
                            if (callback) {
                                callback(err, binary_id);
                            }
                            tmp.resolve(binary_id);
                        });
                    });
                }
            });
        });
        return tmp.promise;
    },

//    liberateCoreDfd: function (deviceID) {
//        if (!deviceID || (deviceID == '')) {
//            return when.reject("deviceID was empty");
//        }
//
//        return this.update(deviceID, { user_id: null });
//    },

     query: function (collection, filter, callback) {
        var tmp = when.defer();
        this.find(collection, filter, function (err, docs) {
            if (docs) {
                docs.toArray(function (err, arr) {
                    if (err) {
                        tmp.reject(err);
                        if (callback) {
                            try { callback(err, null); } catch(ex) { logger.error("query: error in callback ", ex); }
                        }
                        return;
                    }

                    tmp.resolve(arr);
                    if (callback) {
                        try { callback(err, arr); } catch(ex) { logger.error("query: error in callback ", ex); }
                    }
                });
            }
            else {
                tmp.reject(err);

                if (callback) {
                    try { callback(err, null); } catch(ex) { logger.error("query: error in callback ", ex); }
                }
            }
        });

        return tmp.promise;
    },

    /**
     * resolves if something in the database exists matching that query.
     * @param collection
     * @param filter
     * @returns {promise|*|Function|Promise|promise}
     */
    exists: function (collection, filter, callback) {
        var dfd = when.defer();
        this.query(collection, filter)
            .then(function (arr) {
                if (arr && (arr.length > 0)) {
                    dfd.resolve(true);
                }
                else {
                    dfd.reject(false);
                }
            },
            function () {
                dfd.reject(false);
            }
        );

        if (callback) {
            when(dfd.promise).then(callback, callback);
        }

        return dfd.promise;
    },

    /**
     * Todo: probably a slick way of combining 'exists' and 'count'
     * @param collection
     * @param filter
     * @param callback
     * @returns {promise|*|Function|Promise|promise}
     */
    count: function (collection, filter, callback) {
        var dfd = when.defer();

        this.query(collection, filter)
            .then(function (arr) {
                if (arr && (arr.length > 0)) {
                    dfd.resolve(arr.length);
                }
                else {
                    dfd.resolve(0);
                }
            },
            function (err) {
                dfd.reject(err);
            }
        );

        if (callback) {
            when(dfd.promise).then(callback, callback);
        }

        return dfd.promise;
    },


    insert: function (colName, obj) {
        var defer = when.defer();
        this.getCollection(colName, function (err, collection) {
            collection.insert(obj, { safe: true }, function (err, docs) {
                if (err) {
                    logger.error("insert error: " + err);
                    defer.reject(err);
                }
                else {
                    defer.resolve(docs);
                }
            });
        });

        return defer.promise;
    },

    reset_key_valid_flag: function (deviceID) {
        if (!deviceID || (deviceID == '')) {
            logger.error("Database: arguments must not be null.");
            return when.reject("arguments must not be null");
        }

        //this should force the device service to retry all the keys
        return this.update("core_keys", { deviceID: deviceID }, { valid: null }, true);
    },

    add_core_publickey: function (deviceID, options, callback) {
        if (!deviceID || !options || !options.publicKey) {
            logger.error("Database: arguments must not be null.");
            return when.reject("arguments must not be null");
        }

        var that = this;
        var tmp = when.defer();

        options.deviceID = deviceID;

        var filter = {
            deviceID: deviceID,
            publicKey: options.publicKey
        };

        var insertNewKey = function () {
            //console.log('inserting key');
            that.insert("core_keys", options).then(function (err, docs) {
                    logger.log("provisioned new key for " + deviceID);
                    tmp.resolve(docs);
                },
                function (err) {
                    tmp.reject("Provisioning failed: " + err);
                });
        };


        //find any exact key matches if we have them...
        this.query("core_keys", filter)
            .then(function (doc) {
                if (doc && (doc.length > 0)) {
                    tmp.resolve("Already have that key");
                }
                else {
                    insertNewKey();
                }
            },
            function (err) {
                //cool, we don't have that key, insert it.
                insertNewKey();
            });

        if (callback) {
            tmp.promise.then(callback);
        }
        return tmp.promise;
    },

    updateUser: function (username, attrs) {
        var deferred = when.defer();

        this.getCollection('users', function (err, collection) {
            var criteria = { username: username },
                set_attrs = { $set: attrs };

            collection.update(criteria, set_attrs, { safe: true, upsert: true }, function (err, docs) {
                if (err) {
                    deferred.reject(err);
                }
                else {
                    console.log('updated, ', docs);
                    deferred.resolve(attrs);
                }
            });
        });

        return deferred.promise;
    },

    release_device: function (coreID, callback) {
        this.getCollection('cores', function (err, collection) {
            collection.update({ deviceID: coreID },
                { $unset: { user_id: '' } },
                { safe: true },
                callback);
        });
    },

    _end: null
};
exports = module.exports = new Database();
