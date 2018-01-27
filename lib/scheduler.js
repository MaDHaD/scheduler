const redis = require('redis'),
    moment = require('moment'),
    config = require('../config');

let createRedisClient = function (options) {
    let host = options.host;
    let port = options.port;
    let db = options.db;
    let redisOptions = options.redisOptions;
    let redisClient = redis.createClient(port, host, redisOptions);

    if (db) {
        redisClient.select(db);
    }

    return redisClient;
};

class Scheduler {
    constructor(options) {
        options = options || {};

        this.db = options.db || 0;
        this.clients = {
            scheduler: createRedisClient(options),
            listener: createRedisClient(options)
        };
        this.redisEvents();
    }
}

Scheduler.prototype.redisEvents = function () {
    let self = this;
    this.cleanup();

    this.clients.scheduler.send_command('config', ['set','notify-keyspace-events','EKx'], (err, status) => {
        const expired_subKey = '__keyevent@'+self.db+'__:expired';

        self.clients.listener.subscribe(expired_subKey, function(){
            self.clients.listener.on('message', function (channel, key) {
                self.clients.scheduler.hgetall(key + '_archived', function (err, obj) {
                    self.handleExpireEvent(obj)
                })
            });
        })
    })
};

Scheduler.prototype.checkMessagesExpired = function () {
    let self = this;
    this.clients.scheduler.keys("*", function (err, keys) {
        for(key of keys) {
            self.clients.scheduler.hgetall(key, function (err, obj) {
                if(moment().format('x') < obj.expireAt) {
                    self.schedule({key: obj.originalKey, duration: self.getMillisToExpire(obj.expireAt)})
                } else {
                    self.handleExpireEvent(obj)
                }
            });
        }
    });
};

Scheduler.prototype.getMillisToExpire = function (expiration) {
    return expiration - moment().format('x');
};

Scheduler.prototype.schedule = function (options, cb) {
    let self = this;
    let duration = options.duration || 1;
    let exipration = moment().add(duration, 'seconds').format('x');

    this.clients.scheduler.exists(options.key, function (err, exists) {
        if (exists) {
            self.clients.scheduler.pexpire(options.key, duration, cb);
        } else {
            self.clients.scheduler.hmset([options.key, "expireAt", exipration, 'originalKey', options.key]);
            self.clients.scheduler.hmset([options.key + '_archived', "expireAt", exipration, 'originalKey', options.key]);
            self.clients.scheduler.pexpire(options.key, self.getMillisToExpire(exipration), cb)
        }
    });

};

Scheduler.prototype.handleExpireEvent = function (obj) {
    console.log("[" + moment(obj.expiredAt).format("YYYY-MM-DD HH:mm:ss") + "] Expired event: " + obj.originalKey);
    this.clients.scheduler.del(obj.originalKey + '_archived');

};

Scheduler.prototype.cleanup = function () {
    this.clients.listener.removeAllListeners();
    this.clients.scheduler.removeAllListeners();
};

module.exports = Scheduler;