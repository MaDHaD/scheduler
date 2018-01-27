'use strict';
const app = require('express')(),
    bodyParser = require('body-parser'),
    config = require('./config'),
    Scheduler = require('./lib/scheduler'),
    scheduler = new Scheduler(config.redis);

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());
app.set('port', (process.env.PORT || 5000));

app.post('/message', function(req, res) {
    res.send(req.body);

    scheduler.schedule({key: req.body.message, duration: req.body.duration}, function (err) {
        if (err) {
            console.error(err);
        } else {
            console.log('Scheduled successfully!');
        }
    });

});

app.listen(app.get('port'), function(){
    scheduler.checkMessagesExpired();

    console.log('Server listening on port: ', app.get('port'));
});