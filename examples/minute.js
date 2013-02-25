var Aggregator = require('../lib/aggregator');

var ag = new Aggregator('mongodb://localhost/metrics');
var query = {
    date: {
        $gt: new Date('2013 02 22 4:00 UTC').getTime(),
        $lt: new Date('2013 02 22 5:00 UTC').getTime()
    }
};

ag.aggregate(query, 'minute', 'metrics', prt);

function prt(err, results) {
    
    if(err) {
        console.error(err);
    } else {
        results.find().each(function(err, doc) {
            console.log(doc);
        });
    }
}