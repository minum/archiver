var mongo 	= require('mongodb');
var qbox    = require('qbox');

function Aggregator(mongoUrl) {

	if(!(this instanceof Aggregator)) {
        return new Aggregator(mongoUrl);
    }

    var mongoDatabase = qbox.create(); 
    mongo.MongoClient.connect(mongoUrl, function(err, db) {

        if(err) {
            throw err;
        }

        mongoDatabase.db = db;
        mongoDatabase.start();

        db.on('error', function(err) {
            console.error('mongodb error: ' + err.toString());
        });
    });

    /*
		@param query {Object} - mongodb query object
		@param timeResolution {String} - timeResolution for aggregate, possible values: minute, hour, day, month
		@param collection {String} - collection to invoke
		@param callback {Function} - callback function 
			function(err, resultCollectionObject)
    */
	this.aggregate = function aggregate(query, timeResolution, collection, callback) {

		mongoDatabase.ready(function() {

			mongoDatabase.db.collection(collection).mapReduce(mapFunction, reduceFunction, {
				out: 'archive_aggregate_results',
				query: query,
				scope: { timeResolution: timeResolution }
			}, callback);
		});
	};

	function mapFunction () {
		
		var resolution = this.resolution;
		var keyDate = resolution.y + " " + (resolution.mo + 1);

		if(timeResolution == 'minute') {
			keyDate += " " + resolution.d + " " + resolution.d + ":" + resolution.m + " UTC"; 
		} else if(timeResolution == 'hour') {
			keyDate += " " + resolution.d + " " + resolution.d + ":00 UTC"; 
		} else if(timeResolution == 'day') {
			keyDate += " " + resolution.d + " UTC";
		} else {
			keyDate += " 1 UTC";
		}

		var value = {
			sum: this.value, 
			count: 1,
			min: this.value,
			max: this.value
		};

		emit({name: this.name, source: this.source, date: keyDate }, value);
	}

	function reduceFunction (id, values) {
		
		var result;

		for(var lc=1; lc<values.length; lc++) {
			var value = values[lc];

			if(result) {
				result.sum += value.sum;
				result.count += value.count;
				if(result.min > value.min) {
					result.min = value.min;
				}
				if(result.max < value.max) {
					result.max = value.max;
				}
			} else {
				result = value;
			}

		}

		return result;
	}
}

module.exports = Aggregator;