var mongo = require('mongodb'),
    Server = mongo.Server,
    Db = mongo.Db,
    express = require('express'),
    app = express();

var server = new Server('localhost', 27017, { auto_reconnect: true });
var db = new Db('weathered', server, { safe: false });

db.open(function (err, db) {
	if (err) throw err;

	app.listen(9090);
	console.log('Listening on port 9090.');	
})

app.use(express.static('site'));

app.use('/scripts/components', express.static('components'));

app.get('/stations', function (req, res) {
	db.collection('stations', function (err, coll) {
		if (err) {
			res.json(500, err);
		} else {
			coll.find({location: {$ne: null}}).toArray(function (err, items) {
				if (err) {
					res.json(500, err);
				} else {
					// convert to geojson
					var features = {
						type: "FeatureCollection",
						features: []
					};
					for (var i = 0; i < items.length; i++) {
						features.features.push({
							type: "Feature",
							geometry: {
								type: "Point",
								coordinates: [items[i].location[1], items[i].location[0]]
							},
							properties: {
								id: items[i]._id,
								name: items[i].name
							},
							id: items[i]._id
						});
					}
					res.json(200, features);
				}
			})
		}
	});
});

