var MongoClient = require('mongodb').MongoClient;
var neo4j = require('neo4j');
var neo = new neo4j.GraphDatabase('http://username:password@localhost:7474');

// Connect to the db
MongoClient.connect("mongodb://localhost:27017/exampleDb", function(err, db) {
  if(err) { return console.dir(err); }

  db.collection('test', function(err, collection) {});

  db.collection('test', {w:1}, function(err, collection) {});

  db.createCollection('test', function(err, collection) {});

  db.createCollection('test', {w:1}, function(err, collection) {});

});


neo.cypher({
    query: 'MATCH (u:User {email: {email}}) RETURN u',
    params: {
        email: 'alice@example.com',
    },
}, function (err, results) {
    if (err) throw err;
    var result = results[0];
    if (!result) {
        console.log('No user found.');
    } else {
        var user = result['u'];
        console.log(JSON.stringify(user, null, 4));
    }
});
