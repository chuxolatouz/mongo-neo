var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');
var request = require("request");
var url = 'mongodb://localhost:27017/proyectofase1';
var r=require("request");
var txUrl = "http://localhost:7474/db/data/transaction/commit";

var personajes = [];
var casas = new Array;

function cypher(query,params,cb) {
  r.post({uri:txUrl,
          json:{statements:[{statement:query,parameters:params}]}},
         function(err,res) { cb(err,res.body)})
}

var query="MATCH (n) RETURN n LIMIT 25"
var params={limit: 10}
var cb=function(err,data) { console.log(JSON.stringify(data)) }

cypher(query,params,cb)

var findPersonajes = function(db, callback) {
   var cursor =db.collection('personajes').find();
   cursor.each(function(err, doc) {
      assert.equal(err, null);
      if (doc != null) {
         //console.dir(doc);
      } else {
         callback();
      }
   });
};

var findCasas = function(db, callback) {

   var cursor =db.collection('casas').find();
   cursor.each(function(err, doc) {
      assert.equal(err, null);
      if (doc != null) {
         console.dir(doc);
      } else {
         callback();
      }
   });
};

MongoClient.connect(url, function(err, db) {
  assert.equal(null, err);
  findPersonajes(db, function() {

  });
   findCasas(db, function() {

        db.close();
  });

});
