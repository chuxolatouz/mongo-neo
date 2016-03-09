var request = require('sync-request');
var query = "CREATE(Martell:casa{ idm:90, name:\"Martell\", Zona:\"Dorne\"})";
var params = {};
var l = ['week', 'month'];

/*
function newcypher(query,params){
  var res = request('POST', 'http://localhost:7474/db/data/transaction/commit', {
    json: { statements: [{statement:query,parameters:params}]}
  });
  var user = JSON.parse(res.getBody('utf8'));
  console.dir(user);
}

newcypher(query,params);
*/
