var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');
var request = require("request");
var url = 'mongodb://localhost:27017/proyectofase1';
var r=require("request");
var txUrl = "http://localhost:7474/db/data/transaction/commit";

var personajes = [];
var casas = [];
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
	personajes.push(doc);
      } else {
         callback();
      }
   });
};

var insertNeo4jcasa = function(item){
	console.log('process and insert into neo4j');
	for(var i = 0; i <item.length; i++){
//	estos son los datos a insertar en cada nodo
	console.dir(item[i].Nombre);
	console.dir(item[i]._id);
	console.dir(item[i].Zona);
	}
//aqui terminan los datos de los nodos y comienzan las relaciones
	for(var j = 0; j <item.length; j++){
		if (item[j].Aliados != undefined){
			for(var k = 0; k <item[j].Aliados.length; k++){
				for(var l = 0; l <item.length; l++){
					if(item[j].ALiados[k]==item[l].Nombre){
						console.log('------------');
						console.dir(item[l].Nombre);
						console.log('es aliado  de');
						console.dir(item[j].Nombre);
					}
				}

			}
		}

	}
	for(var j = 0; j <item.length; j++){
		if (item[j].Enemigos != undefined){
			for(var k = 0; k <item[j].Enemigos.length; k++){
				for(var l = 0; l <item.length; l++){
					if(item[j].Enemigos[k]==item[l].Nombre){
						console.log('------------');
						console.dir(item[l].Nombre);
						console.log('es enemigo  de');
						console.dir(item[j].Nombre);
					}
				}

			}
		}

	}
}
var insertNeo4jpersonaje = function(item){
	//estos son los datos a insertar en cada nodo
	console.log('process and insert into neo4j');
	for(var i = 0; i <item.length; i++){
    var nombre,id,zona,genero,eventos,casa;
		console.log('---------------------');
    //console.dir(item[i]);
		if (item[i].Nombre != undefined){
      nombre=item[i].Nombre;
      nombre = '\"'+nombre+'\"';
    } else {
        nombre="Doe"+i;
        nombre = '\"'+nombre+'\"';
    }
    //console.dir(nombre);
		if (item[i]._id != undefined){
    id = item[i]._id;
    }
    console.dir(id);
    if (item[i].Casa != undefined){
      casa = item[i].Casa;
      casa = '\"'+casa+'\"';
    } else {
      casa = "Arena";
    }
    //console.dir(casa);
		if (item[i].Zona != undefined){
		zona = item[i].Zona;
    zona = '\"'+zona+'\"';
    } else {
    zona = "Dorne";
    }
    //console.dir(zona);
		if (item[i].Genero != undefined){
      genero = item[i].Genero;
		//console.dir(item[i].Genero);
    genero = '\"'+genero+'\"';
		}
		if (item[i].Eventos != undefined){
		console.dir(item[i].Eventos);
		}




    //console.dir('\"'+nombre+'\"');
    query = "CREATE(personaje"+i+":"+casa.replace(/"/g,"")+"{ idm:"+id+","+"name:"+nombre+","+" Genero:"+genero+","+" Zona:"+zona+","+" Casa:"+casa+"})";
    console.dir(query);
    var cb=function(err,data) { console.log(JSON.stringify(data)) }
    var params = {};
    //cypher(query,params,cb);
	}

	//aqui terminan los datos de los nodos y comienzan las relaciones

	for(var j = 0; j <item.length; j++){

		if (item[j].Hijos != undefined){
			for(var k = 0; k <item[j].Hijos.length; k++){
				for(var l = 0; l <item.length; l++){
					if(item[j].Hijos[k]==item[l]._id){
						console.log('------------');
						console.dir(item[l]._id);
						console.log('es hijo de');
						console.dir(item[j]._id);
            var padre = item[l]._id;
            var hijo = item[j]._id;
            var query ="MATCH (a),(b) WHERE a.idm ="+padre+" and b.idm = "+hijo+" CREATE (a)-[r:ES_HIJO_DE]->(b)";
            console.dir(query);
            var cb=function(err,data) { console.log(JSON.stringify(data)) }
            var params = {};
            cypher(query,params,cb);
					}
				}

			}
		}

		if (item[j].Padres != undefined){
			for(var k = 0; k <item[j].Padres.length; k++){
				for(var l = 0; l <item.length; l++){
					if(item[j].Padres[k]==item[l]._id){
						console.log('------------');
						console.dir(item[l]._id);
						console.log('es padre de');
						console.dir(item[j]._id);
            var padre = item[l]._id;
            var hijo = item[j]._id;
            var query ="MATCH (a),(b) WHERE a.idm ="+padre+" and b.idm = "+hijo+" CREATE (a)-[r:ES_PADRE_DE]->(b)";
            console.dir(query);
            var cb=function(err,data) { console.log(JSON.stringify(data)) }
            var params = {};
            cypher(query,params,cb);
					}
				}

			}
		}

		if (item[j].Conyuge != undefined){
				for(var l = 0; l <item.length; l++){
					if(item[j].Conyuge==item[l]._id){
						console.log('------------');
						console.dir(item[l]._id);
						console.log('es conyuge de');
						console.dir(item[j]._id);
            var padre = item[l]._id;
            var cony = item[j]._id;
            var query ="MATCH (a),(b) WHERE a.idm ="+padre+" and b.idm = "+cony+" CREATE (a)-[r:ES_CONYUGE_DE]->(b)";
            console.dir(query);
            var cb=function(err,data) { console.log(JSON.stringify(data)) }
            var params = {};
            cypher(query,params,cb);
					}
				}

		}
	}

}
var findCasas = function(db, callback) {

   var cursor =db.collection('casas').find();
   cursor.each(function(err, doc) {
      assert.equal(err, null);
      if (doc != null) {
         //console.dir(doc);
	casas.push(doc);
      } else {
         callback();
      }
   });
};

MongoClient.connect(url, function(err, db) {
  assert.equal(null, err);
  findPersonajes(db, function() {
	insertNeo4jpersonaje(personajes);
  });
   findCasas(db, function() {
	//insertNeo4jcasa(casas);
        db.close();
  });

});
