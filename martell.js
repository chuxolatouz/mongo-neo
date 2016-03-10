var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');
var request = require('sync-request');
var url = 'mongodb://localhost:27017/proyectofase1';
var r=require("request");
var txUrl = "http://localhost:7474/db/data/transaction/commit";

var personajes = [];
var casas = [];

function newcypher(query,params){
  var res = request('POST', 'http://localhost:7474/db/data/transaction/commit', {
    json: { statements: [{statement:query,parameters:params}]}
  });
  console.dir(query);
  var user = res.getBody('utf8');
  console.dir(user);
}

function cypher(query,params,cb) {
  r.post({uri:txUrl,
          json:{statements:[{statement:query,parameters:params}]}},
         function(err,res) { cb(err,res.body)})
}

var query="MATCH (n) RETURN n LIMIT 25"
var params={limit: 10}
var cb=function(err,data) { console.log(JSON.stringify(data)) }
//cypher(query,params,cb)

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
	var casa, nombre, zona;
	for(var i = 0; i <item.length; i++){
//	estos son los datos a insertar en cada nodo
	//console.log('----------------');
	//console.dir(item[i].Nombre);
	nombre = '\"'+item[i].Nombre+'\"';
	//console.dir(item[i]._id);
	casa = item[i]._id;
		if (item[i].Zona != undefined){
			//console.dir(item[i].Zona);
			zona = '\"'+item[i].Zona+'\"';
		} else {
			zona = "Desconocido";
			zona = '\"'+item[i].Zona+'\"';
		}
	var params={};
	var cb=function(err,data) { console.log(JSON.stringify(data)) };
	var query = "CREATE("+"Casa"+nombre.replace(/"/g,"")+":casa{nombre:"+nombre+", idc:"+casa+", zona:"+zona+"})";
	//console.dir(query);
//query = "CREATE(personaje"+i+":"+casa.replace(/"/g,"")+"{ idm:"+id+","+"name:"+nombre+","+" Genero:"+genero+","+" Zona:"+zona+","+" Casa:"+casa+"})";
	//cypher(query,params,cb);
  newcypher(query,params);
	//console.log("after query casa");

	}

	for(var i = 0; i <item.length; i++){
		if (item[i].Aliados != undefined){
			for (var j=0; j<item[i].Aliados.length; j++){
				for (var k=0; k < item.length; k++){
					if(item[i].Aliados[j] == item[k].Nombre){
						/*console.log('------------');
						console.dir(item[i].Nombre);
						console.log('es aliado  de');
						console.dir(item[k].Nombre);*/
						var aliado1 = item[i]._id;
						var aliado2 = item[k]._id;

            var query ="MATCH (a),(b) WHERE a.idc ="+aliado1+" and b.idc = "+aliado2+" CREATE (a)-[r:ES_ALIADO_DE]->(b)";
            var cb=function(err,data) { console.log(JSON.stringify(data)) }
            var params = {};
            //cypher(query,params,cb);
            newcypher(query,params);
            //console.dir(query);
					}
				}
			}
		}
		if (item[i].Enemigos != undefined){
			for (var j=0; j<item[i].Enemigos.length; j++){
				for (var k=0; k < item.length; k++){
					if(item[i].Enemigos[j] == item[k].Nombre){
						/*console.log('------------');
						console.dir(item[i].Nombre);
						console.log('es enemigo  de');
						console.dir(item[k].Nombre);*/
						var enemigo1 = item[i]._id;
						var enemigo2 = item[k]._id;

            var query ="MATCH (a),(b) WHERE a.idc ="+enemigo1+" and b.idc = "+enemigo2+" CREATE (a)-[r:ES_ENEMIGO_DE]->(b)";
            var cb=function(err,data) { console.log(JSON.stringify(data)) }
            var params = {};
            //cypher(query,params,cb);
            newcypher(query,params);
            //console.dir(query);
					}
				}
			}
		}
	}
}

var insertNeo4jpersonaje = function(item){
	//estos son los datos a insertar en cada nodo
	//console.log('process and insert into neo4j');
	var nombre = "",id= "",zona="",genero="",eventos="[]",casa="",mato="[]";
	for(var i = 0; i <item.length; i++){
    		//console.log('---------------------');
    		////console.dir(item[i]);
		if (item[i].Nombre != undefined){
      			nombre=item[i].Nombre;
      			nombre = '\"'+nombre+'\"';
    		} else {
        		nombre="Undefined";
        		nombre = '\"'+nombre+'\"';
   		}
    		////console.dir(nombre);
		if (item[i]._id != undefined){
    			id = item[i]._id;
    		}
    		////console.dir(id);
    		if (item[i].Casa != undefined){
     			 casa = item[i].Casa;
     			 casa = '\"'+casa+'\"';
    		} else {
      			casa = "Desconocido";
			casa = '\"'+casa+'\"';
    		}
    		////console.dir(casa);
		if (item[i].Zona != undefined){
			zona = item[i].Zona;
 			zona = '\"'+zona+'\"';
   		} else {
    			zona = "Desconocido";
 			zona = '\"'+zona+'\"';
    		}
    		////console.dir(zona);
		if (item[i].Genero != undefined){
      			genero = item[i].Genero;
    			genero = '\"'+genero+'\"';
		}
		////console.dir(item[i].Genero);
		if (item[i].Eventos != undefined){
      //console.log('------------');
      eventos = JSON.stringify(item[i].Eventos);
      //eventos = JSON.stringify(item[i].Eventos).replace(/"/g,"\"");
			//console.dir(item[i].Eventos);
		} else {
      eventos = "[]";
    }
    if(item[i].Mató != undefined){
      mato= JSON.stringify(item[i].Mató);
    } else {
      mato = "[]";
    }
   		////console.dir('\"'+nombre+'\"');
    		query = "CREATE(personaje"+i+":"+casa.replace(/"/g,"")+"{ idm:"+id+","+"name:"+nombre+","+" Genero:"+genero+","+"Eventos:"+eventos+","+" Mato:"+mato+","+" Zona:"+zona+","+" Casa:"+casa+"})";
    		//console.dir(query);
    		var cb=function(err,data) { console.log(JSON.stringify(data)) }
    		var params = {};
    		//cypher(query,params,cb);
        newcypher(query,params);
        //console.dir(query);
	}

	//aqui terminan los datos de los nodos y comienzan las relaciones

	for(var j = 0; j <item.length; j++){
		if(item[j].Casa != undefined){
					var personaje = item[j]._id;
					var casa = item[j].Casa;
            				var query ="MATCH (a),(b) WHERE a.idm ="+personaje+" and b.nombre = "+'\"'+casa+'\"'+" CREATE (a)-[r:PERTENECE_A"+"{rating:"+Math.random()+"}"+"]->(b)";
            				var cb=function(err,data) { console.log(JSON.stringify(data)) }
            				var params = {};
            				//cypher(query,params,cb);
                    newcypher(query,params);
            				//console.dir(query);
		}

		if (item[j].Hijos != undefined){
			for(var k = 0; k <item[j].Hijos.length; k++){
				for(var l = 0; l <item.length; l++){
					if(item[j].Hijos[k]==item[l]._id){
						//console.log('------------');
						//console.dir(item[l].Nombre);
						//console.log('es hijo de');
						//console.dir(item[j].Nombre);
            					var padre = item[l]._id;
           				        var hijo = item[j]._id;
            var query ="MATCH (a),(b) WHERE a.idm ="+padre+" and b.idm = "+hijo+" CREATE (a)-[r:ES_HIJO_DE"+"{rating:"+Math.random()+"}"+"]->(b)";
            //console.dir(query);
            var cb=function(err,data) { console.log(JSON.stringify(data)) }
            var params = {};
            //cypher(query,params,cb);
            newcypher(query,params);
            //console.dir(query);
					}
				}

			}
		}

		if (item[j].Padres != undefined){
			for(var k = 0; k <item[j].Padres.length; k++){
				for(var l = 0; l <item.length; l++){
					if(item[j].Padres[k]==item[l]._id){
						//console.log('------------');
						//console.dir(item[l]._id);
						//console.log('es padre de');
						//console.dir(item[j]._id);
            					var padre = item[l]._id;
            					var hijo = item[j]._id;
            					var query ="MATCH (a),(b) WHERE a.idm ="+padre+" and b.idm = "+hijo+" CREATE (a)-[r:ES_PADRE_DE"+"{rating:"+Math.random()+"}"+"]->(b)";
            					//console.dir(query);
            					var cb=function(err,data) { console.log(JSON.stringify(data)) }
            					var params = {};

            					//cypher(query,params,cb);
                      newcypher(query,params);
                      //console.dir(query);
					}
				}

			}
		}

		if (item[j].Conyuge != undefined){
				for(var l = 0; l <item.length; l++){
					if(item[j].Conyuge==item[l]._id){
						//console.log('------------');
						//console.dir(item[l]._id);
						//console.log('es conyuge de');
						//console.dir(item[j]._id);
            var padre = item[l]._id;
            var cony = item[j]._id;
            var query ="MATCH (a),(b) WHERE a.idm ="+padre+" and b.idm = "+cony+" CREATE (a)-[r:ES_CONYUGE_DE"+"{rating:"+Math.random()+"}"+"]->(b)";
            //console.dir(query);
            var cb=function(err,data) { console.log(JSON.stringify(data)) }
            var params = {};
            //cypher(query,params,cb);
            newcypher(query,params);
            //console.dir(query);
					}
				}

		}
	}
  var params={};
  var query1 = "MATCH (n)-[:PERTENECE_A]->(x) WHERE n.Casa <> '' RETURN n.Casa as Casa, count(n.Casa) as cont order by cont desc";
  var query2 = "MATCH (x)-[:ES_PADRE_DE]->(n) where x.Genero=\"M\" and n.Genero=\"M\" with x.name as Padre, count(n.Genero) as HijosVarones where HijosVarones >= 2 return Padre, HijosVarones Order by HijosVarones desc";
  var query3 = "MATCH (n) WHERE n.Eventos <> \'\' unwind n.Eventos AS evento WITH n.name as Nombre, count(evento) AS NumEventos RETURN Nombre, NumEventos ORDER BY NumEventos DESC LIMIT 1";
  var query4 = "MATCH (x)-[:ES_ALIADO_DE]->(n:casa) WITH n.nombre as Casa, count(x) AS Cantidad RETURN Casa , Cantidad ORDER BY Cantidad DESC LIMIT 1";
  var query5 = "MATCH (n) unwind n.Mato AS mato WITH n.Casa as Casa, n.name as Nombre, count(mato) AS CantAsesinatos RETURN Casa, Nombre, CantAsesinatos ORDER BY CantAsesinatos DESC LIMIT 1";
  var query6 = "MATCH (x)-[:ES_CONYUGE_DE]->(n) where  n.Casa <> \'Martell\' WITH count(n) AS CantidadDifMiembros RETURN CantidadDifMiembros"
  newcypher(query1,params);
  newcypher(query2,params);
  newcypher(query3,params);
  newcypher(query4,params);
  newcypher(query5,params);
  newcypher(query6,params);

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
  findCasas(db, function() {
	insertNeo4jcasa(casas);

  });
  findPersonajes(db, function() {
	insertNeo4jpersonaje(personajes);
   db.close();
  });

});
