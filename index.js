'use strict'

var Class		= require('class')
  , coroutine	= require('coroutine')
  , fs			= require('fs')
  , util		= require('util')

var re_patch = /^([a-zA-Z\d_-]+)\.([a-zA-Z\d_-]+)\.cql$/
var re_main = /^(([a-zA-Z\d_-]+))\.cql$/

var BaseDriver = Class.inherit({

})

var CassandraDriver = BaseDriver.inherit({

    onCreate: function(cc) {
    	this.cc = cc
    },

	checkStructure: coroutine.method(function*(driver, g) {

		var result = yield driver.cc.query('select * from system.schema_columnfamilies where keyspace_name = \'' + driver.cc.config.keyspace + '\'', g.resume)

		var existsEntityTable = false
		for(var i = 0, c = result.rows, l = c.length; i < l; i++) {
			var row = c[i]
			if(row.columnfamily_name === 'entitys') {
				existsEntityTable = true
				break
			}
		}

		if(existsEntityTable) return

		yield driver.cc.query('create table entitys ( name varchar, patches varchar, primary key (name) )', g.resume)

	}),

	readModelPatches: coroutine.method(function*(driver, name, g) {

		var result = yield driver.cc.query('select * from entitys where name = \'' + name + '\'', g.resume)
		var patches = {}

		if(result.rows.length > 0) {
			var row = result.rows[0]
			patches = row.patches !== null ? JSON.parse(row.patches) : {}
		}

		return patches
	}),

	processQuerys: coroutine.method(function*(driver, querys, g) {

		for(var i = 0, l = querys.length; i < l; i++) {
			var query = querys[i]
			if(query[0] == 'd') {
				yield driver.cc.query(query, g.resumeWithError)
			}
			else {
				yield driver.cc.query(query, g.resume)
			}
		}
	}),

	updateModelPatches: coroutine.method(function*(driver, name, patches, g) {

		yield driver.cc.query('update entitys set patches = \'' + JSON.stringify(patches) +' \' where name = \'' + name + '\'', g.resume)

	}),

	getHandler: function() {	

		return this.cc
	}


})

var processPatches = coroutine(function*(patches, driver, g) {

	var item = null
	for(var name in patches) {
		item = patches[name]
		break
	}

	if(null === item) {
		console.log('done appling patches to model ' + item.name)
		return
	}

	var patches_ = yield driver.readModelPatches(item.name, g.resume)

	for(var name in patches_) {
		delete patches[name]
	}

	if('.main' in patches) {

		patches_['.main'] = true
		console.log('apply patch ".main" to model ' + item.name)

		yield driver.processQuerys(patches['.main'].querys, g.resume)

		delete patches['.main']
		yield driver.updateModelPatches(item.name, patches_, g.resume)
	}

	while(true) {
		var cnt = 0, process = false
		for(var name in patches) {
			cnt ++
			var patch = patches[name]
			var notEnought = false
			for(var i = 0, c = patch.deps, l = c.length; i < l; i++) {
				var d = c[i]
				if(!(d in patches_)) {
					notEnought = true
					break
				}
			}

			if(!notEnought) {
				patches_[name] = true
				console.log('apply patch "' + name + '" to model ' + item.name)
				yield driver.processQuerys(patch.querys, g.resume)
				delete patches[name]

				yield driver.updateModelPatches(item.name, patches_, g.resume)
				process = true
				break
			}
		}

		if(cnt < 1) break
		if(!process) {
			console.log('exit from processPatches')
			console.log(util.inspect(patches,{depth:null}))
			break
		}
	}

	console.log('done appling patches to model ' + item.name)		
})



var loadModels = coroutine(function*(path, driver, g) {

    var models = { }

	yield driver.checkStructure(g.resume)

	var patches = { }

	var files = yield fs.readdir(path, g.resume)

	for(var i = 0, l = files.length; i < l; i++) {

	    var file = files[i]

	   	if(file[0] == '.') continue

		var stat = yield fs.stat(path + '/' + file, g.resume)
		if(stat.isFile()) {

			if('.js' === file.substr(-3)) {
				var modelName = file.substr(0, file.length - 3)
				models[modelName] = require(path + '/' + file)

				var proto = models[modelName].prototype

				proto.driver = driver.getHandler()
				proto.modelName = modelName
			}
			else if('.cql' === file.substr(-4)) {

				// modelname.cql - main
				// modelname.patchname.cql - patch
				// 		first string dependency - dep: patch1,patch2

				var a, model = null, patch = null
				if(a = re_patch.exec(file)) {
					model = a[1], patch = a[2]
				}
				else if (a = re_main.exec(file)) {
					model = a[1], patch = '.main'
				}

				if(model) {
					if(!(model in patches)) {
						patches[model] = { }
					}

					var content = ('' + (yield fs.readFile(path + '/' + file, g.resume)))
					var querys = content.split(';'), q = [], deps = []
					for(var i1 = 0, l1 = querys.length; i1 < l1; i1++) {
						var query = querys[i1]
						query = query.replace(/^[\r\s\t\n]+|[\r\n\s\t]+$/g, '')
						query = query.replace(/[\t\r\n]+/g, '')
						if(query.length < 1) continue
						if(query.length > 6 && query.substr(0, 6) == 'deps: ') {
							deps = query.substr(6).split(',')
						}
						else {
							q.push(query)
						}
					}

					patches[model][patch] = {
						name: model,
						querys: q,
						deps: deps
					}
				}
			}
		}
	}

	// console.log(util.inspect(patches,{depth:null}))

	for(var name in patches) {
		var item = patches[name]
		processPatches(item, driver, g.group(1, name))
	}
	var r = yield 0
	console.log('done appling patches')


	return models
})

module.exports = {

	loadModels: 	loadModels,

	drivers: {
		Base:		BaseDriver,

		Cassandra:	CassandraDriver,
	}
}


/*

	use case:

	var modelManager = require('model-manager')

	var cc = tcc.create({ poolSize: 5, hosts: [ '192.168.88.101' ], keyspace: 'ks' })
	// cc.config.keyspace
	// need: cc.query(queryText, callback)
	// for - modelManager.drivers.Cassandra

	global.models = yield modelManager.loadModels(__dirname + '/models', modelManager.drivers.Cassandra.create(cc), g.resume)


*/