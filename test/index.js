'use strict';

const adapter = require('../index'),
      expect = require('expect.js'),
      http = require('http').Server,
      io = require('socket.io'),
      ioc = require('socket.io-client'),
      amqp = require('amqplib'),
      msgpack = require('msgpack-js'),      
      debug = require('debug')('test');


var RABBIT_MQ_URI = 'amqp://rabbitmq';
if (process.env['RABBIT_MQ_URI']) {
    RABBIT_MQ_URI = process.env['RABBIT_MQ_URI'];
}
    
describe('socket.io-rabbitmq', function () {

    this.timeout(10000);

    it('basic emit', function(done) {
	create(function(server, client) {
	    server.on('connection', function(socket) {
		debug("client connected");
		socket.emit('msg', 'test');
	    });

	    client.on('msg', function(msg) {
		debug('message received %s', msg);
		expect(msg).to.eql('test');
		done();
	    });
	    
	});
    });
    
    it('broadcasts', function (done) {
	create(function (server1, client1) {
	    create(function (server2, client2){
		client1.on('woot', function (a, b) {
		    expect(a).to.eql([]);
		    expect(b).to.eql({a: 'b'});
		    done();
		});

		server2.on('connection', function (c2) {
		    server2.emit('woot', [], {a: 'b'});
		});
	    });
	});
    });

    it('broadcasts to rooms', function (done) {
        create(function (server1, client1) {
	    server1.on('connection', function (c1) {
		c1.join('woot');
	    });

	    client1.on('broadcast', function () {
		setTimeout(done, 100);
	    });

	    create(function (server2, client2) {
		server2.on('connection', function (c2) {
		    // does not join, performs broadcast
		    c2.on('do broadcast', function () {
			c2.to('woot').emit('broadcast');
		    });
		});

		client2.on('broadcast', function () {
		    throw new Error('Not in room');
		});

		create(function (server3, client3) {
		    server3.on('connection', function (c3) {
			// does not join, signals broadcast
			client2.emit('do broadcast');
		    });

		    client3.on('broadcast', function () {
			throw new Error('Not in room');
		    });
		});
	    });
	});
    });

    it('send to room with queued message', function(done) {
	create(function(server1, client1) {
	    server1.on('connection', function(c1) {
		server1.to('late').emit('msg','delayed');
		setTimeout(()=>{
		    debug('c1 joining late');
		    c1.join('late');
		}, 2000);
	    });

	    client1.on('msg', function(msg) {
		debug('woot room received %s', msg);
		expect(msg).to.eql('delayed');
		done();
	    });
	});
    });
    
    it('send to rooms via routing in rabbitmq', function(done) {
	var roles = {
	    client1: ['supervisor'],
	    client2: ['supervisor']
	};

	var respond = 0;
	function received() {
	    respond++;
	    if (respond == 2) {
		done();
	    }
	}
	
	create(roles, function(server1, client1) {
	    server1.on('connection', function(c1) {
		debug('client1 joining unique room');
		c1.join('client1');
	    });
	    
	    client1.on('woot', function(data) {
		debug("Client1 received data:");
		debug(data);
		expect(data).to.eql('hello');
		received();
	    });
	    
	    create(roles, function(server2, client2) {

		server2.on('connection', function(c2) {
		    debug('client2 joining unique room');
		    c2.join('client2');
		});

		client2.on('woot', function(data) {
		    debug("Client2 received data:");
		    debug(data);
		    expect(data).to.eql('hello');
		    received();
		});

		amqp.connect(RABBIT_MQ_URI).then(function(conn){
		    return conn.createChannel();
		}).then(function(ch) {
		    const exchange = 'amqpDirectExchange'
		    ch.assertExchange(exchange, 'direct', {durable:false});
		    debug("Sending message to supervisors");
		    var msg = {
			type: 2,
			data: ['woot', 'hello']
		    }		    
		    ch.publish(exchange, 'supervisor', msgpack.encode([msg]));
		});			
	    });
	});
    });

    // create a pair of socket.io server+client
    function create(roles, fn) {

	if (!fn) {
	    fn = roles;
	    roles = {};
	}
	
        const srv = http();
        const sio = io(srv);
	
        sio.adapter(adapter(RABBIT_MQ_URI, roles));
	var amqpAdapter = sio.sockets.adapter;
	
	initAmqp(amqpAdapter, function() {
	    srv.listen(function (err) {
		debug("Setting up server and client");
		if (err) {
		    debug(err);		    
		    throw err;
		} // abort tests
		
		const url = 'http://localhost:'+srv.address().port+'/'
		fn(sio.of('/'), ioc(url));
	    });	
	});

    }

    function initAmqp(adapter, fn) {
	if (!adapter.amqpPublishChannel) {
	    debug("waiting for init...");
	    setTimeout(function() {
		initAmqp(adapter, fn);
	    }, 1000);
	} else {
	    fn();
	}
	
    }
	
});
