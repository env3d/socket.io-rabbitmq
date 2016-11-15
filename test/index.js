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

    it('send with ack', function(done){
	create(function(server, client) {
	    server.on('connection', function(socket) {
		debug("client connected");
		socket.join('ackRoom');
		
		server.to('ackRoom').emit('msg', 'test');
		
		socket.on('ack',function(deliveryId) {
		    debug('ack received from client, performing ack on amqpChannel');
		    socket.amqpChannel((ch)=>ch.ack(deliveryId));
		    done();
		});
	    });

	    client.on('msg', function(msg, deliveryId) {
		debug('message received %s', msg);
		debug(deliveryId);		
		expect(msg).to.eql('test');
		client.emit('ack',deliveryId);
	    });
	});
    });

    // We have some issues when broadcasting to a socket.id
    // the assert for socket.id 
    it('emiting to a socket via broadcast', function(done) {
	create(function(server, client) {
	    server.on('connection', function(socket) {
		server.to(socket.id).emit('msg','hi');		
	    });

	    client.on('msg', function(msg) {
		expect(msg).to.eql('hi');
		done();
	    });
	    
	});
    });
	 
    it('multiple clients to one room', function(done) {
	var connected = 0;
	var ackCount = 0;

	const numMessages = 3;
	const numClients = 2;
	
	const room = 'ackMultiRoom';

	var messageId = 0;
	
	create(function(server, client1) {

	    server.on('connection', function(socket) {
		debug('client connected %s', socket.id);
		socket.join(room);
		debug('total clients connected %s',++connected);
		if (connected == numClients) {
		    for (var i = 1; i <= numMessages; i++) {
			setTimeout(() => {
			    server.to(room).emit('msg', 'message '+(messageId++));
			}, i*500);			
		    }
		}

		socket.on('ack', function(deliveryId) {
		    debug('ack from socket %s, id %s', socket.id, deliveryId.channel);
		    socket.amqpChannel(function(ch) {
			/*
			if (ackCount == 0) {
			    if (deliveryId.channel === socket.id) {
				debug('not acking on purpose, closing connection');
				socket.disconnect();
				return;
			    }
			}
			*/
			ch.ack(deliveryId);
			debug('Ack received from %s, ack count: %s',socket.id, ++ackCount);

			if (ackCount == numMessages*numClients) {
			    done();
			}
		    });
		});
	    });

	    client1.on('msg', function(msg, deliveryId) {
		debug('client1: %s', msg);
		debug(deliveryId);		
		this.emit('ack', deliveryId);
		//this.close();
	    });
	    
	    // create another client to connect to the same server
	    for (var i = 1; i < numClients; i++) {
		debug('creating additional client');
		ioc(client1.url, {multiplex: false}).on(
		    'msg',
		    function(msg, deliveryId) {
			debug('client2: %s', msg);
			debug(deliveryId);
			this.emit('ack', deliveryId);
			//this.close();
		    }
		);
	    }
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
	    
	    client1.on('woot', function(data, deliveryId) {
		debug("Client1 received data:");
		debug(data, deliveryId);
		expect(data).to.eql('hello');
		received();
	    });
	    
	    create(roles, function(server2, client2) {

		server2.on('connection', function(c2) {
		    debug('client2 joining unique room');
		    c2.join('client2');

		    debug("Sending message to supervisors via direct amqp channel");
		    var msg = {
			type: 2,
			data: ['woot', 'hello']
		    }
		    
		    c2.amqpChannel(function(channel) {
			debug('publishing message to woot');
			//console.log(channel);
			channel.publish('amqpDirectExchange', 'supervisor', msgpack.encode([msg]));
		    });
		});

		client2.on('woot', function(data, deliveryId) {
		    debug("Client2 received data:");
		    debug(data,deliveryId);
		    expect(data).to.eql('hello');
		    received();
		});
	    });
	});	
    });
    
    it('multi server room listing', function(done) {
	create(function(server1, client1) {
	    server1.on('connection', function(socket) {
		socket.join('user1');
	    });
	    create(function(server2, client2) {
		server2.on('connection', function(socket) {
		    socket.join('user2');
		    console.log(server1.adapter.rooms);
		    console.log(server2.adapter.rooms);
		    done();
		});
	    });
	});
    });

    it.skip('kick out other clients in the same node', function(done) {
	create(function(server, client1) {
	    
	    setTimeout(() => {
		server.to('room1').emit('msg', 'hi');
	    }, 1000);
	    
	    server.on('connection', function(socket) {		
		socket.join('room1');

	    });

	    client1.on('msg', function(data) {
		debug('client 1 receives %s', data);
	    });

	    var client2 = ioc(client1.url, {multiplex: false});
	    client2.on('msg', function(data) {
		debug('client 2 receives %s', data);
	    });	    
	    
	});
    });

    it.only('kick out other clients from different node', function(done) {
	create(function(server1, client1) {
	    server1.on('connection', function(socket) {
		socket.join('room1');
	    });
	    create(function(server2, client2) {
		server2.on('connection', function(socket) {
		    setTimeout(() => {
			debug('client2 joining');
			socket.join('room1');			
		    }, 1000);
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
		const client = ioc(url,{multiplex:false});
		client.url = url;

		fn(sio.of('/'), client);
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
