'use strict';

/**
 * Module dependencies.
 */

const Adapter = require('socket.io-adapter'),
      amqplib = require('amqplib'),      
      debug = require('debug')('socket.io-rabbitmq'),
      Socket = require('socket.io/lib/socket.js'),
      msgpack = require('msgpack-js'),
      shortid = require('shortid');

/**
 * Module exports.
 */

module.exports = adapter;

var defaults = {
    exchange_name: 'amqpFanoutExchange',
    exchange_type: 'fanout',
    global_queue: 'amqpGlobalReceiverQueue',
    direct_exchange_name: 'amqpDirectExchange'
}

function adapter(uri, roles)
{
    defaults.uri = uri;
    defaults.roles = roles || {};
    return AMQPAdapter;
}

/**
 * Inherits from `Adapter`.
 */

AMQPAdapter.prototype.__proto__ = Adapter.prototype;

/**
 * Adapter constructor.
 *
 * @param {String} nsp name
 * @api public
 */

function AMQPAdapter(nsp)
{
    Adapter.call(this, nsp);
    var self = this;

    self.amqp_uuid = shortid.generate();
    defaults.global_queue += self.amqp_uuid;
    
    // a list of client channels, key by socket.io's sid
    self.amqpChannels = {};
    // consumers is a hash of sid to consumerTag
    self.amqpConsumers = {};
    // this is the channel for publishing
    self.amqpPublishChannel = null;

    debug('Connecting to rabbitmq %s', defaults.uri);
    amqplib.connect(defaults.uri).then(function(conn) {
	debug('Creating channel');
	self.amqp_connection = conn;
	return conn.createChannel();
    }).then(function(ch) {
	debug('Creating exchanges and global queue');
	ch.assertExchange(defaults.direct_exchange_name, 'direct', {durable: false});	
	ch.assertExchange(defaults.exchange_name, defaults.exchange_type, {durable: false});
	self.amqpPublishChannel = ch;
	return ch.assertQueue(defaults.global_queue, {durable: false, autoDelete: true});
    }).then(function(queue) {
	debug('Binding global queue to fanout exchange');
	debug(queue);
	return self.amqpPublishChannel.bindQueue(defaults.global_queue, defaults.exchange_name, '', {});
    }).then(function(ok){
	debug('Subscribe to the global queue');
	return self.amqpPublishChannel.consume(defaults.global_queue, msg => {
	    debug("Message received from global queue");	    
	    
	    const args = msgpack.decode(msg.content);
	    const originUuid = args.shift();
	    debug('Message coming from %s', originUuid);
	    debug(args[1]);
	    if (self.amqp_uuid === originUuid) {
		debug('Message originates from this node.  Safely ignored...');
		return;
	    }

	    if (args[0] === 'disconnect') {
		// internal request to disconnect from a room
		var room = args[1];
		debug('Disconnecting all connections from room %s', room);
		
		if (self.rooms[room]) {
		    Object.keys(self.rooms[room].sockets).forEach(function(socketId) {
			debug('removing socket %s from room %s', socketId, room);
			self.nsp.connected[socketId].disconnect(true);		    
		    });
		}
		return;
	    }
	    
	    debug('Broadcasting to local node');			
	    args.push(true);
	    Adapter.prototype.broadcast.apply(self, args);
	}, {noAck: true});	
    }).catch(function(err){
	debug('Error creating connection');
	debug(err);
    });

    // A socket object can call socket.amqpChannel() to retrieve
    // the amqpChannel object to perform amqp related commands
    // such as publish and confirm.

    // see http://www.squaremobius.net/amqp.node/channel_api.html for what
    // you can do with channels
    nsp.on('connection', function(socket) {
	socket.amqpChannel = function(callback) {
	    if (self.amqpChannels[socket.id]) {
		debug(callback);
		callback(self.amqpChannels[socket.id]);
	    } else {
		setTimeout(() => socket.amqpChannel(callback), 100);
	    }
	}		
    });

    // allow client to retrieve amqpChannel as well
    nsp.amqpChannel = function(callback) {
	if (self.amqpPublishChannel) {
	    callback(self.amqpPublishChannel);
	} else {
	    setTimeout(() => {nsp.amqpChannel(callback)}, 100);	    
	}
    }
}

/**
 * Subscribe client to room messages.
 *
 * @param {String} id Client ID
 * @param {String} room
 * @param {Function} fn (optional)
 * @api public
 */

AMQPAdapter.prototype.add = function (id, room, fn)
{
    debug('adding %s to %s ', id, room);

    const self = this;

    if(this.rooms[room]) {
	Object.keys(this.rooms[room].sockets).forEach(function(socketId) {
	    if (socketId === id) {
		debug('>>> %s is already in this room', socketId);
	    } else {
		debug('>>> Another connection, %s, is already in the room, kicking them out', socketId);
		self.nsp.connected[socketId].disconnect(true);
	    }
	});
    }
    // tell all other nodes to disconnect from this room
    self.amqpPublishChannel.publish(defaults.exchange_name, '', msgpack.encode([self.amqp_uuid,'disconnect',room]));		
    
    Adapter.prototype.add.call(this, id, room);

    var opts = null;
    if (id === room) {
	opts = {durable: false, autoDelete:true};
    }
    
    // channel already created
    if (this.amqpChannels[id]) {
	// simply subscribe
	subscribeToQueue.call(this, this.amqpChannels[id], id, room, opts, defaults.roles);
	return;
    }
    
    // createChannel contains the main subscribe
    // logic, and also contains the callback when
    // a message is received
    createChannel.call(this, id, room, opts, function() {
	subscribeToQueue.call(this, this.amqpChannels[id], id, room, opts, defaults.roles);
    });

};
	
/**
 * Broadcasts a packet.
 *
 * @param {Object} packet packet to emit
 * @param {Object} opts options
 * @param {Boolean} remote whether the packet came from another node
 * @api public
 */

AMQPAdapter.prototype.broadcast = function (packet, opts)
{
    debug('broadcast');
    debug(packet);
    debug(opts);

    //const msg = msgpack.encode([this.amqpConsumerID, packet, opts]);        
    const self = this;
    
    // send packet to named queues.  room is translated to queues
    // The broadcast simply queues the message, the receive callback will
    // send the data back to all the sockets
    // we revert to regular broadcast if rooms are not invovled.    
    if (opts.rooms) {
	opts.rooms.forEach(function(room) {
	    // this is tricky... in socket.io, room can be explicit, or implicit in the case of
	    // the connection name.  When we create the amqp queue, we use the socket.id name as
	    // the queue name, but we declare autoDelete: true, so we have to match it here.
	    if (self.nsp.connected[room]) {
		self.amqpPublishChannel.assertQueue(room, {durable: false, autoDelete: true});
	    } else {
		self.amqpPublishChannel.assertQueue(room, {durable: false});
	    }
	    self.amqpPublishChannel.publish('', room, msgpack.encode([packet,opts]));	
	});
    } else {
	debug('forwarding to global queue');
	self.amqpPublishChannel.publish(defaults.exchange_name, '', msgpack.encode([self.amqp_uuid,packet,opts]));
    }
};

/**
 * Unsubscribe client from room messages.
 *
 * @param {String} id Session Id
 * @param {String} room Room Id
 * @param {Function} fn (optional) callback
 * @api public
 */

AMQPAdapter.prototype.del = function (id, room, fn)
{
    debug('removing %s from %s', id, room);

    this.amqpConsumers[id] = this.amqpConsumers[id].filter(consumerTag => {
	if (consumerTag.room === room) {
	    this.amqpChannels[id].cancel(consumerTag.tag);
	    return false;
	} else {
	    return true;
	}
    });

    
    Adapter.prototype.del.call(this, id, room);    
};

/**
 * Unsubscribe client completely.
 *
 * @param {String} id Client ID
 * @param {Function} fn (optional)
 * @api public
 */

AMQPAdapter.prototype.delAll = function (id, fn)
{
    debug('removing %s from all rooms', id);

    var self = this;
    var cancelChannel = function(id) {
	if (self.amqpChannels && self.amqpChannels[id]) {
	    debug('deleting channel %s', id);
	    self.amqpChannels[id].close();
	    delete self.amqpChannels[id];
	    delete self.amqpConsumers[id];
	} else {
	    debug('channel %s not available yet, waiting', id);
	    setTimeout(function() {
		cancelChannel(id);
	    }, 100);
	}
    }

    cancelChannel(id);
    
    /*
    this.amqpChannels[id] ? this.amqpChannels[id].close() : {} ;
    delete this.amqpChannels[id];
    delete this.amqpConsumers[id];
    */
    
    Adapter.prototype.delAll.call(this, id);
};

/**
 * Utility method to allow sending a message to a particular key
 *
 */
AMQPAdapter.prototype.send = function(message, key) {
    var self = this;    
    self.amqpPublishChannel.publish(defaults.direct_exchange_name, key, msgpack.encode([message]));
}


/**
 * Private function to create a channel
 */
var errorCount = 0;
function createChannel(id, room, opts, fn) {
    var self = this;
    if (!self.amqp_connection) {
	debug('connection not available, wait a bit');
	if (errorCount++ < 5) {
	    setTimeout(() => createChannel.call(this, id, room, opts, fn), 1000);
	} else {
	    console.log('no connection available at all, giving up');
	    // @todo should probably hard crash this guy
	    process.exit(1);
	}
    } else {

	self.amqp_connection.createChannel().then(function(ch) {
	    // override the ack method of channel to allow multiple
	    // clients to send ack without killing the channel if
	    // the deliveryTag comes from a different channel
	    debug('channel created');
	    ch.original_ack = ch.ack;
	    ch.ack = function(deliveryTag) {
		if (deliveryTag.channel === id) {
		    debug('acking!');
		    this.original_ack(deliveryTag);
		} else {
		    debug('message not from this socket, not acking');
		}
	    };
	    
	    self.amqpChannels[id] = ch;
	    fn.call(self);
	}).catch(function(error) {
	    console.log(error);
	    process.exit(1);
	});
    }
}

/**
 * Private method.  Given a channel, subscribe to a queue
 * we expect this method to be called with a context
 */
function subscribeToQueue(ch, id, room, options, roles) {
    var self = this;
    
    const opts = options || {durable: false};

    debug('creating queue %s with options', room);
    debug(opts);
    ch.assertQueue(room, opts);

    // We probably want to do some kind of lookup on the user id
    // to see which routing key to bind to for this channel
    // roles is a hash that maps id to a list of roles
    if (roles && roles[room]) {
	roles[room].forEach(function (role) {
	    // bind the role as a routingKey to the id queue
	    debug('binding %s to routingKey %s', room, role);
	    ch.bindQueue(room, defaults.direct_exchange_name, role);		
	});
    } 

    var alreadySubscribed = (self.amqpConsumers[id] ? self.amqpConsumers[id].filter(	
	tag => tag.room === room
    ) : []).length > 0;

    if (alreadySubscribed) {
	debug('%s already subscribed to %s', id, room);
	return;
    }

    ch.consume(room, msg => {
	channelConsumeCallback.call(self, id, room, msg);
    }, {}).then(function(ok) {
	debug('connection %s has consumerTag %s', id, ok.consumerTag);
	if (!self.amqpConsumers[id]) {
	    self.amqpConsumers[id] = [];
	}
	self.amqpConsumers[id].push({room: room, tag: ok.consumerTag});	
    });
}

function channelConsumeCallback(id, room, msg) {
    debug('Connection %s received message from queue', id);
    debug(msg);
    
    const args = msgpack.decode(msg.content);
    
    var rebroadcast = true;
    if (args.length == 1) {
	rebroadcast = false;
	// someone published to the queue directly, have to add some additional
	// data to make it a socket.io mesage
	args.push({
	    rooms: [room],
	    flags: undefined
	});
    };
    
    // add the delivery tag to allow ack
    const deliveryTag = {
	channel: id,
	fields: {
	    deliveryTag: msg.fields.deliveryTag
	}
    };
    args[0].data.push(deliveryTag);
    args.push(true);
    debug(args);

    // ack the message if the room does not require acknolwedgement
    // @todo, need to figure out how to indiciate a room requires
    //        ack, for now, if a room name contains 'ack', it requires it
    if (!room.includes('ack')) {
	debug('auto acking');
	this.amqpChannels[id].ack(deliveryTag);
    }
    
    // This is the actual delivery to client
    Adapter.prototype.broadcast.apply(this, args);

    // broadcast to global queue to notify other nodes
    if (rebroadcast) 
	this.amqpPublishChannel.publish(defaults.exchange_name, '', msgpack.encode([this.amqp_uuid, args[0], args[1]]));    
}
