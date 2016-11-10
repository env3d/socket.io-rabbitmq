'use strict';

/**
 * Module dependencies.
 */

const Adapter = require('socket.io-adapter'),
      amqplib = require('amqplib'),      
      debug = require('debug')('socket.io-rabbitmq'),
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
	    debug('Broadcasting to local node');			
	    args.push(true);
	    Adapter.prototype.broadcast.apply(self, args);
	}, {noAck: true});	
    }).catch(function(err){
	debug('Error creating connection');
	debug(err);
    })

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
	    self.amqpPublishChannel.assertQueue(room, {durable: false});
	    self.amqpPublishChannel.publish('', room, msgpack.encode([packet,opts]));	
	})
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

    this.amqpChannels[id] ? this.amqpChannels[id].close() : {} ;
    delete this.amqpChannels[id];
    delete this.amqpConsumers[id];
    
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
	    debug('no connection available at all, giving up');
	}
    } else {
	self.amqp_connection.createChannel().then(function(ch) {
	    self.amqpChannels[id] = ch;
	    debug(opts);
	    fn.call(self, ch, id, room, opts);	    
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
    
    // probably don't want to ack here, want client to send ack as
    // we are just a proxy here
    this.amqpChannels[id].ack(msg);

    var rebroadcast = true;
    if (args.length == 1) {
	rebroadcast = false;
	// someone published to the queue directly, have to add the opts
	args.push({
	    rooms: [room],
	    flags: undefined
	});
    }
    
    args.push(true);
    debug(args);
    
    Adapter.prototype.broadcast.apply(this, args);

    // broadcast to global queue to notify other nodes
    if (rebroadcast) 
	this.amqpPublishChannel.publish(defaults.exchange_name, '', msgpack.encode([this.amqp_uuid, args[0], args[1]]));    
}
