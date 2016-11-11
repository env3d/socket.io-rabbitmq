# socket.io-rabbitmq

A Socket.IO Adapter for use with RabbitMQ, written from scratch but insipired by https://github.com/sensibill/socket.io-amqp.
This adapter has the following semantics:

- Each connection is modelled as an autoDelete queue
- Each room is modeled as a persistent queue
- We can bind routing keys to a room, allowing for "role" type behavior

[![Build Status](https://api.travis-ci.org/env3d/socket.io-rabbitmq.svg?branch=master)](https://travis-ci.org/env3d/socket.io-rabbitmq)

## How to use

```js
var io = require('socket.io')(3000);
var amqp_adapter = require('socket.io-rabbitmq');
io.adapter(amqp_adapter('amqp://localhost'));
```
## API

### adapter(uri, [{roles}])

`uri` is a string like `amqp://localhost` which points to your AMQP / RabbitMQ server.
The amqp:// scheme is MANDATORY. If you need to use a username & password, they must
be embedded in the URI.

The second parameter, roles, is used to attach routing keys to a room:

```js
io.adapter(rabbitmq_adapter('amqp://localhost', {john: ['supervisor'], joe: ['supervisor', 'worker']})
```

If we send a message to rabbitmq with 'supervisor' as a routing key, the message will be delivered to
both john and joe.