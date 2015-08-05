Stratio RabbitMQ Sink
====================

RabbitMQ Sink. Send events to RabbitMQ.

Configuration
=============

The available config parameters are:

- `host` *(string, required)*: The host to rabbitMQ

- `port` *(string, required)*: The port to rabbitMQ

- `user` *(string, required)*: A valid RabbitMQ username.

- `password` *(string, required)*: Password.

- `exchange` *(string)*: A exchange name, you can use this directly instead of queue

- `routing-key` *(string)*: A routing key to an exchange

- `queue` *(string)*: A queue name, you can use this directly instead of exchange

- `virtual-host` *(string)*: By default /

- `basic-properties` *(boolean)* : Use to Mapping Basic AMQP properties

- `custom-properties` *(boolean)*: Use to Mapping Custom message properties

- `batchSize` *(integer)*: Number of events that will be grouped in the same query and transaction. Defaults to 20.

Table

Variable           | Default       | Description
------------------ | ------------- | -----------
host               | ``localhost`` | The host RabbitMQ
port               | ``5672``      | The port RabbitMQ
user               | ``guest``     | The username RabbitMQ
password           | ``guest``     | The password RabbitMQ
exchange           | ``amq.topic`` | The exchange to publish the message **
queue              | ``queue``     | The queue to publish the message **
routing-key        | ````          | The routing key to use when publishing
virtual-host       | ``/``         | The virtual host name to connect to
basic-properties   | ``true``      | Mapping the basic AMQP message properties in the event headers
custom-properties  | ``true``      | Add custom message properties, Add to th header event with "custom_" prefix
mandatory          | ``false``     | Enable mandatory publishing
publisher-confirms | ``false``     | Enable publisher confirmations
ssl                | ``false``     | Connect to RabbitMQ via SSL

Sample Complete-flow Flume config
=================================

The following file describes an example configuration of an Flume agent that uses a [Spooling directory source](http://flume.apache.org/FlumeUserGuide.html#spooling-directory-source), a [File channel](http://flume.apache.org/FlumeUserGuide.html#file-channel) and our Kafka Sink

``` 
    # Name the components on this agent
    agent.sources = r1
    agent.sinks = RabbitSink
    agent.channels = c1

    # Describe/configure the source
    agent.sources.r1.type = spoolDir
    agent.sources.r1.spoolDir = /home/flume/data/files/

    # Describe the sink
    agent.sinks.RabbitSink.channels = c1
    agent.sinks.RabbitSink.type = com.stratio.ingestion.sink.rabbitmq.RabbitMQSink
    agent.sinks.RabbitSink.host = localhost
    agent.sinks.RabbitSink.port = 5672
    agent.sinks.RabbitSink.user     = guest
    agent.sinks.RabbitSink.password = guest
    agent.sinks.RabbitSink.virtual-host = /
    agent.sinks.RabbitSink.exchange = exchange
    agent.sinks.RabbitSink.routing-key = routing-key
    #agent.sinks.RabbitSink.queue   = queue
    agent.sinks.RabbitSink.publisher-confirms = true
    agent.sinks.RabbitSink.batchSize = 100

    # Use a channel which buffers events in file
    agent.channels.c1.type = file
    agent.channels.c1.checkpointDir = /home/user/flume/channel/check/
    agent.channels.c1.dataDirs = /home/user/flume/channel/data/
    agent.channels.c1.transactionCapacity=10000

    # Bind the source and sink to the channel
    agent.sources.r1.channels = c1
    agent.sinks.RabbitSink.channel = c1
```