package com.stratio.ingestion.sink.rabbitmq;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitMQSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMQSink.class);

    private static final String DEFAULT_EXCHANGE = "amq.topic";
    private static final String EMPTY_EXCHANGE   = "";
    private static final String DEFAULT_ROUTING  = "";
    private static final String DEFAULT_QUEUE    = "";

    private static final String HOST_TAG        = "host";
    private static final String PORT_TAG        = "port";
    private static final String EXCHANGE_TAG    = "exchange";
    private static final String QUEUE_TAG       = "queue";
    private static final String ROUTING_TAG     = "routing-key";
    private static final String USER_TAG        = "user";
    private static final String PASSWORD_TAG    = "password";
    private static final String SSL_TAG         = "ssl";
    private static final String VIRTUAL_HOST_TAG   = "virtual-host";
    private static final String BASIC_PROPERTIES_TAG    = "basic-properties";
    private static final String CUSTOM_PROPERTIES_TAG   = "custom-properties";
    private static final String MANDATORY_TAG    = "mandatory";
    private static final String PUBLISHER_CONFIRMS_TAG  = "publisher-confirms";
    private static final String CONF_BATCH_SIZE = "batchSize";

    private String queue;
    private String exchange;
    private String routingKey;
    private Boolean customProperties;
    private Boolean basicProperties;
    private Boolean mandatory;
    private Boolean publisherConfirms;
    private int batchsize;
    private String  host;
    private Integer port;
    private boolean sslEnabled = false;
    private String virtualHost;
    private String user;
    private String password;
    private Channel rmqChannel = null;
    private Connection connection = null;
    private ConnectionFactory factory;
    private SinkCounter sinkCounter;

    private static final String CUSTOM_PROP  = "custom_";
    private static final String APPID_PROP   = "appId";
    private static final String CONTENTENCODING_PROP = "contentEncoding";
    private static final String CLUSTERID_PROP   = "clusterId";
    private static final String CONTENTTYPE_PROP     = "contentType";
    private static final String DELIVERYMODE_PROP    = "deliveryMode";
    private static final String EXPIRES_PROP         = "expires";
    private static final String CORRELATIONID_PROP   = "correlationId";
    private static final String MESSAGEID_PROP = "messageId";
    private static final String PRIORITY_PROP  = "priority";
    private static final String REPLYTO_PROP   = "replyTo";
    private static final String TIMESTAMP_PROP = "timestamp";
    private static final String TYPE_PROP      = "type";
    private static final String USERID_PROP    = "userId";

    private static BasicProperties.Builder builder;
    public RabbitMQSink() {
        this(new ConnectionFactory());
    }

    public RabbitMQSink(ConnectionFactory factory) {
        this.factory = factory;
    }

    @Override
    public void configure(Context context) {
        this.sinkCounter = new SinkCounter(this.getName());

        host = context.getString(HOST_TAG, ConnectionFactory.DEFAULT_HOST);
        port = context.getInteger(PORT_TAG, ConnectionFactory.DEFAULT_AMQP_PORT);
        user = context.getString(USER_TAG, ConnectionFactory.DEFAULT_USER);
        password = context.getString(PASSWORD_TAG, ConnectionFactory.DEFAULT_PASS);
        exchange = context.getString(EXCHANGE_TAG, DEFAULT_EXCHANGE);
        routingKey = context.getString(ROUTING_TAG, DEFAULT_ROUTING);
        queue =  context.getString(QUEUE_TAG, DEFAULT_QUEUE);
        sslEnabled = context.getBoolean(SSL_TAG, false);
        virtualHost = context.getString(VIRTUAL_HOST_TAG, ConnectionFactory.DEFAULT_VHOST);
        basicProperties = context.getBoolean(BASIC_PROPERTIES_TAG, true);
        customProperties= context.getBoolean(CUSTOM_PROPERTIES_TAG, true);
        mandatory = context.getBoolean(MANDATORY_TAG, false);
        publisherConfirms = context.getBoolean(PUBLISHER_CONFIRMS_TAG, false);
        batchsize =  context.getInteger(CONF_BATCH_SIZE, 1);
    }

    @Override
    public Status process() throws EventDeliveryException {

        Status status = Status.BACKOFF;

        if (connection == null) {
            this.sinkCounter.start();

            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(user);
            factory.setPassword(password);
            factory.setVirtualHost(virtualHost);
            logger.debug("Connecting to RabbitMQ "+host+":"+port+" "+virtualHost+" - "+user+":"+password);

            if (sslEnabled) {
                try {
                    factory.useSslProtocol();
                } catch (NoSuchAlgorithmException ex) {
                    this.sinkCounter.getConnectionFailedCount();
                    logger.error("Could not enable SSL: {}", ex.toString());
                    throw new EventDeliveryException("Could not Enable SSL: " + ex.toString());
                } catch (KeyManagementException ex) {
                    this.sinkCounter.getConnectionFailedCount();
                    logger.error("Could not enable SSL: {}", ex.toString());
                    throw new EventDeliveryException("Could not Enable SSL: " + ex.toString());
                }
            }
            try {
                connection = factory.newConnection();

                rmqChannel = connection.createChannel();

                if (publisherConfirms) {
                    rmqChannel.confirmSelect();
                }

            } catch (IOException ex) {
                ex.printStackTrace();
                logger.error(ex.getMessage() + " ------ " + ex.getCause());
                this.sinkCounter.getConnectionFailedCount();
                throw new EventDeliveryException(ex.toString());
            }

        }

        Transaction transaction = getChannel().getTransaction();

        try {
            transaction.begin();

            List<Event> eventList = this.takeEventsFromChannel(getChannel());
            status = Status.READY;
            if (!eventList.isEmpty()) {
                if (eventList.size() == this.batchsize) {
                    this.sinkCounter.incrementBatchCompleteCount();
                } else {
                    this.sinkCounter.incrementBatchUnderflowCount();
                }
                for (Event event : eventList) {
                    if (event == null) {
                        this.sinkCounter.getEventDrainAttemptCount();
                        status = Status.BACKOFF;
                    } else {

                        publishRabbitMQMessage(event);
                        status = Status.READY;
                    }
                }
                this.sinkCounter.addToEventDrainSuccessCount(eventList.size());
            } else {
                this.sinkCounter.incrementBatchEmptyCount();
            }
            transaction.commit();
            status = Status.READY;

        } catch (ChannelException ex) {
            this.sinkCounter.getConnectionFailedCount();
            transaction.rollback();
            status = Status.BACKOFF;
            logger.error("Unable to get event from channel. Exception follows.", ex);

        } catch (EventDeliveryException ex) {
            this.sinkCounter.getConnectionFailedCount();
            transaction.rollback();
            status = Status.BACKOFF;
            logger.error("Delivery exception: {}", ex);
            this.sinkCounter.incrementConnectionFailedCount();
        } finally {
            transaction.close();
        }

        return status;
    }

    private List<Event> takeEventsFromChannel(org.apache.flume.Channel channel) {
        List<Event> events = new ArrayList<Event>();
        for (int i = 0; i < this.batchsize; i++) {
            this.sinkCounter.incrementEventDrainAttemptCount();
            events.add(channel.take());
        }
        events.removeAll(Collections.singleton(null));
        return events;
    }

    private void publishRabbitMQMessage(Event event)throws EventDeliveryException {

            Map<String, String> headers = event.getHeaders();

            // Use a headers supplied routing key if it exists
            if (headers.containsKey(ROUTING_TAG)) {
                routingKey = headers.get(ROUTING_TAG);
            } else if(!queue.equals(DEFAULT_QUEUE) && exchange.equals(DEFAULT_EXCHANGE)){
                routingKey = queue;
                exchange   = EMPTY_EXCHANGE;
            }
            try {
                rmqChannel.basicPublish(exchange, routingKey, mandatory, getPropertiesFromEvent(headers), event.getBody());
            } catch (IOException ex) {
                logger.error("Error publishing event message: {}", ex.toString());
                closeRabbitMQConnection();
                throw new EventDeliveryException(ex.toString());
            }

            if (publisherConfirms) waitForConfirmation();
    }

    public BasicProperties getPropertiesFromEvent(Map<String, String> headers){

        builder = new BasicProperties.Builder();

        if(basicProperties){
            builder = checkBasicProperties(headers);
        }else if(customProperties){
            builder = checkCustomProperties(headers);
        }

        return builder.build();
    }

    private BasicProperties.Builder checkCustomProperties(Map<String, String> headers) {

        Map<String,Object> customHeaders = new HashMap<String, Object>() ;
        if (headers!=null && headers.size()>0) {
            for(String head :headers.keySet()){
                if (head.contains(CUSTOM_PROP)) {
                    customHeaders.put(head.replaceFirst(CUSTOM_PROP, ""),headers.get(head));
                }
            }
            builder.headers(customHeaders);
        }
        return builder;
    }

    private BasicProperties.Builder checkBasicProperties(Map<String, String> headers) {

        if (headers!=null && headers.size()>0) {

            if (headers.containsKey(APPID_PROP)) {
                builder.appId(headers.get(APPID_PROP));
            }
            if (headers.containsKey(USERID_PROP)) {
                builder.userId(headers.get(USERID_PROP));
            }
            if (headers.containsKey(CLUSTERID_PROP)) {
                builder.contentEncoding(headers.get(CLUSTERID_PROP));
            }

            if (headers.containsKey(CONTENTENCODING_PROP)) {
                builder.contentEncoding(headers.get(CONTENTENCODING_PROP));
            }

            if (headers.containsKey(CONTENTTYPE_PROP)) {
                builder.contentType(headers.get(CONTENTTYPE_PROP));
            }

            if (headers.containsKey(CORRELATIONID_PROP)) {
                builder.correlationId(headers.get(CORRELATIONID_PROP));
            }

            if (headers.containsKey(DELIVERYMODE_PROP)) {
                builder.deliveryMode(Integer.parseInt(headers.get(DELIVERYMODE_PROP)));
            }

            if (headers.containsKey(EXPIRES_PROP)) {
                builder.expiration(headers.get(EXPIRES_PROP));
            }

            if (headers.containsKey(MESSAGEID_PROP)) {
                builder.messageId(headers.get(MESSAGEID_PROP));
            }

            if (headers.containsKey(PRIORITY_PROP)) {
                builder.priority(Integer.parseInt(headers.get(PRIORITY_PROP)));
            }

            if (headers.containsKey(REPLYTO_PROP)) {
                builder.replyTo(headers.get(REPLYTO_PROP));
            }

            if (headers.containsKey(TIMESTAMP_PROP)) {
                builder.timestamp(new Date(Long.parseLong(headers.get(TIMESTAMP_PROP))));
            }

            if (headers.containsKey(TYPE_PROP)) {
                builder.type(headers.get(TYPE_PROP));
            }

        }
        return builder;
    }

    private void closeRabbitMQConnection() {
        if (rmqChannel != null) {
            try {
                rmqChannel.close();
            } catch (IOException ex) {
                logger.error("Could not close the RabbitMQ Channel: {}", ex.toString());
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException ex) {
                logger.error("Could not close the RabbitMQ Connection: {}", ex.toString());
            }
        }
        rmqChannel = null;
        connection = null;

    }

    private void waitForConfirmation() throws EventDeliveryException {
        try {
            rmqChannel.waitForConfirms();
        } catch (InterruptedException ex) {
            logger.error("Error waiting for publisher confirmation: {}", ex.toString());
            closeRabbitMQConnection();
            throw new EventDeliveryException(ex.toString());
        }
    }

}
