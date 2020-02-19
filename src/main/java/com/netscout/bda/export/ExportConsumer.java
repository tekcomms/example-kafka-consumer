package com.netscout.bda.export;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;

import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;

/**
 * Example consumer to use for reference.  The consumer supports both secured & non-secured Kafka clusters and can consume
 * from one or more Kafka topics.  This is a simple example consumer and no emphasis was put on efficiency.
 */
public class ExportConsumer {

    private static final int FROM_BEGINNING_OFFSET = 0;
    private static final int FROM_END_OFFSET = -1;
    public static final String TLS_CERT_FILE = "tlsCertFile:";
    public static final String TLS_KEY_FILE = "tlsKeyFile:";
    private static final Logger LOGGER = LoggerFactory.getLogger(ExportConsumer.class);
    private final boolean useAvroSerialization;
    private String useSchemaRegistry = null;
    private final boolean usePulsar;
    private boolean pulsarAvroEnabled;
    private final boolean tlsEnabled;
    private final boolean tlsAuthEnabled;
    private boolean schemaRegistrySslEnabled;

    private String bootstrapServers;
    private String consumerGroup;
    private String kafkaServiceName;
    private String kafkaClientId;
    private String trustPassword;
    private String trustFileLocation;
    private String keyPassword;
    private String keyFileLocation;
    private String securityProtocol;
    private String pulsarTrustFile;
    private String pulsarKeyFile;
    private String pulsarCertFile;
    private int pulsarThread = 1;
    private List<String> topics = new ArrayList<String>();
    private List<String> partitions = new ArrayList<>();

    private String basePath;
    private int maxRecords;
    private Map<String, CsvWriter> writers = new HashMap<String, CsvWriter>();
    private ObjectMapper mapper = new ObjectMapper();
    private Class<? extends Deserializer> deserializer;

    private String pulsarTLSAuthenticationClass = "org.apache.pulsar.client.impl.auth.AuthenticationTls";

    /**
     * Constructs an ExportConsumer for a specific set of kafka bootstrap server, consumer group and list of topics
     *
     * @param bootstrapServers Kafka bootstrap servers to connect to
     * @param consumerGroup    Kafka consumer group to use
     * @param topics           Kafka topics to consume from
     * @param basePath         full path to the directory for CSVs to be written to
     * @param maxRecords       max number of records written to each CSV file
     * @param usePulsar
     */
    public ExportConsumer(final String bootstrapServers, final String consumerGroup, final List topics, final List partitions,
                          final String basePath, final int maxRecords, final Class<? extends Deserializer> deserializer,
                          final String useSchemaRegistry, final boolean schemaRegistrySslEnabled,
                          final boolean useAvroSerialization, final String securityProtocol, final boolean usePulsar,
                          final boolean pulsarAvroEnabled, final boolean tlsEnabled, final boolean tlsAuthEnabled, final int pulsarThread) {
        this.bootstrapServers = bootstrapServers;
        this.consumerGroup = consumerGroup;
        this.trustPassword = "";
        this.topics = topics;
        this.partitions = partitions;
        this.basePath = basePath;
        this.maxRecords = maxRecords;
        this.deserializer = deserializer;
        this.useSchemaRegistry = useSchemaRegistry;
        this.schemaRegistrySslEnabled = schemaRegistrySslEnabled;
        this.useAvroSerialization = useAvroSerialization;
        this.securityProtocol = securityProtocol;
        this.usePulsar = usePulsar;
        this.pulsarAvroEnabled = pulsarAvroEnabled;
        this.tlsEnabled = tlsEnabled;
        this.tlsAuthEnabled = tlsAuthEnabled;
        this.pulsarThread = pulsarThread;
    }

    /**
     * Enables SASL_SSL security for the consumer
     *
     * @param kafkaServiceName  Kerberos service name used by the kafka cluster
     * @param kafkaClientId     Kafka client id
     * @param trustFileLocation Location of SSL truststore file
     * @param trustPassword     Password for SSL truststore file
     */
    public void withSaslSslSecurity(final String kafkaServiceName, final String kafkaClientId, final String trustFileLocation, final String trustPassword) {
        this.kafkaServiceName = kafkaServiceName;
        this.kafkaClientId = kafkaClientId;
        this.trustFileLocation = trustFileLocation;
        this.trustPassword = trustPassword;
    }
    
    /**
     * Enables SASL security for the consumer
     *
     * @param kafkaClientId     Kafka client id
     */
    public void withSaslSecurity(final String kafkaClientId) {
        this.kafkaClientId = kafkaClientId;
    }

    /**
     * Enables SSL security for the consumer
     *
     * @param kafkaClientId     Kafka client id
     * @param trustFileLocation Location of SSL truststore file
     * @param trustPassword     Password for SSL truststore file
     * @param keyFileLocation Location of SSL keystore file
     * @param keyPassword     Password for SSL keystore file
     */
    public void withSslSecurity(final String kafkaClientId, final String trustFileLocation, final String trustPassword, final String keyFileLocation, final String keyPassword) {
        this.kafkaClientId = kafkaClientId;
        this.trustFileLocation = trustFileLocation;
        this.trustPassword = trustPassword;
        this.keyFileLocation = keyFileLocation;
        this.keyPassword = keyPassword;
    }

    /**
     * Enables Pulsar security for the consumer
     *
     * @param pulsarTrustFile  Location of TLS truststore file
     * @param pulsarCertFile   Location of TLS Certificate file
     * @param pulsarKeyFile    Location of TLS Keystore file
     */
    public void withPulsarSecurity(final String pulsarTrustFile, final String pulsarCertFile, final String pulsarKeyFile) {
        this.pulsarTrustFile = pulsarTrustFile;
        this.pulsarCertFile = pulsarCertFile;
        this.pulsarKeyFile = pulsarKeyFile;
    }

    public void adjustOffset(final KafkaConsumer consumer, final int offset) {
        if( offset == FROM_BEGINNING_OFFSET || offset == FROM_END_OFFSET) {
            consumer.subscribe(topics, new ConsumerRebalanceListener() {
                //Noop needed to satisfy the API
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    LOGGER.info("{} topic-partitions are revoked from this consumer\n", Arrays.toString(partitions.toArray()));
                }

                //On any partitions assigned to this consumer, seek to the correct position
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    if(offset == FROM_BEGINNING_OFFSET){
                        LOGGER.info("Setting offset to beginning for topic partition: {}", partitions);
                        consumer.seekToBeginning(partitions);
                    }else if(offset == FROM_END_OFFSET){
                        LOGGER.info("Setting it to the end for topic partition: {}", partitions);
                        consumer.seekToEnd(partitions);
                    }
                }
            });
        }
    }

    private void adjustOffset(ConsumerBuilder<?> consumerBuilder, final int offset) {
        if (offset == FROM_BEGINNING_OFFSET) {
            consumerBuilder.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
        } else if (offset == FROM_END_OFFSET) {
            consumerBuilder.subscriptionInitialPosition(SubscriptionInitialPosition.Latest);
        }
    }

    /**
     * Starts the consumer and will run continuously polling the configured topics
     */
    public void start(int offset) {
        if (usePulsar) {
            Set<Consumer<?>> consumers = new HashSet<>();
            PulsarClient pulsarClient = null;

            try {
                topics.forEach(topic -> {
                    try {
                        final ConsumerBuilder consumerBuilder;
                        if (pulsarAvroEnabled) {
                            consumerBuilder = getPulsarClient().newConsumer(Schema.AUTO_CONSUME());
                        } else {
                            consumerBuilder = getPulsarClient().newConsumer();
                        }
                        adjustOffset(consumerBuilder, offset);
                        consumerBuilder.subscriptionName(topic);
                        consumers.add(consumerBuilder.topic(topic).consumerName("consumer_" + topic).subscribe());
                        LOGGER.info("Successed create consumer for topic {}", topic);
                    } catch (PulsarClientException e) {
                        LOGGER.error("Failed to create consumer for topic {}", topic, e);
                    }
                });

                while (true) {
                    consumers.parallelStream().forEach(con -> {
                        Message msg = null;
                        try {
                            msg = con.receive(5, TimeUnit.SECONDS);

                            if (msg != null) {
                                if (!writers.containsKey(msg.getTopicName())) {
                                    String topicName = msg.getTopicName();
                                    String outputTopicName = topicName.substring(topicName.lastIndexOf("//") + 2).replace("/", ".");
                                    writers.put(topicName, new CsvWriter(outputTopicName, basePath, maxRecords));
                                }

                                if (pulsarAvroEnabled) {
                                    writers.get(msg.getTopicName()).write((org.apache.pulsar.client.api.schema.GenericRecord) msg.getValue());
                                } else {
                                    ObjectMapper mapper = new ObjectMapper();
                                    writers.get(msg.getTopicName()).write(mapper.readValue(msg.getData(), HashMap.class));
                                }
                                // Acknowledge the message so that it can be deleted by the message broker
                                con.acknowledge(msg);
                            }
                        } catch (Exception e) {
                            // Message failed to process, redeliver later
                            LOGGER.error(e.getMessage(), e);
                            con.negativeAcknowledge(msg);
                        }
                    });
                }
            } catch (Exception pce) {
                consumers.forEach(consumer -> consumer.redeliverUnacknowledgedMessages());
                LOGGER.error("Error with Pulsar Consumer", pce);
            } finally {
                try {
                    if (pulsarClient != null) {
                        pulsarClient.close();
                    }
                    consumers.forEach(consumer -> {
                        try {
                            consumer.unsubscribe();
                            consumer.close();
                        } catch (PulsarClientException pe) {
                            LOGGER.error("Error closing Pulsar Consumer", pe);
                        }
                    });
                } catch (PulsarClientException e) {
                    LOGGER.error("Error closing Pulsar Consumer", e);
                }
            }
        } else if( useAvroSerialization ) {

            KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(getConsumerProperties());
            configConsumer(consumer);

            adjustOffset(consumer, offset);
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(5000);
                if (records != null) {
                    for (ConsumerRecord<String, GenericRecord> rec : records) {
                        if (!writers.containsKey(rec.topic())) {
                            writers.put(rec.topic(), new CsvWriter(rec.topic(), basePath, maxRecords));
                        }

                        writers.get(rec.topic()).write(rec.value());
                    }
                }
            }
        } else {
            KafkaConsumer<String, Map<String, Object>> consumer = new KafkaConsumer<>(getConsumerProperties());
            configConsumer(consumer);

            adjustOffset(consumer, offset);
            while (true) {
                ConsumerRecords<String, Map<String, Object>> records = consumer.poll(10000);
                for (ConsumerRecord<String, Map<String, Object>> rec : records) {
                    if (!writers.containsKey(rec.topic())) {
                        writers.put(rec.topic(), new CsvWriter(rec.topic().replace(":", "").replace("//", ""), basePath, maxRecords));
                    }

                    writers.get(rec.topic()).write(rec.value());
                }
            }
        }
    }

    private void configConsumer(KafkaConsumer consumer) {
        if (partitions.isEmpty()) {
            consumer.subscribe(topics);
        } else {
            consumer.assign(getTopicPartitions());
        }
    }

    private List<TopicPartition> getTopicPartitions() {
        List<TopicPartition>  topicPartitions = new ArrayList<>();
        topics.forEach(topic ->
                partitions.forEach(partition ->
                        topicPartitions.add(new TopicPartition(topic, Integer.valueOf(partition)))
                )
        );

        return topicPartitions;
    }

    /**
     * List the topics the user has permission to see on the Kafka broker
     */
    public void listTopics() {
        KafkaConsumer<String, Map<String, Object>> consumer = new KafkaConsumer<String, Map<String, Object>>(getConsumerProperties());
        try {
            Map<String, List<PartitionInfo>> topics = consumer.listTopics();
            LOGGER.info("Topics: ");
            for (String topic : topics.keySet()) {
                LOGGER.info("\t" + topic);
            }
        } catch (Exception e) {
            LOGGER.error("Error listing topics. ", e);
        }
    }

    /**
     * Builds the appropriate Kafka consumer properties
     *
     * @return Properties containing appropriate settings for the Kafka consumer
     */
    private Properties getConsumerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", consumerGroup);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "latest");
        if( useSchemaRegistry == null) {
            // normal
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", deserializer.getName());
        } else {
            props.put("key.deserializer", LongDeserializer.class.getName());
            // Use Kafka Avro Deserializer.
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
            // Use Specific Record or else you get Avro GenericRecord.
//            props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
            // Schema registry location.
            props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, useSchemaRegistry);
            if(schemaRegistrySslEnabled) {
                HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
                    @Override
                    public boolean verify(String s, SSLSession sslSession) {
                        return true;
                    }
                });
            }
        }

        if (!securityProtocol.equalsIgnoreCase("plaintext")) {
            addSecurity(props);
        }

        return props;
    }

    /**
     * Add SASL_SSL security settings to the input properties
     *
     * @param props Properties to add security parameters to
     */
    private void addSecurity(Properties props) {
        props.setProperty("security.protocol", securityProtocol.toUpperCase());
        props.setProperty("client.id", kafkaClientId);


        if (!securityProtocol.equalsIgnoreCase("plaintext")) {
            if (securityProtocol.toLowerCase().contains("sasl")) {
                String saslMechanism = System.getProperty("kafka_sasl_mechanism", "GSSAPI").trim();
                props.setProperty("sasl.mechanism", saslMechanism);
                if (saslMechanism.equalsIgnoreCase("GSSAPI")) {
                    props.setProperty("sasl.kerberos.service.name", kafkaServiceName);
                }
            }
            if (securityProtocol.toLowerCase().contains("ssl")) {
                props.setProperty("ssl.truststore.location", trustFileLocation);
                props.setProperty("ssl.truststore.password", trustPassword);
            }

            if (securityProtocol.equalsIgnoreCase("ssl") && keyPassword !=null && !keyPassword.isEmpty()) {
                props.setProperty("ssl.keystore.location", keyFileLocation);
                props.setProperty("ssl.keystore.password", keyPassword);
                props.setProperty("ssl.key.password", keyPassword);
            }
        }
    }

    private PulsarClient getPulsarClient() {
        PulsarClient pulsarClient = null;
        try {
            ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(bootstrapServers);
            if (tlsEnabled) {
                clientBuilder.enableTls(true);
                clientBuilder.tlsTrustCertsFilePath(pulsarTrustFile);
                if (tlsAuthEnabled) {
                    StringJoiner joiner = new StringJoiner(",");
                    joiner.add(TLS_CERT_FILE + pulsarCertFile).add(TLS_KEY_FILE + pulsarKeyFile);
                    clientBuilder.authentication(pulsarTLSAuthenticationClass, joiner.toString());
                }
            }
            if (pulsarThread > 1) {
                clientBuilder.listenerThreads(pulsarThread);
                clientBuilder.ioThreads(pulsarThread);
                clientBuilder.listenerThreads(pulsarThread);
            }
            pulsarClient = clientBuilder.build();
        } catch (Exception e) {
            LOGGER.error("Error during create Pulsar client", e);
        }
        return pulsarClient;
    }
}