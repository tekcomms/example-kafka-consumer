package com.netscout.bda.export;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Entry point for the example export consumer, providing CLI options to drive the behavior of the consumer.
 * This is a simple consumer example and is not intended to be the most efficient version of  a consumer, parser and CSV writer
 */
public class ExportConsumerExample {

    private static final Logger LOGGER = LoggerFactory.getLogger("NETSCOUT Example Consumer");

    /**
     * Entry point, parsing arguments and creating and starting the appropriate ExportConsumer
     * @param args
     */
    public static void main(final String[] args) {

        LOGGER.info("============================================================");
        LOGGER.info("                       Example Consumer");
        LOGGER.info("============================================================");

        try {
            CommandLineParser parser = new DefaultParser();
            Options options = ExportConsumerExample.getOptions();
            CommandLine cmd = parser.parse(options, args);

            if( cmd.hasOption("h") ) {
                printUsage(options);
                return;
            }

            String securityProtocol = getSecurityProtocol(cmd);
            boolean pulsarEnabled = isPulsarEnabled(cmd);
            ExportConsumer consumer = new ExportConsumer(getKafkaHosts(cmd), getGroup(cmd), getTopics(cmd), getPartitions(cmd),
                    getBasePath(cmd), getMaxRecords(cmd),  getDeserializer(cmd), getSchemaRegistryURL(cmd),
                    getSchemaRegistrySslEnabled(cmd), isAvroDeserializerType(cmd), securityProtocol, pulsarEnabled, isPulsarAvroEnabled(cmd),
                    isPulsarTlsEnabled(cmd), isPulsarTlsAuthEnabled(cmd), getPulsarThread(cmd));

            if( securityProtocol.equalsIgnoreCase("sasl_ssl") ) {
                consumer.withSaslSslSecurity(getService(cmd), getClientId(cmd), getTrustFile(cmd), getTrustPassword(cmd));
            } else if( securityProtocol.equalsIgnoreCase("sasl_plaintext") ) {
                consumer.withSaslSecurity(getClientId(cmd));
            } else if( securityProtocol.equalsIgnoreCase("ssl") ) {
                consumer.withSslSecurity(getClientId(cmd), getTrustFile(cmd), getTrustPassword(cmd), getKeyFile(cmd), getKeyPassword(cmd));
            }

            if (pulsarEnabled) {
                consumer.withPulsarSecurity(getPulsarTrustStore(cmd), getPulsarCert(cmd), getPulsarKeyStore(cmd));
            }

            if( cmd.hasOption("l") ) {
                consumer.listTopics();
            } else {
                consumer.start(getOffset(cmd));
            }
        } catch(ParseException ex) {
            LOGGER.error("Parse Exception: " + ex.getMessage());
        }

    }

    /**
     * Command line options
     * @return built Options containing all possible program arguments
     */
    private static Options getOptions() {
        Options options = new Options();
        options.addOption("k", "kafka", true, "Bootstrap Kafka hosts.  Defaults to 'localhost:9092'");
        options.addOption("g", "group", true, "Kafka consumer group. Defaults to 'export-example'");
        options.addOption("t", "topics", true, "Comma separated topics. Defaults to 'export'");
        options.addOption("ps", "partitions", true, "Comma separated partition numbers. Default to an empty list.");
        options.addOption("l", "list-topics", false, "List Kafka topics and exit");
        options.addOption("d", "deserializer", true, "Deserialization type (json|avro). Defaults to json");
        options.addOption("s", "service", true, "Kerberos service name for kafka. Defaults to 'kafka'");
        options.addOption("c", "client-id", true, "Kafka client id. Defaults to 'example_client'");
        options.addOption("sp", "security-protocol", true, "Kafka security protocal. Defaults to 'plaintext'");
        options.addOption("tf", "trust-file", true, "Full path to trust file. Defaults to '<home>/certs/nBA.truststore.jks");
        options.addOption("tp", "trust-password", true, "Trust file password.");
        options.addOption("kf", "key-file", true, "Full path to key file. Defaults to '<home>/certs/nBA.keystore.jks");
        options.addOption("kp", "key-password", true, "Key file password.");
        options.addOption("b", "base-path", true, "Base directory path for consumed records to be written in CSV format to. Defaults to /user/home/export/consumer");
        options.addOption("m", "max-records", true, "Max number of records to be written to each file.  Defaults to 1000");
        options.addOption("o", "offset", true, "Offset to start consumer from.  0 indicates beginning, -1 indicates end. All other values or no option starts at the consumer's true position");
        options.addOption("r", "registry", true, "URL of Schema Registry (only applicable when deserialization type is avro)");
        options.addOption("rs", "registry-ssl", true, "Schema Registry SSL security boolean flag. Defaults to false");
        options.addOption("p", "pulsar", true, "Is system targeted at Apache Pulsar. Defaults to false");
        options.addOption("ae", "pulsar-avro-enabled", true, "Is Pulsar Avro Enabled. Defaults to true");
        options.addOption("pa", "pulsar-tls-auth", true, "Is Pulsar TLS Authenticate enabled. Defaults to false");
        options.addOption("pe", "pulsar-tls-enabled", true, "Is Pulsar TLS enabled. Defaults to false");
        options.addOption("pc", "pulsar-cert", true, "Full path to Pulsar Certificate file. Defaults to null");
        options.addOption("pk", "pulsar-keystore", true, "Full path to Pulsar KeyStore file. Defaults to null");
        options.addOption("pt", "pulsar-truststore", true, "Full path to Pulsar TrustStore file. Defaults to null");
        options.addOption("pl", "pulsar-listener-threads", true, "Pular io thread, connection broker and listener threads. Default to 1");
        options.addOption("h", "help", false, "Print this message");

        return options;
    }

    /**
     * Get the Kafka hosts option if available, returning the default if not
     * @param cmd program command line arguments
     * @return Kafka hosts
     */
    private static String getKafkaHosts(final CommandLine cmd) {
        if( cmd.hasOption("k") ) {
            return cmd.getOptionValue("k");
        } else {
            return "localhost:9092";
        }
    }

    /**
     * Get the Kafka consumer group option if available, returning the default if not
     * @param cmd program command line arguments
     * @return Kafka consumer group
     */
    private static String getGroup(final CommandLine cmd) {
        if( cmd.hasOption("g") ) {
            return cmd.getOptionValue("g");
        } else {
            return "export-example";
        }
    }

    /**
     * Get the Kafka topics option if available, returning the default if not
     * @param cmd program command line arguments
     * @return Kafka topics
     */
    private static List getTopics(final CommandLine cmd) {
        if( cmd.hasOption("t") ) {
            return Arrays.asList(cmd.getOptionValue("t").split(","));
        } else {
            return Arrays.asList("export");
        }
    }

    /**
     * Get the Kafka partition option if available, returning the default if not
     * @param cmd program command line arguments
     * @return Kafka partitions
     */
    private static List getPartitions(final CommandLine cmd) {
        if( cmd.hasOption("ps") ) {
            return Arrays.asList(cmd.getOptionValue("ps").split(","));
        } else {
            return new ArrayList();
        }
    }

    /**
     * Get the deserialization type if available, returning json if not
     * @param cmd program command line arguments
     * @return deserialization class
     */
    private static Class<? extends Deserializer> getDeserializer(final CommandLine cmd) {
        Class<? extends Deserializer> clazz = JsonDeserializer.class;
        if( cmd.hasOption("d") ) {
            String deserializerType = cmd.getOptionValue("d");
            if (deserializerType.equalsIgnoreCase("kafka") || deserializerType.equalsIgnoreCase("json") ) {
                clazz = JsonDeserializer.class;
            } else if (deserializerType.equalsIgnoreCase("kafka-avro") || deserializerType.equalsIgnoreCase("avro") ) {
                clazz = AvroDeserializer.class;
            } else {
                LOGGER.warn("Unrecognized deserializer type '{}', defaulting to json", deserializerType);
            }
        }
        return clazz;
    }

    private static boolean isAvroDeserializerType(final CommandLine cmd) {
        Class<? extends Deserializer> clazz = JsonDeserializer.class;
        if( cmd.hasOption("d") ) {
            return cmd.getOptionValue("d").equalsIgnoreCase("avro");
        }
        return false;
    }

    private static boolean isPulsarEnabled(final CommandLine cmd) {
        if( cmd.hasOption("p") ) {
            return Boolean.valueOf(cmd.getOptionValue("p"));
        }
        return false;
    }

    private static boolean isPulsarAvroEnabled(final CommandLine cmd) {
        if( cmd.hasOption("ae") ) {
            return Boolean.valueOf(cmd.getOptionValue("ae"));
        }
        return true;
    }

    private static boolean isPulsarTlsEnabled(final CommandLine cmd) {
        if( cmd.hasOption("pe") ) {
            return Boolean.valueOf(cmd.getOptionValue("pe"));
        }
        return false;
    }

    private static boolean isPulsarTlsAuthEnabled(final CommandLine cmd) {
        if( cmd.hasOption("pa") ) {
            return Boolean.valueOf(cmd.getOptionValue("pa"));
        }
        return false;
    }

    private static String getPulsarCert(final CommandLine cmd) {
        if( cmd.hasOption("pc") ) {
            return cmd.getOptionValue("pc");
        }
        return null;
    }

    private static String getPulsarKeyStore(final CommandLine cmd) {
        if( cmd.hasOption("pk") ) {
            return cmd.getOptionValue("pk");
        }
        return null;
    }

    private static String getPulsarTrustStore(final CommandLine cmd) {
        if( cmd.hasOption("pt") ) {
            return cmd.getOptionValue("pt");
        }
        return null;
    }

    private static int getPulsarThread(final CommandLine cmd) {
        if( cmd.hasOption("pl") ) {
            return Integer.parseInt(cmd.getOptionValue("pl"));
        } else {
            //indicates default consumer behavior
            return 1;
        }
    }

    private static String getSchemaRegistryURL(CommandLine cmd) {
        if( cmd.hasOption("r") ) {
            if( isAvroDeserializerType(cmd) ) {
                return cmd.getOptionValue("r");
            } else {
                LOGGER.warn("Schema Registry specified for non-avro based consumer. Ignoring.");
            }
        }

        return null;
    }

    private static boolean getSchemaRegistrySslEnabled(CommandLine cmd) {
        if( cmd.hasOption("rs") ) {
            return Boolean.valueOf(cmd.getOptionValue("rs"));
        } else {
            return false;
        }
    }

    /**
     * Get the Kafka service option if available, returning the default if not
     * @param cmd program command line arguments
     * @return Kafka service
     */
    private static String getService(final CommandLine cmd) {
        if( cmd.hasOption("s") ) {
            return cmd.getOptionValue("s");
        } else {
            return "kafka";
        }
    }

    /**
     * Get the Kafka client id option if available, returning the default if not
     * @param cmd program command line arguments
     * @return Kafka client id
     */
    private static String getClientId(final CommandLine cmd) {
        if( cmd.hasOption("c") ) {
            return cmd.getOptionValue("c");
        } else {
            return "example_client";
        }
    }

    /**
     * Get the Security Protocol option if available, returning the default if not
     * @param cmd program command line arguments
     * @return Security Protocol type
     */
    private static String getSecurityProtocol(final CommandLine cmd) {
        if( cmd.hasOption("sp") ) {
            return cmd.getOptionValue("sp");
        } else {
            return "PLAINTEXT";
        }
    }

    /**
     * Get the SSL trust file option if available, returning the default if not
     * @param cmd program command line arguments
     * @return SSL trust file
     */
    private static String getTrustFile(final CommandLine cmd) {
        if( cmd.hasOption("tf") ) {
            return cmd.getOptionValue("tf");
        } else {
            return System.getProperty("user.home")+"/certs/nBA.truststore.jks";
        }
    }

    /**
     * Get the SSL trust file password option if available, returning the default if not
     * @param cmd program command line arguments
     * @return SSL trust file password
     */
    private static String getTrustPassword(final CommandLine cmd) {
        if( cmd.hasOption("tp") ) {
            return cmd.getOptionValue("tp");
        } else {
            return "";
        }
    }

    /**
     * Get the SSL key file option if available, returning the default if not
     * @param cmd program command line arguments
     * @return SSL key file
     */
    private static String getKeyFile(final CommandLine cmd) {
        if( cmd.hasOption("kf") ) {
            return cmd.getOptionValue("kf");
        } else {
            return System.getProperty("user.home")+"/certs/nBA.keystore.jks";
        }
    }

    /**
     * Get the SSL key file password option if available, returning the default if not
     * @param cmd program command line arguments
     * @return SSL key file password
     */
    private static String getKeyPassword(final CommandLine cmd) {
        if( cmd.hasOption("kp") ) {
            return cmd.getOptionValue("kp");
        } else {
            return "";
        }
    }

    /**
     * Get the CSV base path option if available, returning the default if not
     * @param cmd program command line arguments
     * @return CSV base path
     */
    private static String getBasePath(final CommandLine cmd) {
        if( cmd.hasOption("b") ) {
            return cmd.getOptionValue("b");
        } else {
            return System.getProperty("user.home") + "/export/consumer";
        }
    }

    /**
     * Get the CSV max records option if available, returning the default if not
     * @param cmd program command line arguments
     * @return CSV max records
     */
    private static int getMaxRecords(final CommandLine cmd) {
        if( cmd.hasOption("m") ) {
            return Integer.parseInt(cmd.getOptionValue("m"));
        } else {
            return 1000;
        }
    }


    /**
     * Get the offset option if available, returning the default if not
     * @param cmd program command line arguments
     * @return consumer offset
     */
    private static int getOffset(final CommandLine cmd) {
        if( cmd.hasOption("o") ) {
            return Integer.parseInt(cmd.getOptionValue("o"));
        } else {
            //indicates default consumer behavior
            return -99;
        }
    }

    /**
     * Print the help usage
     * @param options program options
     */
    private static void printUsage(final Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "java -jar [JARNAME]", options );
    }
}