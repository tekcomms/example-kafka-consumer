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

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.util.Arrays;
import java.util.List;

/**
 * Entry point for the example export consumer, providing CLI options to drive the behavior of the consumer.
 * This is a simple consumer example and is not intended to be the most efficient version of  a consumer, parser and CSV writer
 */
public class ExportConsumerExample {

    private static final Logger logger = LoggerFactory.getLogger("NETSCOUT Example Consumer");

    /**
     * Entry point, parsing arguments and creating and starting the appropriate ExportConsumer
     * @param args
     */
    public static void main(final String[] args) {

        logger.info("============================================================");
        logger.info("                       Example Consumer");
        logger.info("============================================================");

        try {
            CommandLineParser parser = new DefaultParser();
            Options options = ExportConsumerExample.getOptions();
            CommandLine cmd = parser.parse(options, args);

            if( cmd.hasOption("h") ) {
                printUsage(options);
                return;
            }

            ExportConsumer consumer = new ExportConsumer(getKafkaHosts(cmd), getGroup(cmd), getTopics(cmd), getBasePath(cmd), getMaxRecords(cmd),  getDeserializer(cmd));

            if( isNotBlank(getTrustPassword(cmd)) ) {
                consumer.withSecurity(getService(cmd), getClientId(cmd), getTrustFile(cmd), getTrustPassword(cmd));
            }

            if( cmd.hasOption("l") ) {
                consumer.listTopics();
            } else {
                consumer.start(getOffset(cmd));
            }

            printUsage(options);

        } catch(ParseException ex) {
            logger.error ("Parse Exception: " + ex.getMessage());
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
        options.addOption("l", "list-topics", false, "List Kafka topics and exit");
        options.addOption("d", "deserializer", true, "Deserialization type (json|avro). Defaults to json");
        options.addOption("s", "service", true, "Kerberos service name for kafka. Defaults to 'kafka'");
        options.addOption("c", "client-id", true, "Kafka client id. Defaults to 'example_client'");
        options.addOption("f", "trust-file", true, "Full path to trust file. Defaults to '<home>/certs/nBA.truststore.jks");
        options.addOption("p", "trust-password", true, "Trust file password. Presence of password enables security feature");
        options.addOption("b", "base-path", true, "Base directory path for consumed records to be written in CSV format to. Defaults to /user/home/export/consumer");
        options.addOption("m", "max-records", true, "Max number of records to be written to each file.  Defaults to 1000");
        options.addOption("o", "offset", true, "Offset to start consumer from.  0 indicates beginning, -1 indicates end. All other values or no option starts at the consumer's true position");
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
     * Get the deserialization type if available, returning json if not
     * @param cmd program command line arguments
     * @return deserialization class
     */
    private static Class<? extends Deserializer> getDeserializer(final CommandLine cmd) {
        Class<? extends Deserializer> clazz = JsonDeserializer.class;
        if( cmd.hasOption("d") ) {
            String deserializerType = cmd.getOptionValue("d");
            if (deserializerType.equalsIgnoreCase("json")) {
                clazz = JsonDeserializer.class;
            } else if (deserializerType.equalsIgnoreCase("avro")) {
                clazz = AvroDeserializer.class;
            } else {
                logger.warn("Unrecognized deserializer type '{}', defaulting to json", deserializerType);
            }
        }
        return clazz;
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
     * Get the SSL trust file option if available, returning the default if not
     * @param cmd program command line arguments
     * @return SSL trust file
     */
    private static String getTrustFile(final CommandLine cmd) {
        if( cmd.hasOption("f") ) {
            return cmd.getOptionValue("f");
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
        if( cmd.hasOption("p") ) {
            return cmd.getOptionValue("p");
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