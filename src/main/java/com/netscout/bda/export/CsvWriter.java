package com.netscout.bda.export;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * A CSV file writer that takes a map of key:values and outputs them to CSV files with headers.  The assumption is that each
 * CsvWriter maps to a single topic and the records on the topic are homogeneous, making it possible to generate the header
 * off of any given record.  The generated files can be found at <basePath>/<topic>/consumer_<topic>_<currentime_ms>.csv
 */
public class CsvWriter {

    private static final Logger logger = LoggerFactory.getLogger(CsvWriter.class);

    private String topic;
    private String basePath = System.getProperty("user.home") + "/export/consumer";
    private int maxRecords = 1000;
    private int currentRecords = 0;
    private CSVPrinter csvPrinter;
    /**
     * How many records to pretty print/message
     */
    private int drain = 1;
    private ObjectMapper mapper = new ObjectMapper();

    /**
     * Constructor for a specific topic, using defaults for basePath and maxRecords
     *
     * @param topic topic associated with this writer
     */
    public CsvWriter(final String topic) {
        this.topic = topic;
        init();
    }

    /**
     * Constructor for a specific topic, writing to the basePath and using the maxRecords per file
     *
     * @param topic      topic associated with this writer
     * @param basePath   base path to write CSV files to
     * @param maxRecords max number of records written to each CSV file
     */
    public CsvWriter(final String topic, final String basePath, final int maxRecords) {
        this.topic = topic;
        this.basePath = basePath;
        this.maxRecords = maxRecords;
        init();
    }

    private void init() {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                try {
                    if (csvPrinter != null) {
                        csvPrinter.flush();
                        csvPrinter.close();
                    }
                } catch (IOException e) {
                    logger.error("Problem closing file on exit: " + e.getMessage());
                }
            }
        }));
    }

    /**
     * Write method taking the map containing a single record's key:values and writing it to a file
     *
     * @param map byte array containing JSON data
     */
    public void write(Map<String, Object> map) {
        if (csvPrinter == null || currentRecords >= maxRecords) {
            csvPrinter = rotateFile();
        }

        writeCsv(map);
        currentRecords++;
    }

    /**
     * Converts the map to a CSV string, including a header if it is the first record and writes to the file.
     *
     * @param map map of key:values to be written in CSV
     */
    private void writeCsv(Map<String, Object> map) {
        try {
            if (map != null) {
                if (currentRecords == 0) {
                    csvPrinter.printRecord(map.keySet());
                    if (drain > 0) {
                        logger.info("=============================================");
                        logger.info(" Sample Record for topic '{}' : {}", topic, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(map));
                    }
                }
                if (currentRecords > 0 && currentRecords < drain && drain > 0) {
                    logger.info("{}", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(map));
                }
                csvPrinter.printRecord(map.values());
            } else {
                logger.info("CsvWriter: Skipping null map!");
            }
        } catch (IOException e) {
            logger.error("Error writing CSV {}", e.getMessage());
        }
    }

    /**
     * Create a new file and associated CSVPrinter, closing the existing one if open.
     *
     * @return CSVPrinter for the newly created file
     */
    private CSVPrinter rotateFile() {
        try {
            currentRecords = 0;
            if (csvPrinter != null) {
                logger.info("Closing out current file");
                csvPrinter.flush();
                csvPrinter.close();
            }

            String csvPath = basePath + "/" + topic + "/";
            Path path = Paths.get(csvPath);
            if (!Files.exists(path)) {
                Files.createDirectories(path);
            }

            File f = new File(csvPath + "consumer_" + topic + "_" + System.currentTimeMillis() + ".csv");
            logger.info("Writing to file: {}", f.getAbsolutePath());
            return new CSVPrinter(new FileWriter(f), CSVFormat.RFC4180.withQuoteMode(QuoteMode.MINIMAL));
        } catch (IOException e) {
            logger.error("Problem opening file: {}", e.getMessage());
            return null;
        }
    }

}
