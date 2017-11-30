package com.netscout.bda.export;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An AVRO deserializer to be used with Kafka.  Takes the AVRO byte data off of the topic and reads it into a LinkedHashMap
 */
public class AvroDeserializer implements Deserializer {

    private static final Logger logger = LoggerFactory.getLogger(AvroDeserializer.class);

    private static final Schema schema = new Schema.Parser().parse("{ \"type\":\"map\", \"values\": [\"string\", \"double\", \"long\", \"null\"] }");

    private final DatumReader<LinkedHashMap<String, Object>> reader = new SpecificDatumReader<>(schema);

    @Override
    public void configure(Map map, boolean b) {
        // empty
    }

    @Override
    public LinkedHashMap deserialize(String s, byte[] bytes) {
        try {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
            return reader.read(new LinkedHashMap<String, Object>(), decoder);
        } catch (Exception e) {
            logger.error("AVRO Error", e);
        }
        return null;
    }

    @Override
    public void close() {
        // empty
    }
}