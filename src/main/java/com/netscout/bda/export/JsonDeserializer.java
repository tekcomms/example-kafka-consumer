package com.netscout.bda.export;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;

/**
 * An JSON deserializer to be used with Kafka.  Takes the JSON byte data off of the topic and reads it into a Map
 */
public class JsonDeserializer implements Deserializer<Map<String, Object>> {

    private static final Logger logger = LoggerFactory.getLogger(JsonDeserializer.class);

    private final ObjectMapper mapper = new ObjectMapper();


    /**
     * Configures the deserializer, setting up the mapper configurations
     *
     * @param map
     * @param b
     */
    public void configure(Map<String, ?> map, boolean b) {
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    }

    /**
     * Deserialize the data off of the topic
     *
     * @param topic topic the data was pulled from
     * @param data  serialized byte data coming off of the topic
     * @return
     */
    public Map<String, Object> deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, 0, data.length, new TypeReference<SortedMap<String, Object>>() {
            });
        } catch (JsonGenerationException | JsonMappingException e) {
            logger.error("JSON Error", e);
        } catch (IOException e) {
            logger.error("IO Error", e);
        }

        return null;
    }

    public void close() {
        // empty
    }
}
