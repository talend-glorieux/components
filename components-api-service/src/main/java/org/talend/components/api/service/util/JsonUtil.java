package org.talend.components.api.service.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.talend.daikon.properties.Properties;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;

public class JsonUtil {
    static final String TAG_JSON_SCHEMA = "jsonschema";
    static final String TAG_JSON_UI = "uischema";
    static final String TAG_JSON_DATA = "jsondata";
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final JsonGenerator generator = new JsonGenerator();

    public static Properties fromJson(String jsonStr) throws IOException,
            ClassNotFoundException, InvocationTargetException, NoSuchMethodException,
            InstantiationException, IllegalAccessException {
        JsonNode jsonNode = mapper.readTree(jsonStr);
        Properties root = generator.resolveJson(jsonNode.get(TAG_JSON_SCHEMA).toString(),
                jsonNode.get(TAG_JSON_DATA).toString(), "root");

        return root;
    }

    public static Properties fromJson(InputStream jsonIS) throws IOException,
            ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
            InstantiationException, IllegalAccessException {
        JsonNode jsonNode = mapper.readTree(jsonIS);
        Properties root = generator.resolveJson(jsonNode.get(TAG_JSON_SCHEMA).toString(),
                jsonNode.get(TAG_JSON_DATA).toString(), "root");

        return root;
    }

    public static String toJson(Properties cp, boolean hasWidget) {
        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.set(TAG_JSON_SCHEMA, generator.genSchema(cp));
        objectNode.set(TAG_JSON_DATA, generator.genData(cp));
        if (hasWidget) {
            objectNode.set(TAG_JSON_UI, generator.genWidget(cp));
        }
        return objectNode.toString();
    }
}
