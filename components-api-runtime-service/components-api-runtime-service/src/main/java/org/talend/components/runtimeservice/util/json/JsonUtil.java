package org.talend.components.runtimeservice.util.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.talend.daikon.properties.Properties;

import java.io.InputStream;

/**
 * Util for round trip between ComponentProperties and JSONSchema/UISchema/JSONData
 */
public class JsonUtil {
    static final String TAG_JSON_SCHEMA = "jsonschema";
    static final String TAG_JSON_UI = "uischema";
    static final String TAG_JSON_DATA = "jsondata";
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final JsonSchemaGenerator jsonSchemaGenerator = new JsonSchemaGenerator();
    private static final UiSchemaGenerator uiSchemaGenerator = new UiSchemaGenerator();
    private static final JsonDataGenerator jsonDataGenerator = new JsonDataGenerator();
    private static final JsonResolver resolver = new JsonResolver();

    public static Properties fromJson(String jsonStr) {
        try {
            JsonNode jsonNode = mapper.readTree(jsonStr);
            Properties root = resolver.resolveJson(jsonNode.get(TAG_JSON_SCHEMA).toString(),
                    jsonNode.get(TAG_JSON_DATA).toString());
            return root;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Properties fromJson(InputStream jsonIS) {
        try {
            JsonNode jsonNode = mapper.readTree(jsonIS);
            Properties root = resolver.resolveJson(jsonNode.get(TAG_JSON_SCHEMA).toString(),
                    jsonNode.get(TAG_JSON_DATA).toString());

            return root;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String toJson(Properties cp, boolean hasWidget) {
        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.set(TAG_JSON_SCHEMA, jsonSchemaGenerator.genSchema(cp));
        objectNode.set(TAG_JSON_DATA, jsonDataGenerator.genData(cp));
        if (hasWidget) {
            objectNode.set(TAG_JSON_UI, uiSchemaGenerator.genWidget(cp));
        }
        return objectNode.toString();
    }
}
