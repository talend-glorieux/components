package org.talend.components.runtimeservice.util.json;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.apache.avro.Schema;
import org.talend.daikon.properties.Properties;
import org.talend.daikon.properties.property.Property;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.talend.components.runtimeservice.util.json.JsonBaseTool.findClass;
import static org.talend.components.runtimeservice.util.json.JsonBaseTool.getSubProperties;
import static org.talend.components.runtimeservice.util.json.JsonBaseTool.getSubProperty;

public class JsonResolver {

    protected static final ObjectMapper mapper = new ObjectMapper();

    protected Properties resolveJson(String jsonSchemaStr, String jsonDataStr)
            throws NoSuchMethodException, IOException, InstantiationException, IllegalAccessException, InvocationTargetException,
            ClassNotFoundException {
        JsonNode jsonSchema = mapper.readTree(jsonSchemaStr);
        if (jsonSchema.isObject()) {
            JsonNode classNameNode = jsonSchema.get(JsonSchemaConstants.CUSTOM_TAG_ID);
            Class<?> aClass = Class.forName(classNameNode.textValue());
            Constructor<?> declaredConstructor = aClass.getDeclaredConstructor(String.class);
            Properties cProperties = (Properties) declaredConstructor.newInstance("root");
            return resolveJson(jsonDataStr, cProperties);
        }
        return null;
    }

    protected Properties resolveJson(String jsonDataStr, Properties cProperties)
            throws IOException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
            InstantiationException {
        JsonNode jsonData = mapper.readTree(jsonDataStr);

        List<Property> propertyList = getSubProperty(cProperties);
        Field[] declaredFields = cProperties.getClass().getDeclaredFields();
        for (Property property : propertyList) {
            for (Field declaredField : declaredFields) {
                String fieldName = declaredField.getName();
                if (fieldName.equals(property.getName())) {
                    property.setValue(getTPropertyValue(property, jsonData.get(fieldName)));
                }
            }
        }
        List<Properties> propertiesList = getSubProperties(cProperties);
        for (Properties properties : propertiesList) {
            for (Field declaredField : declaredFields) {
                String fieldName = declaredField.getName();
                if (fieldName.equals(properties.getName())) {
                    resolveJson(jsonData.get(fieldName).toString(), cProperties.getProperties(fieldName));
                }
            }
        }
        return cProperties;
    }

    private Object getTPropertyValue(Property property, JsonNode dataNode) {
        String javaType = property.getType();
        if (dataNode == null || dataNode.isNull()) {
            return null;
        } else if (JsonSchemaConstants.TYPE_STRING.equals(JsonSchemaConstants.getTypeMapping().get(javaType))) {
            return dataNode.textValue();
        } else if (JsonSchemaConstants.TYPE_INTEGER.equals(JsonSchemaConstants.getTypeMapping().get(javaType))) {
            return dataNode.intValue();
        } else if (JsonSchemaConstants.TYPE_NUMBER.equals(JsonSchemaConstants.getTypeMapping().get(javaType))) {
            if (Float.class.getName().equals(javaType)) {
                return dataNode.numberValue().floatValue();
            }
            return dataNode.numberValue();
        } else if (JsonSchemaConstants.TYPE_BOOLEAN.equals(JsonSchemaConstants.getTypeMapping().get(javaType))) {
            return dataNode.booleanValue();
        } else if (Schema.class.getName().equals(javaType)) {
            return new Schema.Parser().parse(dataNode.textValue());
        } else if (findClass(javaType).isEnum()) {
            return Enum.valueOf(findClass(javaType), dataNode.textValue());
        } else {
            throw new RuntimeException("do not support " + javaType + " now.");
        }

    }
}
