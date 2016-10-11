package org.talend.components.runtimeservice.util.json;

import static org.talend.components.runtimeservice.util.json.JsonBaseTool.*;

import java.util.Date;
import java.util.List;

import org.talend.daikon.NamedThing;
import org.talend.daikon.properties.Properties;
import org.talend.daikon.properties.property.EnumProperty;
import org.talend.daikon.properties.property.Property;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Generator JSONSchema from Properties
 */
public class JsonSchemaGenerator {

    protected <T extends Properties> ObjectNode genSchema(T properties) {
        return processTProperties(properties, properties.getClass().getName());
    }

    private ObjectNode processTProperties(Properties cProperties, String className) {
        ObjectNode schema = JsonNodeFactory.instance.objectNode();
        schema.put(JsonSchemaConstants.TAG_TYPE, JsonSchemaConstants.TYPE_OBJECT);
        if (className != null) {
            schema.put(JsonSchemaConstants.CUSTOM_TAG_ID, className);
        }
        schema.putObject(JsonSchemaConstants.TAG_PROPERTIES);

        List<Property> propertyList = getSubProperty(cProperties);
        for (Property property : propertyList) {
            String name = property.getName();
            if (property.isRequired()) {
                addToRequired(schema, name);
            }
            ((ObjectNode) schema.get(JsonSchemaConstants.TAG_PROPERTIES)).set(name, processTProperty(property));
        }
        List<Properties> propertiesList = getSubProperties(cProperties);
        for (Properties properties : propertiesList) {
            String name = properties.getName();
            ((ObjectNode) schema.get(JsonSchemaConstants.TAG_PROPERTIES)).set(name, processTProperties(properties, null));
        }
        return schema;
    }

    private ObjectNode processTProperty(Property property) {
        ObjectNode schema = JsonNodeFactory.instance.objectNode();
        if (!property.getPossibleValues().isEmpty()) {
            if (property instanceof EnumProperty) {
                schema.put(JsonSchemaConstants.TAG_TYPE, JsonSchemaConstants.TYPE_STRING);
            } else {
                schema.put(JsonSchemaConstants.TAG_TYPE, JsonSchemaConstants.getTypeMapping().get(property.getType()));
            }
            ArrayNode enumList = schema.putArray(JsonSchemaConstants.TAG_ENUM);
            List possibleValues = property.getPossibleValues();
            for (Object possibleValue : possibleValues) {
                String value = possibleValue.toString();
                if (NamedThing.class.isAssignableFrom(possibleValue.getClass())) {
                    value = ((NamedThing) possibleValue).getName();
                }
                enumList.add(value);
            }
        } else if (property.getType().startsWith("java.util.List")) {
            resolveList(schema, property);
        } else {
            schema.put(JsonSchemaConstants.TAG_TYPE, JsonSchemaConstants.getTypeMapping().get(property.getType()));
            if (Date.class.getName().equals(property.getType())) {
                schema.put(JsonSchemaConstants.TAG_FORMAT, "date-time");
            }
        }
        return schema;
    }

    private void resolveList(ObjectNode schema, Property property) {
        String className = property.getType();
        schema.put(JsonSchemaConstants.TAG_TYPE, JsonSchemaConstants.TYPE_ARRAY);
        ObjectNode items = JsonNodeFactory.instance.objectNode();
        schema.set(JsonSchemaConstants.TAG_ITEMS, items);
        String innerClassName = className.substring("java.util.List<".length(), className.length() - 1);
        Class<?> aClass = findClass(innerClassName);
        if (aClass.isEnum()) {
            items.put(JsonSchemaConstants.TAG_TYPE, JsonSchemaConstants.TYPE_STRING);
            ArrayNode enumList = items.putArray(JsonSchemaConstants.TAG_ENUM);
            for (Object k : aClass.getEnumConstants()) {
                enumList.add(k.toString());
            }
        } else {
            items.put(JsonSchemaConstants.TAG_TYPE, JsonSchemaConstants.getTypeMapping().get(innerClassName));
        }
    }

    private void addToRequired(ObjectNode schema, String name) {
        ArrayNode requiredNode;
        if (!schema.has(JsonSchemaConstants.TAG_REQUIRED)) {
            requiredNode = schema.putArray(JsonSchemaConstants.TAG_REQUIRED);
        } else {
            requiredNode = (ArrayNode) schema.get(JsonSchemaConstants.TAG_REQUIRED);
        }
        requiredNode.add(name);
    }

}
