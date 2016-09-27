package org.talend.components.runtimeservice.util.json;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.talend.daikon.NamedThing;
import org.talend.daikon.properties.Properties;
import org.talend.daikon.properties.property.EnumProperty;
import org.talend.daikon.properties.property.Property;

import java.util.Date;
import java.util.List;

/**
 * Generator JSONSchema from Properties
 */
public class JSONSchemaGenerator extends JSONBaseTool {
    protected <T extends Properties> ObjectNode genSchema(T properties) {
        return processTProperties(properties);
    }

    private ObjectNode processTProperties(Properties cProperties) {
        ObjectNode schema = createSchema();
        schema.put(JSONSchemaMeta.TAG_TYPE, JSONSchemaMeta.TYPE_OBJECT);
        schema.put(JSONSchemaMeta.TAG_ID, cProperties.getClass().getName());
        schema.putObject(JSONSchemaMeta.TAG_PROPERTIES);

        List<Property> propertyList = listTProperty(cProperties);
        for (Property property : propertyList) {
            String name = property.getName();
            if (property.isRequired()) {
                addToRequired(schema, name);
            }
            ((ObjectNode) schema.get(JSONSchemaMeta.TAG_PROPERTIES)).set(name, processTProperty(property));
        }
        List<Properties> propertiesList = listTProperties(cProperties);
        for (Properties properties : propertiesList) {
            String name = properties.getName();
            ((ObjectNode) schema.get(JSONSchemaMeta.TAG_PROPERTIES)).set(name, processTProperties(properties));
        }
        return schema;
    }

    private ObjectNode processTProperty(Property property) {
        ObjectNode schema = createSchema();
        if (!property.getPossibleValues().isEmpty()) {
            if (property instanceof EnumProperty) {
                schema.put(JSONSchemaMeta.TAG_TYPE, JSONSchemaMeta.TYPE_STRING);
            } else {
                schema.put(JSONSchemaMeta.TAG_TYPE, TYPE_MAPPING.get(property.getType()));
            }
            ArrayNode enumList = schema.putArray(JSONSchemaMeta.TAG_ENUM);
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
            schema.put(JSONSchemaMeta.TAG_TYPE, TYPE_MAPPING.get(property.getType()));
            if (Date.class.getName().equals(property.getType())) {
                schema.put(JSONSchemaMeta.TAG_FORMAT, "date-time");
            }
        }
        return schema;
    }

    private void resolveList(ObjectNode schema, Property property) {
        String className = property.getType();
        schema.put(JSONSchemaMeta.TAG_TYPE, JSONSchemaMeta.TAG_ARRAYS);
        ObjectNode items = createSchema();
        schema.set(JSONSchemaMeta.TAG_ITEMS, items);
        String innerClassName = className.substring("java.util.List<".length(), className.length() - 1);
        Class<?> aClass = findClass(innerClassName);
        if (aClass.isEnum()) {
            items.put(JSONSchemaMeta.TAG_TYPE, JSONSchemaMeta.TYPE_STRING);
            ArrayNode enumList = items.putArray(JSONSchemaMeta.TAG_ENUM);
            for (Object k : aClass.getEnumConstants()) {
                enumList.add(k.toString());
            }
        } else {
            items.put(JSONSchemaMeta.TAG_TYPE, TYPE_MAPPING.get(innerClassName));
        }
    }

    private void addToRequired(ObjectNode schema, String name) {
        ArrayNode requiredNode;
        if (!schema.has(JSONSchemaMeta.TAG_REQUIRED)) {
            requiredNode = schema.putArray(JSONSchemaMeta.TAG_REQUIRED);
        } else {
            requiredNode = (ArrayNode) schema.get(JSONSchemaMeta.TAG_REQUIRED);
        }
        requiredNode.add(name);
    }

}
