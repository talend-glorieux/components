package org.talend.components.runtimeservice.util.json;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.talend.daikon.properties.Properties;
import org.talend.daikon.properties.property.Property;

import java.util.List;

import static org.talend.components.runtimeservice.util.json.JsonBaseTool.findClass;
import static org.talend.components.runtimeservice.util.json.JsonBaseTool.getSubProperties;
import static org.talend.components.runtimeservice.util.json.JsonBaseTool.getSubProperty;

public class JsonDataGenerator {

    protected <T extends Properties> ObjectNode genData(T properties) {
        return processTPropertiesData(properties);
    }

    private ObjectNode processTPropertiesData(Properties cProperties) {
        ObjectNode rootNode = JsonNodeFactory.instance.objectNode();

        List<Property> propertyList = getSubProperty(cProperties);
        for (Property property : propertyList) {
            processTPropertyValue(property, rootNode);
        }
        List<Properties> propertiesList = getSubProperties(cProperties);
        for (Properties properties : propertiesList) {
            String name = properties.getName();
            rootNode.set(name, processTPropertiesData(properties));
        }
        return rootNode;
    }

    private ObjectNode processTPropertyValue(Property property, ObjectNode node) {
        String javaType = property.getType();
        String pName = property.getName();
        Object pValue = property.getValue();
        if (pValue == null) {
            // unset if the value is null
            // node.set(pName, node.nullNode());
        } else if (String.class.getName().equals(javaType)) {
            node.put(pName, (String) pValue);
        } else if (Integer.class.getName().equals(javaType)) {
            node.put(pName, (Integer) pValue);
        } else if (findClass(javaType).isEnum()) {
            node.put(pName, pValue.toString());
        } else if (Boolean.class.getName().equals(javaType)) {
            node.put(pName, (Boolean) pValue);
        } else if (Schema.class.getName().equals(javaType)) {
            node.put(pName, pValue.toString());
        } else if (Double.class.getName().equals(javaType)) {
            node.put(pName, (Double) pValue);
        } else if (Float.class.getName().equals(javaType)) {
            node.put(pName, (Float) pValue);
        } else {
            throw new RuntimeException("do not support " + javaType + " now.");
        }

        return node;
    }
}
