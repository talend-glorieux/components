package org.talend.components.runtimeservice.util.json;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.talend.daikon.properties.Properties;
import org.talend.daikon.properties.property.Property;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.List;

public class JSONResolver extends JSONBaseTool {

    protected Properties resolveJson(String jsonSchemaStr, String jsonDataStr, String propertiesName) throws NoSuchMethodException, IOException, InstantiationException, IllegalAccessException, InvocationTargetException, ClassNotFoundException {
        return resolveJson(jsonSchemaStr, jsonDataStr, propertiesName, null);
    }

    protected Properties resolveJson(String jsonSchemaStr, String jsonDataStr, String
            propertiesName, Properties cProperties) throws IOException, ClassNotFoundException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException, InstantiationException {
        JsonNode jsonSchema = mapper.readTree(jsonSchemaStr);
        JsonNode jsonData = mapper.readTree(jsonDataStr);

        if (jsonSchema.isObject()) {
            JsonNode classNameNode = jsonSchema.get(JSONSchemaMeta.TAG_ID);
            jsonSchema = jsonSchema.get(JSONSchemaMeta.TAG_PROPERTIES);
            Class<?> aClass = Class.forName(classNameNode.textValue());
            if (Properties.class.isAssignableFrom(aClass)) {
                if(cProperties == null) {
                    Constructor<?> declaredConstructor = aClass.getDeclaredConstructor(String.class);
                    cProperties = (Properties) declaredConstructor.newInstance
                            (propertiesName);
                }

                List<Property> propertyList = listTProperty(cProperties);
                Field[] declaredFields = aClass.getDeclaredFields();
                for (Property property : propertyList) {
                    for (Field declaredField : declaredFields) {
                        String fieldName = declaredField.getName();
                        if (fieldName.equals(property.getName())) {
                            property.setValue(getTPropertyValue(property, jsonData.get(fieldName)));
                        }
                    }
                }
                List<Properties> propertiesList = listTProperties(cProperties);
                for (Properties properties : propertiesList) {
                    for (Field declaredField : declaredFields) {
                        String fieldName = declaredField.getName();
                        if (fieldName.equals(properties.getName())) {
                            Properties finalProps = null;
                            if(Modifier.isFinal(declaredField.getModifiers())) {
                                finalProps = cProperties.getProperties(fieldName);
                                resolveJson(jsonSchema.get(fieldName)
                                        .toString(), jsonData.get(fieldName).toString(), fieldName, finalProps);
                            } else {
                                declaredField.set(cProperties, resolveJson(jsonSchema.get(fieldName)
                                        .toString(), jsonData.get(fieldName).toString(), fieldName, finalProps));
                            }
                        }
                    }
                }
                return cProperties;
            }
        }
        return null;
    }

    private Object getTPropertyValue(Property property, JsonNode dataNode) {
        String javaType = property.getType();
        if (dataNode == null || dataNode.isNull()) {
            return null;
        } else if (JSONSchemaMeta.TYPE_STRING.equals(TYPE_MAPPING.get(javaType))) {
            return dataNode.textValue();
        } else if (JSONSchemaMeta.TYPE_INTEGER.equals(TYPE_MAPPING.get(javaType))) {
            return dataNode.intValue();
        } else if (JSONSchemaMeta.TYPE_NUMBER.equals(TYPE_MAPPING.get(javaType))) {
            if (Float.class.getName().equals(javaType)) {
                return dataNode.numberValue().floatValue();
            }
            return dataNode.numberValue();
        } else if (JSONSchemaMeta.TYPE_BOOLEAN.equals(TYPE_MAPPING.get(javaType))) {
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
