package org.talend.components.api.service.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.talend.components.api.properties.ComponentReferenceProperties;
import org.talend.daikon.NamedThing;
import org.talend.daikon.properties.Properties;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.presentation.Widget;
import org.talend.daikon.properties.property.EnumProperty;
import org.talend.daikon.properties.property.Property;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

/**
 * Generate json-schema and ui-schema from {@ComponentProperties},
 * and only consider {@Property} and {@Properties} inside.
 */
public class JsonGenerator {
    //Schema tag
    private static final String TAG_PROPERTIES = "properties";
    private static final String TAG_REQUIRED = "required";
    private static final String TAG_TYPE = "type";
    private static final String TAG_FORMAT = "format";
    private static final String TAG_ID = "id";
    private static final String TAG_ENUM = "enum";
    private static final String TAG_ARRAYS = "arrays";
    private static final String TAG_ITEMS = "items";

    //UI tag
    private static final String UI_TAG_CUSTOM_WIDGET = "ui:field";
    private static final String UI_TAG_WIDGET = "ui:widget";
    private static final String UI_TAG_ORDER = "ui:order";

    private static final Map<String, String> TYPE_MAPPING = new HashMap<>();

    private static final Map<String, String> WIDGET_MAPPING = new HashMap<>();

    private static final Map<String, String> CUSTOM_WIDGET_MAPPING = new HashMap<>();
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        TYPE_MAPPING.put(Character.class.getName(), "string");
        TYPE_MAPPING.put(String.class.getName(), "string");
        TYPE_MAPPING.put(CharSequence.class.getName(), "string");

        TYPE_MAPPING.put(Schema.class.getName(), "schema");

        TYPE_MAPPING.put(Boolean.class.getName(), "boolean");

        TYPE_MAPPING.put(Float.class.getName(), "number");
        TYPE_MAPPING.put(Double.class.getName(), "number");
        TYPE_MAPPING.put(BigDecimal.class.getName(), "number");

        TYPE_MAPPING.put(Byte.class.getName(), "integer");
        TYPE_MAPPING.put(Short.class.getName(), "integer");
        TYPE_MAPPING.put(Integer.class.getName(), "integer");
        TYPE_MAPPING.put(Long.class.getName(), "integer");
        TYPE_MAPPING.put(BigInteger.class.getName(), "integer");

        TYPE_MAPPING.put(Date.class.getName(), "string");

        //table is a custom widget type for UISchema
        CUSTOM_WIDGET_MAPPING.put(Widget.TABLE_WIDGET_TYPE, "table");
        CUSTOM_WIDGET_MAPPING.put(Widget.SCHEMA_EDITOR_WIDGET_TYPE, "schema");
        CUSTOM_WIDGET_MAPPING.put(Widget.SCHEMA_REFERENCE_WIDGET_TYPE, "schema");
        WIDGET_MAPPING.put(Widget.HIDDEN_TEXT_WIDGET_TYPE, "password");
        //null means use the default
        WIDGET_MAPPING.put(Widget.DEFAULT_WIDGET_TYPE, null);
        WIDGET_MAPPING.put(Widget.SCHEMA_EDITOR_WIDGET_TYPE, null);
        WIDGET_MAPPING.put(Widget.SCHEMA_REFERENCE_WIDGET_TYPE, null);
        WIDGET_MAPPING.put(Widget.NAME_SELECTION_AREA_WIDGET_TYPE, null);
        WIDGET_MAPPING.put(Widget.NAME_SELECTION_REFERENCE_WIDGET_TYPE, null);
        WIDGET_MAPPING.put(Widget.COMPONENT_REFERENCE_WIDGET_TYPE, null);
        WIDGET_MAPPING.put(Widget.BUTTON_WIDGET_TYPE, null);
        WIDGET_MAPPING.put(Widget.FILE_WIDGET_TYPE, null);
        WIDGET_MAPPING.put(Widget.ENUMERATION_WIDGET_TYPE, null);
    }

    private ObjectNode createSchema() {
        return mapper.createObjectNode();
    }

    protected  <T extends Properties> ObjectNode genSchema(T properties) {
        return processTProperties(properties);
    }

    protected <T extends Properties> ObjectNode genWidget(T properties) {
        return processTPropertiesWidget(properties);
    }

    protected <T extends Properties> ObjectNode genData(T properties) {
        return processTPropertiesData(properties);
    }

    protected Properties resolveJson(String jsonSchemaStr, String jsonDataStr, String
            propertiesName) throws IOException, ClassNotFoundException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException, InstantiationException {
        JsonNode jsonSchema = mapper.readTree(jsonSchemaStr);
        JsonNode jsonData = mapper.readTree(jsonDataStr);

        if (jsonSchema.isObject()) {
            JsonNode classNameNode = jsonSchema.get(TAG_ID);
            jsonSchema = jsonSchema.get(TAG_PROPERTIES);
            Class<?> aClass = Class.forName(classNameNode.textValue());
            if (Properties.class.isAssignableFrom(aClass)) {
                Constructor<?> declaredConstructor = aClass.getDeclaredConstructor(String.class);
                Properties cProperties = (Properties) declaredConstructor.newInstance
                        (propertiesName);

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
                            declaredField.set(cProperties, resolveJson(jsonSchema.get(fieldName)
                                    .toString(), jsonData.get(fieldName).toString(), fieldName));
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
        } else if ("string".equals(TYPE_MAPPING.get(javaType))) {
            return dataNode.textValue();
        } else if ("integer".equals(TYPE_MAPPING.get(javaType))) {
            return dataNode.intValue();
        } else if ("number".equals(TYPE_MAPPING.get(javaType))) {
            if(Float.class.getName().equals(javaType)){
                return dataNode.numberValue().floatValue();
            }
            return dataNode.numberValue();
        } else if ("boolean".equals(TYPE_MAPPING.get(javaType))) {
            return dataNode.booleanValue();
        } else if ("schema".equals(TYPE_MAPPING.get(javaType))) {
            return new Schema.Parser().parse(dataNode.textValue());
        } else if (findClass(javaType).isEnum()) {
            return Enum.valueOf(findClass(javaType), dataNode.textValue());
        } else {
            throw new RuntimeException("do not support " + javaType + " now.");
        }

    }

    private ObjectNode processTPropertiesData(Properties cProperties) {
        ObjectNode rootNode = createSchema();

        List<Property> propertyList = listTProperty(cProperties);
        for (Property property : propertyList) {
            processTPropertyValue(property, rootNode);
        }
        List<Properties> propertiesList = listTProperties(cProperties);
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
//            node.set(pName, node.nullNode());
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
            //}else if(){ for list
        } else {
            throw new RuntimeException("do not support " + javaType + " now.");
        }

        return node;
    }

    private ObjectNode processTPropertiesWidget(Properties cProperties) {
        ObjectNode schema = createSchema();

        List<JsonWidget> jsonWidgets = listTypedWidget(cProperties);

        Map<Integer, String> order = new TreeMap<>();

        List<Property> propertyList = listTProperty(cProperties);
        List<Properties> propertiesList = listTProperties(cProperties);
        for (JsonWidget jsonWidget : jsonWidgets) {
            NamedThing content = jsonWidget.getContent();
            if (propertyList.contains(content)) {
                ObjectNode jsonNodes = processTWidget(jsonWidget.getWidget(), createSchema());
                if (jsonNodes.size() != 0) {
                    schema.set(jsonWidget.getName(), jsonNodes);
                }
                order.put(jsonWidget.getOrder(), jsonWidget.getName());
            } else {
                if (content instanceof Form) {
                    content = ((Form) content).getProperties();
                }
                if (propertiesList.contains(content)) {
                    ObjectNode jsonNodes = processTPropertiesWidget((Properties) content);
                    jsonNodes = processTWidget(jsonWidget.getWidget(), jsonNodes);
                    order.put(jsonWidget.getOrder(), jsonWidget.getName());
                    if (jsonNodes.size() != 0) {
                        schema.set(jsonWidget.getName(), jsonNodes);
                    }
                }
            }
        }

        ArrayNode orderSchema = schema.putArray(UI_TAG_ORDER);
        for (Integer i : order.keySet()) {
            orderSchema.add(order.get(i));
        }

        //For the property which not in the form(hidden property)
        for (Property property : propertyList) {
            String propName = property.getName();
            if(!order.values().contains(propName)){
                orderSchema.add(propName);
                schema.set(propName, setHiddenWidget(createSchema()));
            }
        }

        return schema;
    }

    private ObjectNode setHiddenWidget(ObjectNode schema) {
        schema.put(UI_TAG_WIDGET, "hidden");
        return schema;
    }

    private ObjectNode processTWidget(Widget widget, ObjectNode schema) {
        if (widget.isHidden()) {
            schema = setHiddenWidget(schema);
        } else {
            String widgetType = WIDGET_MAPPING.get(widget.getWidgetType());
            if (widgetType != null) {
                schema.put(UI_TAG_WIDGET, widgetType);
            } else {
                widgetType = CUSTOM_WIDGET_MAPPING.get(widget.getWidgetType());
                if (widgetType != null) {
                    schema.put(UI_TAG_CUSTOM_WIDGET, widgetType);
                }
            }
        }
        return schema;
    }

    private List<JsonWidget> listTypedWidget(Properties cProperties) {
        List<JsonWidget> results = new ArrayList<>();
        String[] formTypes = new String[]{Form.MAIN, Form.ADVANCED};
        for (String formType : formTypes) {
            Form form = cProperties.getForm(formType);
            if (form != null) {
                results.addAll(listTypedWidget(form.getWidgets(), form));
            }
        }
        return results;
    }

    private List<JsonWidget> listTypedWidget(Collection<Widget> widgets, Form form) {
        List<JsonWidget> results = new ArrayList<>();
        for (Widget widget : widgets) {
            NamedThing content = widget.getContent();
            if ((content instanceof Property || content instanceof Properties || content instanceof
                    Form) && !(content instanceof ComponentReferenceProperties)) {
                results.add(new JsonWidget(widget, form));
            }
        }
        return results;
    }

    private ObjectNode processTProperties(Properties cProperties) {
        ObjectNode schema = createSchema();
        schema.put(TAG_TYPE, "object");
        schema.put(TAG_ID, cProperties.getClass().getName());
        schema.putObject(TAG_PROPERTIES);

        List<Property> propertyList = listTProperty(cProperties);
        for (Property property : propertyList) {
            String name = property.getName();
            if (property.isRequired()) {
                addToRequired(schema, name);
            }
            ((ObjectNode) schema.get(TAG_PROPERTIES)).set(name, processTProperty(property));
        }
        List<Properties> propertiesList = listTProperties(cProperties);
        for (Properties properties : propertiesList) {
            String name = properties.getName();
            ((ObjectNode) schema.get(TAG_PROPERTIES)).set(name, processTProperties(properties));
        }
        return schema;
    }

    private void addToRequired(ObjectNode schema, String name) {
        ArrayNode requiredNode;
        if (!schema.has(TAG_REQUIRED)) {
            requiredNode = schema.putArray(TAG_REQUIRED);
        } else {
            requiredNode = (ArrayNode) schema.get(TAG_REQUIRED);
        }
        requiredNode.add(name);
    }

    private ObjectNode processTProperty(Property property) {
        ObjectNode schema = createSchema();
        if (!property.getPossibleValues().isEmpty()) {
            if(property instanceof EnumProperty){
                schema.put(TAG_TYPE, "string");
            }else{
                schema.put(TAG_TYPE, TYPE_MAPPING.get(property.getType()));
            }
            ArrayNode enumList = schema.putArray(TAG_ENUM);
            List possibleValues = property.getPossibleValues();
            for (Object possibleValue : possibleValues) {
                String value = possibleValue.toString();
                if(NamedThing.class.isAssignableFrom(possibleValue.getClass())){
                    value = ((NamedThing)possibleValue).getName();
                }
                enumList.add(value);
            }
        } else if (property.getType().startsWith("java.util.List")) {
            resolveList(schema, property);
        } else {
            schema.put(TAG_TYPE, TYPE_MAPPING.get(property.getType()));
            if(Date.class.getName().equals(property.getType())){
                schema.put(TAG_FORMAT, "date-time");
            }
        }
        return schema;
    }

    private Class findClass(String className) {
        Class<?> aClass = null;
        try {
            aClass = Class.forName(className);
        } catch (ClassNotFoundException e) {
            int lastPosition = className.lastIndexOf(".");
            className = className.substring(0, lastPosition) + "$" + className
                    .substring(lastPosition + 1);
            try {
                aClass = Class.forName(className);
            } catch (ClassNotFoundException e1) {
                e1.printStackTrace();
            }
        }
        return aClass;
    }

    private void resolveList(ObjectNode schema, Property property) {
        String className = property.getType();
        if (className.startsWith("java.util.List")) {
            schema.put(TAG_TYPE, TAG_ARRAYS);
            ObjectNode items = createSchema();
            schema.set(TAG_ITEMS, items);
            String innerClassName = className.substring("java.util.List<".length(), className
                    .length() - 1);
            Class<?> aClass = findClass(innerClassName);
            if (aClass.isEnum()) {
                items.put(TAG_TYPE, "string");
                ArrayNode enumList = items.putArray(TAG_ENUM);
                for (Object k : aClass.getEnumConstants()) {
                    enumList.add(k.toString());
                }
            } else {
                items.put(TAG_TYPE, TYPE_MAPPING.get(innerClassName));
            }
        }
    }

    private List<Property> listTProperty(Properties cProperties) {
        List<Property> propertyList = new ArrayList<>();
        Field[] allFields = cProperties.getClass().getDeclaredFields();
        for (Field field : allFields) {
            if (Property.class.isAssignableFrom(field.getType())) {
                try {
                    propertyList.add((Property) field.get(cProperties));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }
        return propertyList;
    }

    List<Properties> listTProperties(Properties cProperties) {
        List<Properties> propertiesList = new ArrayList<>();
        Field[] allFields = cProperties.getClass().getDeclaredFields();
        for (Field field : allFields) {
            if (Properties.class.isAssignableFrom(field.getType()) &&
                    !ComponentReferenceProperties.class.isAssignableFrom(field.getType())) {
                try {
                    propertiesList.add((Properties) field.get(cProperties));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }
        return propertiesList;
    }
}

