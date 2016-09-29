package org.talend.components.runtimeservice.util.json;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.talend.components.api.properties.ComponentReferenceProperties;
import org.talend.daikon.NamedThing;
import org.talend.daikon.properties.PresentationItem;
import org.talend.daikon.properties.Properties;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.presentation.Widget;
import org.talend.daikon.properties.property.Property;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class UISchemaGenerator extends JSONBaseTool {
    protected <T extends Properties> ObjectNode genWidget(T properties) {
        return processTPropertiesWidget(properties);
    }

    /**
     * Generate UISchema by the given ComponentProperties and relate Form/Widget
     * Only consider Main and Advanced Form
     *
     * @param cProperties
     * @return UISchema
     */
    private ObjectNode processTPropertiesWidget(Properties cProperties) {
        Form mainForm = cProperties.getForm(Form.MAIN);
        Form advancedForm = cProperties.getForm(Form.ADVANCED);
        return processTPropertiesWidget(mainForm, advancedForm);
    }

    private ObjectNode processTPropertiesWidget(Form... forms) {
        ObjectNode schema = createSchema();

        Properties cProperties = null;
        List<JSONWidget> jsonWidgets = new ArrayList<>();
        for (Form form : forms) {
            if (form != null) {
                jsonWidgets.addAll(listTypedWidget(form));
                cProperties = form.getProperties();
            }
        }

        // Merge widget in Main and Advanced form together, need the merged order.
        Map<Integer, String> order = new TreeMap<>();

        List<Property> propertyList = listTProperty(cProperties);
        List<Properties> propertiesList = listTProperties(cProperties);
        for (JSONWidget jsonWidget : jsonWidgets) {
            NamedThing content = jsonWidget.getContent();
            if (propertyList.contains(content) || content instanceof PresentationItem) {
                ObjectNode jsonNodes = processTWidget(jsonWidget.getWidget(), createSchema());
                if (jsonNodes.size() != 0) {
                    schema.set(jsonWidget.getName(), jsonNodes);
                }
                order.put(jsonWidget.getOrder(), jsonWidget.getName());
            } else {
                Properties checkProperties = null;
                Form resolveForm = null;
                if (content instanceof Form) {
                    //ComponentProperties could contains multiple type of Form, form in widget is the current used
                    resolveForm = (Form) content;
                    checkProperties = resolveForm.getProperties();
                } else {
                    checkProperties = (Properties) content;
                    resolveForm = checkProperties.getForm(Form.MAIN);//It's possible to add Properties in widget, so find the Main form default
                }
                if (propertiesList.contains(checkProperties) && resolveForm != null) {
                    ObjectNode jsonNodes = processTPropertiesWidget(resolveForm);
                    jsonNodes = processTWidget(jsonWidget.getWidget(), jsonNodes);//add the current ComponentProperties/Form widget type
                    order.put(jsonWidget.getOrder(), jsonWidget.getName());
                    if (jsonNodes.size() != 0) {
                        schema.set(jsonWidget.getName(), jsonNodes);
                    }
                }
            }
        }

        ArrayNode orderSchema = schema.putArray(UISchemaMeta.TAG_ORDER);
        // Consider merge Main and Advanced in together, advanced * 100 as default, make sure widget in Advanced will after widget in Main
        for (Integer i : order.keySet()) {
            orderSchema.add(order.get(i));
        }

        //For the property which not in the form(hidden property)
        for (Property property : propertyList) {
            String propName = property.getName();
            if (!order.values().contains(propName)) {
                orderSchema.add(propName);
                schema.set(propName, setHiddenWidget(createSchema()));
            }
        }
        //For the properties which not in the form(hidden properties)
        for (Properties properties : propertiesList) {
            String propName = properties.getName();
            if (!order.values().contains(propName)) {
                orderSchema.add(propName);
                schema.set(propName, setHiddenWidget(createSchema()));
            }
        }

        return schema;
    }

    private ObjectNode processTWidget(Widget widget, ObjectNode schema) {
        if (widget.isHidden()) {
            schema = setHiddenWidget(schema);
        } else {
            String widgetType = WIDGET_MAPPING.get(widget.getWidgetType());
            if (widgetType != null) {
                schema.put(UISchemaMeta.TAG_WIDGET, widgetType);
            } else {
                widgetType = CUSTOM_WIDGET_MAPPING.get(widget.getWidgetType());
                if (widgetType != null) {
                    schema.put(UISchemaMeta.TAG_CUSTOM_WIDGET, widgetType);
                }
            }
            schema = addTriggerTWidget(widget, schema);
        }
        return schema;
    }

    private ObjectNode addTriggerTWidget(Widget widget, ObjectNode schema) {
        ArrayNode jsonNodes = schema.putArray(UISchemaMeta.TAG_TRIGGER);
        if(widget.isCallAfter()){
            jsonNodes.add(UISchemaMeta.TRIGGER_AFTER);
        }
        if(widget.isCallBeforeActivate()){
            jsonNodes.add(UISchemaMeta.TRIGGER_BEFORE_ACTIVATE);
        }
        if(widget.isCallBeforePresent()){
            jsonNodes.add(UISchemaMeta.TRIGGER_BEFORE_PRESENT);
        }
        if(widget.isCallValidate()){
            jsonNodes.add(UISchemaMeta.TRIGGER_VALIDATE);
        }
        if(jsonNodes.size() == 0){
            schema.remove(UISchemaMeta.TAG_TRIGGER);
        }
        return schema;
    }

    private List<JSONWidget> listTypedWidget(Form form) {
        List<JSONWidget> results = new ArrayList<>();
        if (form != null) {
            for (Widget widget : form.getWidgets()) {
                NamedThing content = widget.getContent();
                if ((content instanceof Property || content instanceof Properties || content instanceof
                        Form || content instanceof PresentationItem) && !(content instanceof ComponentReferenceProperties)) {
                    results.add(new JSONWidget(widget, form));
                }
            }
        }
        return results;
    }

    private ObjectNode setHiddenWidget(ObjectNode schema) {
        schema.put(UISchemaMeta.TAG_WIDGET, UISchemaMeta.TYPE_HIDDEN);
        return schema;
    }

}
