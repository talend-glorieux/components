package org.talend.components.api.service.util;

import org.apache.avro.Schema;
import org.talend.components.api.properties.ComponentPropertiesImpl;
import org.talend.daikon.NamedThing;
import org.talend.daikon.SimpleNamedThing;
import org.talend.daikon.properties.ValidationResult;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.presentation.Widget;
import org.talend.daikon.properties.property.Property;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.talend.daikon.properties.presentation.Widget.widget;
import static org.talend.daikon.properties.property.PropertyFactory.*;

public class TestComponentProperties extends ComponentPropertiesImpl {
    /**
     * named constructor to be used is these properties are nested in other properties. Do not subclass this method for
     * initialization, use {@link #init()} instead.
     *
     * @param name
     */
    public TestComponentProperties(String name) {
        super(name);
    }

    public Property<String> strProp = newString("strProp", "defaultValue1").setRequired();
    public Property<String> passProp = newString("passProp", "secretValue1");
    public Property<String> listProp = newString("listProp");
    //order of below is reverse
    public Property<Boolean> booProp = newBoolean("booProp", false);
    public Property<Integer> intProp = newInteger("intProp", 1);
    public Property<Double> douProp = newDouble("douProp", 1.0);
    public Property<Float> floProp = newFloat("floProp", 2.0f);
    public Property<Date> datProp = newDate("datProp");
    //order end
    enum enumPropTest{
        A,B
    }
    public Property<enumPropTest> enumProp = newEnum("enumProp", enumPropTest.class);
    Property<Schema> schProp = newSchema("schProp");

    @Override
    public void setupProperties() {
        //mock beforeTable
        List<NamedThing> values = new ArrayList<>();
        values.add(new SimpleNamedThing("a","a"));
        values.add(new SimpleNamedThing("b", "b"));
        listProp.setPossibleValues(values);
    }

    @Override
    public void setupLayout() {
        super.setupLayout();
        Form mainForm = new Form(this, Form.MAIN);
        mainForm.addRow(strProp);
        mainForm.addColumn(widget(passProp).setWidgetType(Widget.HIDDEN_TEXT_WIDGET_TYPE));
        mainForm.addRow(widget(listProp).setWidgetType(Widget.NAME_SELECTION_AREA_WIDGET_TYPE));
        //reverse order
        mainForm.addRow(datProp);
        mainForm.addRow(floProp);
        mainForm.addRow(douProp);
        mainForm.addRow(intProp);
        mainForm.addRow(booProp);

        mainForm.addRow(enumProp);
        mainForm.addRow(widget(schProp).setWidgetType(Widget.SCHEMA_EDITOR_WIDGET_TYPE));
    }

}
