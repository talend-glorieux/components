package org.talend.components.kafka.datastore;

import org.talend.components.api.properties.ComponentReferenceProperties;
import org.talend.components.api.properties.ComponentReferencePropertiesEnclosing;
import org.talend.components.common.datastore.DatastoreProperties;
import org.talend.daikon.properties.PropertiesImpl;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.presentation.Widget;
import org.talend.daikon.properties.property.Property;
import org.talend.daikon.properties.property.PropertyFactory;

import static org.talend.daikon.properties.presentation.Widget.widget;

public class KafkaDatastoreProperties extends PropertiesImpl implements DatastoreProperties, ComponentReferencePropertiesEnclosing {

    //zookeepers useful for list topics
    public Property<String> zookeepers = PropertyFactory.newString("zookeepers").setRequired();

    public Property<String> brokers = PropertyFactory.newString("brokers").setRequired();

    //use this to store datastore id for dataset
    public ComponentReferenceProperties referencedComponent = new ComponentReferenceProperties("referencedComponent", this);

    public KafkaDatastoreProperties(String name) {
        super(name);
    }

    @Override
    public void setupLayout() {
        super.setupLayout();

        Form mainForm = new Form(this, Form.MAIN);
        mainForm.addRow(zookeepers);
        mainForm.addRow(brokers);

        // A form for a reference to a connection, used in a KafkaDataset for example
        // FIXME but a question here is Dataset only need the datastore id.
        Form refForm = Form.create(this, Form.REFERENCE);
        Widget compListWidget = widget(referencedComponent).setWidgetType(Widget.COMPONENT_REFERENCE_WIDGET_TYPE);//FIXME provide a way make the THIS_COMPONENT disable
        referencedComponent.componentType.setValue(KafkaDatastoreDefinition.NAME);
        refForm.addRow(compListWidget);
        refForm.addRow(mainForm);
    }

    @Override
    public void afterReferencedComponent() {
        //FIXME For datastream the reference id should not be ComponentReferenceProperties.ReferenceType.THIS_COMPONENT
    }
}
