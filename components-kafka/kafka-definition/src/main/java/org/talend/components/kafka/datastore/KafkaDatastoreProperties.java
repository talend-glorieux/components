package org.talend.components.kafka.datastore;

import org.talend.components.common.datastore.DatastoreProperties;
import org.talend.daikon.properties.PropertiesImpl;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.property.Property;
import org.talend.daikon.properties.property.PropertyFactory;

public class KafkaDatastoreProperties extends PropertiesImpl implements DatastoreProperties {

    //zookeepers useful for list topics
    public Property<String> zookeepers = PropertyFactory.newString("zookeepers").setRequired();

    public Property<String> brokers = PropertyFactory.newString("brokers").setRequired();

    public KafkaDatastoreProperties(String name) {
        super(name);
    }

    @Override
    public void setupLayout() {
        super.setupLayout();

        Form mainForm = new Form(this, Form.MAIN);
        mainForm.addRow(zookeepers);
        mainForm.addRow(brokers);
    }
}
