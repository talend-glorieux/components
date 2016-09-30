package org.talend.components.kafka;

import org.apache.avro.Schema;
import org.talend.components.api.component.Connector;
import org.talend.components.api.component.PropertyPathConnector;
import org.talend.components.common.io.IOProperties;
import org.talend.components.kafka.dataset.KafkaDatasetProperties;
import org.talend.daikon.properties.Properties;
import org.talend.daikon.properties.PropertiesImpl;
import org.talend.daikon.properties.presentation.Form;

import java.util.Set;

public abstract class KafkaIOBasedProperties extends IOProperties<KafkaDatasetProperties> {

    public KafkaDatasetProperties dataset = new KafkaDatasetProperties("dataset");

    protected transient PropertyPathConnector MAIN_CONNECTOR = new PropertyPathConnector(Connector.MAIN_NAME, "module.main");

    public KafkaIOBasedProperties(String name) {
        super(name);
    }

    @Override
    public void setupLayout() {
        super.setupLayout();

        Form mainForm = new Form(this, Form.MAIN);
        mainForm.addRow(dataset.getForm(Form.REFERENCE));
    }

    @Override
    public void setDatasetProperties(KafkaDatasetProperties datasetProperties) {
        dataset = datasetProperties;
    }

    @Override
    public KafkaDatasetProperties getDatasetProperties() {
        return dataset;
    }

}
