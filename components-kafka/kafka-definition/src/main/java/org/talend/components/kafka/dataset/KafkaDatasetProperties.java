package org.talend.components.kafka.dataset;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.talend.components.api.properties.ComponentReferenceProperties;
import org.talend.components.api.properties.ComponentReferencePropertiesEnclosing;
import org.talend.components.common.SchemaProperties;
import org.talend.components.common.dataset.DatasetProperties;
import org.talend.components.kafka.datastore.KafkaDatastoreDefinition;
import org.talend.components.kafka.datastore.KafkaDatastoreProperties;
import org.talend.daikon.NamedThing;
import org.talend.daikon.SimpleNamedThing;
import org.talend.daikon.properties.PropertiesImpl;
import org.talend.daikon.properties.ValidationResult;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.presentation.Widget;
import org.talend.daikon.properties.property.Property;
import org.talend.daikon.properties.property.PropertyFactory;

import java.util.ArrayList;
import java.util.List;

import static org.talend.daikon.properties.presentation.Widget.widget;

public class KafkaDatasetProperties extends PropertiesImpl implements DatasetProperties<KafkaDatastoreProperties>, ComponentReferencePropertiesEnclosing {

    public KafkaDatastoreProperties datastore = new KafkaDatastoreProperties("datastore");
    public Property<String> topic = PropertyFactory.newString("topic");
    public SchemaProperties main = new SchemaProperties("main");

    //use this to store dataset id for io component
    public ComponentReferenceProperties referencedComponent = new ComponentReferenceProperties("referencedComponent", this);

    public KafkaDatasetProperties(String name) {
        super(name);
    }

    @Override
    public void setupLayout() {
        super.setupLayout();

        Form mainForm = new Form(this, Form.MAIN);
        mainForm.addRow(datastore.getForm(Form.REFERENCE));//FIXME show or not?
        mainForm.addRow(widget(topic).setWidgetType(Widget.NAME_SELECTION_AREA_WIDGET_TYPE));
        mainForm.addRow(main.getForm(Form.MAIN));

        // A form for a reference to a dataset, used in a KafkaIO for example
        Form refForm = Form.create(this, Form.REFERENCE);
        Widget compListWidget = widget(referencedComponent).setWidgetType(Widget.COMPONENT_REFERENCE_WIDGET_TYPE);//FIXME provide a way make the THIS_COMPONENT disable
        referencedComponent.componentType.setValue(KafkaDatastoreDefinition.NAME);
        refForm.addRow(compListWidget);
        refForm.addRow(mainForm);
    }



    public ValidationResult beforeTopic() {
        //FIXME(bchen) replace by kafkaSourceOrSink.getTopics
        List<NamedThing> topics = new ArrayList<>();
        topics.add(new SimpleNamedThing("topic1", "topic1"));
        topics.add(new SimpleNamedThing("topic2", "topic2"));
        topic.setPossibleValues(topics);
        return ValidationResult.OK;
    }

    public ValidationResult afterTopic() {
        //FIXME(bchen) replace by kafkaSourceOrSink.getSchema
        String topicName = topic.getValue();
        SchemaBuilder.FieldAssembler<Schema> fa = SchemaBuilder.record(topicName).namespace(topicName + "_namespace").fields();
        fa = fa.name(topicName + "col1").type(SchemaBuilder.builder().stringBuilder().endString()).noDefault();
        fa = fa.name(topicName + "col2").type(SchemaBuilder.builder().intBuilder().endInt()).noDefault();
        main.schema.setValue(fa.endRecord());
        return ValidationResult.OK;
    }

    @Override
    public void setDatastoreProperties(KafkaDatastoreProperties datastoreProperties) {
        datastore = datastoreProperties;
    }

    @Override
    public KafkaDatastoreProperties getDatastoreProperties() {
        return datastore;
    }

    @Override
    public void afterReferencedComponent() {

    }
}
