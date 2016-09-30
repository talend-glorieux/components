package org.talend.components.kafka.datastore;

import org.talend.components.api.component.runtime.RuntimeInfo;
import org.talend.components.common.datastore.DatastoreDefinition;
import org.talend.daikon.SimpleNamedThing;

public class KafkaDatastoreDefinition extends SimpleNamedThing implements DatastoreDefinition<KafkaDatastoreProperties> {

    public static final String NAME = "KafkaDatastore";

    public KafkaDatastoreDefinition() {
        super(NAME);
    }

    @Override
    public KafkaDatastoreProperties createProperties() {
        KafkaDatastoreProperties properties = new KafkaDatastoreProperties("kafkaDatastore");
        properties.init();
        return properties;
    }

    @Override
    public RuntimeInfo getRuntimeInfo(KafkaDatastoreProperties properties, Object ctx) {
        return null;
    }
}
