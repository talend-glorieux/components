package org.talend.components.kafka.dataset;

import org.talend.components.api.component.runtime.RuntimeInfo;
import org.talend.components.common.dataset.DatasetDefinition;
import org.talend.components.common.datastore.DatastoreDefinition;
import org.talend.components.kafka.datastore.KafkaDatastoreProperties;
import org.talend.daikon.SimpleNamedThing;

public class KafkaDatasetDefinition extends SimpleNamedThing implements DatasetDefinition<KafkaDatasetProperties> {

    private static final String NAME = "KafkaDataset";

    public KafkaDatasetDefinition() {
        super(NAME);
    }

    @Override
    public KafkaDatasetProperties createProperties() {
        KafkaDatasetProperties properties = new KafkaDatasetProperties("kafkaDataset");
        properties.init();
        return properties;
    }

    @Override
    public RuntimeInfo getRuntimeInfo(KafkaDatasetProperties properties, Object ctx) {
        return null;
    }
}
