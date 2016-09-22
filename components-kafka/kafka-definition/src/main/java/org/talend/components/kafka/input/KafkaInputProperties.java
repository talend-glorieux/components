package org.talend.components.kafka.input;

import org.talend.components.api.component.PropertyPathConnector;
import org.talend.components.kafka.KafkaIOBasedProperties;

import java.util.Collections;
import java.util.Set;

public class KafkaInputProperties extends KafkaIOBasedProperties {

    public KafkaInputProperties(String name) {
        super(name);
    }

    @Override
    protected Set<PropertyPathConnector> getAllSchemaPropertiesConnectors(boolean isOutputConnection) {
        if (isOutputConnection) {
            return Collections.singleton(MAIN_CONNECTOR);
        } else {
            return Collections.EMPTY_SET;
        }
    }
}
