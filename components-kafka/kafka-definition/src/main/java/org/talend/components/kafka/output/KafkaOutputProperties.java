package org.talend.components.kafka.output;

import org.talend.components.api.component.PropertyPathConnector;
import org.talend.components.kafka.KafkaIOBasedProperties;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class KafkaOutputProperties extends KafkaIOBasedProperties {

    public KafkaOutputProperties(String name) {
        super(name);
    }

    @Override
    protected Set<PropertyPathConnector> getAllSchemaPropertiesConnectors(boolean isOutputConnection) {
        HashSet<PropertyPathConnector> connectors = new HashSet<>();
        if (isOutputConnection) {
            return Collections.EMPTY_SET;
        } else {
            connectors.add(MAIN_CONNECTOR);
        }
        return connectors;
    }
}
