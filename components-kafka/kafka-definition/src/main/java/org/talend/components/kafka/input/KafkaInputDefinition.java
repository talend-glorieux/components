package org.talend.components.kafka.input;

import org.talend.components.api.component.ConnectorTopology;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.kafka.KafkaIOBasedProperties;
import org.talend.components.kafka.KafkaIODefinition;

import java.util.EnumSet;
import java.util.Set;

public class KafkaInputDefinition extends KafkaIODefinition {

    public KafkaInputDefinition(String componentName) {
        super(componentName);
    }

    @Override
    public Class<? extends ComponentProperties> getPropertyClass() {
        return KafkaIOBasedProperties.class;
    }

    @Override
    public Set<ConnectorTopology> getSupportedConnectorTopologies() {
        return EnumSet.of(ConnectorTopology.OUTGOING);
    }
}