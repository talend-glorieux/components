package org.talend.components.kafka.output;

import org.talend.components.api.component.ConnectorTopology;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.kafka.KafkaIODefinition;

import java.util.EnumSet;
import java.util.Set;

public class KafkaOutputDefinition extends KafkaIODefinition {
    public KafkaOutputDefinition(String componentName) {
        super(componentName);
    }

    @Override
    public Class<? extends ComponentProperties> getPropertyClass() {
        return KafkaOutputProperties.class;
    }

    @Override
    public Set<ConnectorTopology> getSupportedConnectorTopologies() {
        return EnumSet.of(ConnectorTopology.INCOMING);
    }
}
