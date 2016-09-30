package org.talend.components.kafka;

import org.talend.components.api.component.AbstractComponentDefinition;
import org.talend.components.api.component.ConnectorTopology;
import org.talend.components.api.component.runtime.RuntimeInfo;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.daikon.properties.Properties;
import org.talend.daikon.properties.property.Property;

import java.util.Set;

public abstract class KafkaIOBasedDefinition extends AbstractComponentDefinition {
    public KafkaIOBasedDefinition(String componentName) {
        super(componentName);
    }

    @Override
    public Property[] getReturnProperties() {
        return new Property[0];
    }

    @Override
    public RuntimeInfo getRuntimeInfo(Properties properties, ConnectorTopology connectorTopology) {
        return null;
    }

}
