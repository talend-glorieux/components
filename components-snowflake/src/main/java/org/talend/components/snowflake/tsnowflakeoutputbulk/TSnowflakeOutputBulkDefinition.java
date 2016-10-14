package org.talend.components.snowflake.tsnowflakeoutputbulk;

import java.util.EnumSet;
import java.util.Set;

import org.talend.components.api.component.ConnectorTopology;
import org.talend.components.api.component.runtime.DependenciesReader;
import org.talend.components.api.component.runtime.RuntimeInfo;
import org.talend.components.api.component.runtime.SimpleRuntimeInfo;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.snowflake.SnowflakeDefinition;
import org.talend.components.snowflake.SnowflakeTableProperties;
import org.talend.components.snowflake.runtime.SnowflakeSink;
import org.talend.daikon.properties.Properties;
import org.talend.daikon.properties.property.Property;

/**
 * Component that can connect to a snowflake system and put some data into it.
 */

public class TSnowflakeOutputBulkDefinition extends SnowflakeDefinition {

    public static final String COMPONENT_NAME = "tSnowflakeOutputBulk"; //$NON-NLS-1$

    public TSnowflakeOutputBulkDefinition() {
        super(COMPONENT_NAME);
    }

    @Override
    public boolean isSchemaAutoPropagate() {
        return false;
    }

    @Override
    public boolean isConditionalInputs() {
        return true;
    }

    @Override
    public String getPartitioning() {
        return AUTO;
    }

    @Override
    public Class<? extends ComponentProperties> getPropertyClass() {
        return TSnowflakeOutputBulkProperties.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<? extends ComponentProperties>[] getNestedCompatibleComponentPropertiesClass() {
        return concatPropertiesClasses(super.getNestedCompatibleComponentPropertiesClass(),
                new Class[] { SnowflakeTableProperties.class });
    }

    @Override
    public Property[] getReturnProperties() {
        return new Property[] { RETURN_ERROR_MESSAGE_PROP, RETURN_TOTAL_RECORD_COUNT_PROP, RETURN_SUCCESS_RECORD_COUNT_PROP,
                RETURN_REJECT_RECORD_COUNT_PROP };
    }

    @Override
    public RuntimeInfo getRuntimeInfo(Properties properties, ConnectorTopology componentType) {
        if (componentType == ConnectorTopology.INCOMING || componentType == ConnectorTopology.INCOMING_AND_OUTGOING) {
            return new SimpleRuntimeInfo(this.getClass().getClassLoader(),
                    DependenciesReader.computeDependenciesFilePath(getMavenGroupId(), getMavenArtifactId()),
                    SnowflakeSink.class.getCanonicalName());
        } else {
            return null;
        }
    }

    @Override
    public Set<ConnectorTopology> getSupportedConnectorTopologies() {
        return EnumSet.of(ConnectorTopology.INCOMING, ConnectorTopology.INCOMING_AND_OUTGOING);
    }
}
