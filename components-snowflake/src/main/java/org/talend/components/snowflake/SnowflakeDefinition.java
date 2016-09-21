package org.talend.components.snowflake;

import org.talend.components.api.component.AbstractComponentDefinition;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.daikon.properties.property.Property;

/**
 * The SnowflakeDefinition acts as an entry point for all of services that
 * a component provides to integrate with the Studio (at design-time) and other
 * components (at run-time).
 */
public abstract class SnowflakeDefinition extends AbstractComponentDefinition {

    public SnowflakeDefinition(String componentName) {
        super(componentName);
    }

    @Override
    public String[] getFamilies() {
        return new String[] { "Cloud/Snowflake" }; //$NON-NLS-1$
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<? extends ComponentProperties>[] getNestedCompatibleComponentPropertiesClass() {
        return new Class[] { SnowflakeConnectionProperties.class };
    }

    @Override
    public Property[] getReturnProperties() {
        return new Property[] { RETURN_ERROR_MESSAGE_PROP, RETURN_TOTAL_RECORD_COUNT_PROP };
    }

    public String getMavenGroupId() {
        return "org.talend.components";
    }

    public String getMavenArtifactId() {
        return "components-snowflake";
    }

}
