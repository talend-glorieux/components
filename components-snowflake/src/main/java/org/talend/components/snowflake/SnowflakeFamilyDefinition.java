package org.talend.components.snowflake;

import org.talend.components.api.AbstractComponentFamilyDefinition;
import org.talend.components.api.ComponentInstaller;
import org.talend.components.api.Constants;
import org.talend.components.snowflake.tsnowflakeconnection.TSnowflakeConnectionDefinition;
import org.talend.components.snowflake.tsnowflakeinput.TSnowflakeInputDefinition;
import org.talend.components.snowflake.tsnowflakeoutputbulk.TSnowflakeOutputBulkDefinition;

import aQute.bnd.annotation.component.Component;

/**
 * Install all of the definitions provided for the Snowflake family of components.
 */
@Component(name = Constants.COMPONENT_INSTALLER_PREFIX + SnowflakeFamilyDefinition.NAME, provide = ComponentInstaller.class)
public class SnowflakeFamilyDefinition extends AbstractComponentFamilyDefinition implements ComponentInstaller {

    public static final String NAME = "Snowflake";

    public SnowflakeFamilyDefinition() {
        super(NAME,
                // Components
                new TSnowflakeConnectionDefinition(), new TSnowflakeInputDefinition(), new TSnowflakeOutputBulkDefinition(),
                // Component wizards
                new SnowflakeConnectionWizardDefinition());
    }

    @Override
    public void install(ComponentFrameworkContext ctx) {
        ctx.registerComponentFamilyDefinition(this);
    }

}
