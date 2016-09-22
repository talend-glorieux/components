package org.talend.components.kafka;

import aQute.bnd.annotation.component.Component;
import org.talend.components.api.AbstractComponentFamilyDefinition;
import org.talend.components.api.ComponentInstaller;
import org.talend.components.api.Constants;
import org.talend.components.kafka.dataset.KafkaDatasetDefinition;
import org.talend.components.kafka.datastore.KafkaDatastoreDefinition;

/**
 * Install all of the definitions provided for the Kafka family of components.
 */
@Component(name = Constants.COMPONENT_INSTALLER_PREFIX + KafkaFamilyDefinition.NAME, provide = ComponentInstaller.class)
public class KafkaFamilyDefinition extends AbstractComponentFamilyDefinition implements ComponentInstaller {
    public static final String NAME = "Kafka";

    public KafkaFamilyDefinition() {
        super(NAME, new KafkaDatastoreDefinition(), new KafkaDatasetDefinition());
    }

    public void install(ComponentFrameworkContext ctx) {
        ctx.registerComponentFamilyDefinition(this);
    }
}
