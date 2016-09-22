package org.talend.components.common.io;

import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.common.FixedConnectorsComponentProperties;
import org.talend.components.common.dataset.DatasetProperties;
import org.talend.components.common.datastore.DatastoreProperties;
import org.talend.daikon.properties.Properties;

public abstract class IOProperties<DS extends DatasetProperties> extends FixedConnectorsComponentProperties {
    /**
     * FixedSchemaComponentProperties constructor comment.
     *
     * @param name
     */
    public IOProperties(String name) {
        super(name);
    }

    public abstract void setDatasetProperties(DS datasetProperties);
    public abstract DS getDatasetProperties();

}
