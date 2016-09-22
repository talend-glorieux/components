package org.talend.components.common.dataset;

import org.talend.components.common.datastore.DatastoreProperties;
import org.talend.daikon.properties.Properties;

/**
 * Placeholder for DatasetProperties.
 */
public interface DatasetProperties<T extends DatastoreProperties> extends Properties {
    public void setDatastoreProperties(T datastoreProperties);
    public T getDatastoreProperties();
}
