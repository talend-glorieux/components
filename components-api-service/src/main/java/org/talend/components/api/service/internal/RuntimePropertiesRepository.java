package org.talend.components.api.service.internal;

import org.talend.daikon.properties.Properties;

/**
 * Repository used to store properties for a component runtime.
 */
public interface RuntimePropertiesRepository {

    /**
     * Store the given properties to the given id.
     *
     * @param runtimeId the runtime id.
     * @param properties the properties to store.
     */
    void add(String runtimeId, Properties properties);

    /**
     * Return the properties out of the given runtime id.
     *
     * @param runtimeId the runtime id.
     * @return the properties out of the given runtime id or null if there's none.
     */
    Properties get(String runtimeId);

    /**
     * Return true if the repository has access to the properties for the given runtime id.
     * @param runtimeId the wanted runtime id.
     * @return true if the repository has access to the properties for the given runtime id.
     */
    boolean contains(String runtimeId);
}
