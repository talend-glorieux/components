package org.talend.components.api.service.internal;

import java.util.LinkedHashMap;
import java.util.Map;

import org.talend.components.api.properties.ComponentProperties;

/**
 * FIFO implementation of the RuntimePropertiesRepository.
 *
 * When the maximum number of properties is reached, the eldest elements a removed.
 */
public class FifoRuntimePropertiesRepository implements RuntimePropertiesRepository {

    /** Maximum number of properties to store.*/
    private static final int MAP_SIZE=100;

    /** Where the properties are stored. */
    private LinkedHashMap<String, ComponentProperties> map;

    /**
     * Default empty constructor.
     */
    public FifoRuntimePropertiesRepository() {
        map = new LinkedHashMap<String, ComponentProperties>(MAP_SIZE) {

            @Override
            protected boolean removeEldestEntry(Map.Entry<String, ComponentProperties> entry) {
                return size() > MAP_SIZE;
            }
        };
    }


    @Override
    public void add(String runtimeId, ComponentProperties properties) {
        if (!map.containsKey(runtimeId)) {
            map.put(runtimeId, properties);
        }
    }

    @Override
    public ComponentProperties get(String runtimeId) {
        return map.get(runtimeId);
    }

    @Override
    public boolean contains(String runtimeId) {
        return map.containsKey(runtimeId);
    }

}
