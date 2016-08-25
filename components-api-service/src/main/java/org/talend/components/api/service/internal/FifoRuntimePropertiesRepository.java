package org.talend.components.api.service.internal;

import java.util.LinkedHashMap;
import java.util.Map;

import org.talend.daikon.properties.Properties;

/**
 * FIFO implementation of the RuntimePropertiesRepository.
 *
 * When the maximum number of properties is reached, the eldest elements a removed.
 */
public class FifoRuntimePropertiesRepository implements RuntimePropertiesRepository {

    /** Maximum number of properties to store.*/
    private static final int MAP_SIZE=100;

    /** Where the properties are stored. */
    private LinkedHashMap<String, Properties> map;

    /**
     * Default empty constructor.
     */
    public FifoRuntimePropertiesRepository() {
        map = new LinkedHashMap<String, Properties>(MAP_SIZE) {

            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Properties> entry) {
                return size() > MAP_SIZE;
            }
        };
    }


    @Override
    public void add(String runtimeId, Properties properties) {
        if (!map.containsKey(runtimeId)) {
            map.put(runtimeId, properties);
        }
    }

    @Override
    public Properties get(String runtimeId) {
        return map.get(runtimeId);
    }

    @Override
    public boolean contains(String runtimeId) {
        return map.containsKey(runtimeId);
    }

}
