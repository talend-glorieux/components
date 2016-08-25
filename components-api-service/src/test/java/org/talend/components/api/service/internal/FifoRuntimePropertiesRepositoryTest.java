package org.talend.components.api.service.internal;

import static org.junit.Assert.*;

import org.junit.Test;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.api.properties.ComponentPropertiesImpl;

/**
 * Unit test for the org.talend.components.api.service.internal.FifoRuntimePropertiesRepository class.
 *
 * @see FifoRuntimePropertiesRepository
 */
public class FifoRuntimePropertiesRepositoryTest {

    /** The map to test. */
    private FifoRuntimePropertiesRepository map = new FifoRuntimePropertiesRepository();

    @Test
    public void shouldAdd() throws Exception {
        // when
        map.add("1", randomProperties());

        // then
        assertTrue(map.contains("1"));
    }

    @Test
    public void shouldGetWhatWasAdded() throws Exception {

        // given
        ComponentProperties expected = randomProperties();

        // when
        map.add("2", expected);

        // then
        assertEquals(expected, map.get("2"));
    }

    @Test
    public void shouldNotExeedMaximumSize() throws Exception {
        // given
        map.add("first", randomProperties());

        // when
        for (int i=0; i<100; i++) {
            map.add("test-"+i, randomProperties());
        }

        // then
        assertFalse(map.contains("first"));
    }


    private ComponentProperties randomProperties() {
        return new ComponentPropertiesImpl("test-" + System.currentTimeMillis());
    }
}