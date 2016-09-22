package org.talend.components.kafka.dataset;

import org.junit.Before;
import org.junit.Test;
import org.talend.daikon.SimpleNamedThing;

import static org.junit.Assert.*;

public class KafkaDatasetPropertiesTest {

    KafkaDatasetProperties cp;

    @Before
    public void init() {
        KafkaDatasetDefinition definition = new KafkaDatasetDefinition();
        cp = definition.createProperties();
    }

    @Test
    public void test() {
        assertNotNull(cp.getDatastoreProperties());
        assertNull(cp.topic.getValue());
        assertTrue(cp.main.schema.getValue().getFields().isEmpty());

        cp.beforeTopic();
        assertNotNull(cp.getDatastoreProperties());
        assertEquals(2, cp.topic.getPossibleValues().size());
        assertTrue(cp.main.schema.getValue().getFields().isEmpty());

        cp.topic.setValue(((SimpleNamedThing)cp.topic.getPossibleValues().get(0)).getName());
        cp.afterTopic();
        assertNotNull(cp.getDatastoreProperties());
        assertEquals(2, cp.topic.getPossibleValues().size());
        assertFalse(cp.main.schema.getValue().getFields().isEmpty());
    }
}