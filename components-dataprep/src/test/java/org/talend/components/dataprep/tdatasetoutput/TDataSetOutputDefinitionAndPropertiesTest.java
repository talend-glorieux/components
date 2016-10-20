package org.talend.components.dataprep.tdatasetoutput;

import java.util.Collections;

import javax.inject.Inject;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.talend.components.api.component.Connector;
import org.talend.components.api.component.PropertyPathConnector;
import org.talend.components.api.service.ComponentService;
import org.talend.components.service.spring.SpringTestApp;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = SpringTestApp.class)
public class TDataSetOutputDefinitionAndPropertiesTest {

    @Inject
    private ComponentService componentService;

    @Test
    public void testTDataSetOutputDefinition() {
        TDataSetOutputDefinition outputDefinition = (TDataSetOutputDefinition) componentService
                .getComponentDefinition("tDatasetOutput");
        Assert.assertArrayEquals(new String[] { "Talend Data Preparation" }, outputDefinition.getFamilies());
        Assert.assertTrue(outputDefinition.isSchemaAutoPropagate());
    }

    @Test
    public void testTDataSetOutputProperties() {
        TDataSetOutputProperties properties = (TDataSetOutputProperties) componentService
                .getComponentProperties("tDatasetOutput");
        PropertyPathConnector connector = new PropertyPathConnector(Connector.MAIN_NAME, "schema");

        Assert.assertNotNull(properties.getSchema());
        Assert.assertEquals(Collections.emptySet(), properties.getAllSchemaPropertiesConnectors(true));
        Assert.assertEquals(Collections.singleton(connector), properties.getAllSchemaPropertiesConnectors(false));
    }
}
