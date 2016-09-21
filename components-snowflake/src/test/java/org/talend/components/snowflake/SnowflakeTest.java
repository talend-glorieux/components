package org.talend.components.snowflake;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.PrintWriter;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.api.component.runtime.BoundedSource;
import org.talend.components.api.component.runtime.Reader;
import org.talend.components.api.component.runtime.Source;
import org.talend.components.api.service.ComponentService;
import org.talend.components.api.service.internal.ComponentRegistry;
import org.talend.components.api.service.internal.ComponentServiceImpl;
import org.talend.components.api.test.AbstractComponentTest;
import org.talend.daikon.properties.ValidationResult;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.test.PropertiesTestUtils;

@SuppressWarnings("nls")
public class SnowflakeTest extends AbstractComponentTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeTest.class);

    @Rule
    public ErrorCollector errorCollector = new ErrorCollector();

    private SnowflakeTestUtil testUtil = new SnowflakeTestUtil();

    private ComponentServiceImpl componentService;

    @Before
    public void initializeComponentRegistryAndService() {
        // reset the component service
        componentService = null;
    }

    @Override
    public ComponentService getComponentService() {
        if (componentService == null) {
            ComponentRegistry testComponentRegistry = new ComponentRegistry();
            // register component
            testComponentRegistry.registerComponentFamilyDefinition(new SnowflakeFamilyDefinition());
            componentService = new ComponentServiceImpl(testComponentRegistry);
        }
        return componentService;
    }

    public SnowflakeConnectionProperties setupProps(SnowflakeConnectionProperties props, boolean addQuotes) {
        if (props == null) {
            props = (SnowflakeConnectionProperties) new SnowflakeConnectionProperties("foo").init();
        }
        testUtil.initConnectionProps(props);
        return props;
    }

    @Test
    public void testLogin() throws Throwable {
        SnowflakeConnectionProperties props = setupProps(null, false);
        Form f = props.getForm(SnowflakeConnectionProperties.FORM_WIZARD);
        props = (SnowflakeConnectionProperties) PropertiesTestUtils.checkAndValidate(getComponentService(), f, "testConnection",
                props);
        LOGGER.debug(props.getValidationResult().toString());
        assertEquals(ValidationResult.Result.OK, props.getValidationResult().getStatus());
    }

    @Ignore
    @Test
    public void testSnowflakeRuntime() throws Exception {
        SnowflakeDefinition def = (SnowflakeDefinition) getComponentService().getComponentDefinition("Snowflake");
        SnowflakeConnectionProperties props = (SnowflakeConnectionProperties) getComponentService()
                .getComponentProperties("Snowflake");

        // Set up the test schema - not really used for anything now
        Schema schema = SchemaBuilder.builder().record("testRecord").fields().name("field1").type().stringType().noDefault()
                .endRecord();
        props.schema.setStoredValue(schema);
        testUtil.initConnectionProps(props);

        File temp = File.createTempFile("SnowflaketestFile", ".txt");
        try {
            PrintWriter writer = new PrintWriter(temp.getAbsolutePath(), "UTF-8");
            writer.println("The first line");
            writer.println("The second line");
            writer.close();

            // props.filename.setValue(temp.getAbsolutePath());
            Source source = null;// def.getRuntime();
            source.initialize(null, props);
            // assertThat(source, instanceOf(SnowflakeSource.class));

            Reader<?> reader = ((BoundedSource) source).createReader(null);
            assertThat(reader.start(), is(true));
            assertThat(reader.getCurrent(), is((Object) "The first line"));
            // No auto advance when calling getCurrent more than once.
            assertThat(reader.getCurrent(), is((Object) "The first line"));
            assertThat(reader.advance(), is(true));
            assertThat(reader.getCurrent(), is((Object) "The second line"));
            assertThat(reader.advance(), is(false));
        } finally {// remote the temp file
            temp.delete();
        }
    }

}
