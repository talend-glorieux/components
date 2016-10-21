package org.talend.components.snowflake.test;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.api.component.runtime.BoundedReader;
import org.talend.components.api.component.runtime.Result;
import org.talend.components.api.component.runtime.Writer;
import org.talend.components.api.container.DefaultComponentRuntimeContainerImpl;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.api.service.ComponentService;
import org.talend.components.api.service.common.ComponentRegistry;
import org.talend.components.api.service.common.ComponentServiceImpl;
import org.talend.components.api.test.AbstractComponentTest;
import org.talend.components.api.test.ComponentTestUtils;
import org.talend.components.snowflake.SnowflakeConnectionProperties;
import org.talend.components.snowflake.SnowflakeConnectionTableProperties;
import org.talend.components.snowflake.SnowflakeFamilyDefinition;
import org.talend.components.snowflake.SnowflakeOutputProperties;
import org.talend.components.snowflake.SnowflakeTableProperties;
import org.talend.components.snowflake.runtime.SnowflakeSink;
import org.talend.components.snowflake.runtime.SnowflakeSource;
import org.talend.components.snowflake.runtime.SnowflakeSourceOrSink;
import org.talend.components.snowflake.runtime.SnowflakeWriteOperation;
import org.talend.components.snowflake.tsnowflakeinput.TSnowflakeInputDefinition;
import org.talend.components.snowflake.tsnowflakeinput.TSnowflakeInputProperties;
import org.talend.components.snowflake.tsnowflakeoutput.TSnowflakeOutputDefinition;
import org.talend.components.snowflake.tsnowflakeoutput.TSnowflakeOutputProperties;
import org.talend.daikon.properties.ValidationResult;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.property.Property;
import org.talend.daikon.properties.test.PropertiesTestUtils;

@SuppressWarnings("nls")
public class SnowflakeTest extends AbstractComponentTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeTest.class);

    protected RuntimeContainer adaptor;

    @Rule
    public ErrorCollector errorCollector = new ErrorCollector();

    private SnowflakeTestUtil testUtil = new SnowflakeTestUtil();

    private static Connection testConnection;

    private ComponentServiceImpl componentService;

    private static String accountStr = System.getProperty("snowflake.account");
    private static String user = System.getProperty("snowflake.user");
    private static String password = System.getProperty("snowflake.password");
    private static String schema = System.getProperty("snowflake.schema");
    private static String db = System.getProperty("snowflake.db");

    private static String TEST_TABLE = "LOADER_TEST_TABLE";

    // So that multiple tests can run at the same time
    private static String testTable = TEST_TABLE + "_" + Integer.toString(ThreadLocalRandom.current().nextInt(1, 100000));


    public SnowflakeTest() {
        adaptor = new DefaultComponentRuntimeContainerImpl();
    }

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

    public Writer<Result> createSnowflakeOutputWriter(TSnowflakeOutputProperties props) {
        SnowflakeSink SnowflakeSink = new SnowflakeSink();
        SnowflakeSink.initialize(adaptor, props);
        SnowflakeWriteOperation writeOperation = SnowflakeSink.createWriteOperation();
        Writer<Result> writer = writeOperation.createWriter(adaptor);
        return writer;
    }

    public <T> BoundedReader<T> createBoundedReader(ComponentProperties tsip) {
        SnowflakeSource SnowflakeSource = new SnowflakeSource();
        SnowflakeSource.initialize(null, tsip);
        SnowflakeSource.validate(null);
        return SnowflakeSource.createReader(null);
    }


    public ComponentProperties setupProps(SnowflakeConnectionProperties props) {
        if (props == null) {
            props = (SnowflakeConnectionProperties) new SnowflakeConnectionProperties("foo").init();
        }
        testUtil.initConnectionProps(props);
        return props;
    }

    @BeforeClass
    public static void setupDatabase() throws Exception {
        Class.forName("com.snowflake.client.jdbc.SnowflakeDriver");

        if (accountStr == null) {
            throw new Exception("This test expects snowflake.* system properties to be set. See the top of this class for the list of properties");
        }

        try {

            String connectionUrl = "jdbc:snowflake://" + accountStr +
                    ".snowflakecomputing.com";

            connectionUrl +=
                    "/?user=" +
                            user + "&password=" +
                            password + "&schema=" + schema +
                            "&db=" + db;

            Properties properties = new Properties();

            testConnection = DriverManager.getConnection(connectionUrl, properties);
            testConnection.createStatement().execute(
                    "CREATE OR REPLACE SCHEMA " + schema);
            testConnection.createStatement().execute(
                    "USE SCHEMA " + schema);
            testConnection.createStatement().execute(
                    "DROP TABLE IF EXISTS " + schema +
                            "." + testTable +
                            " CASCADE");
            testConnection.createStatement().execute(
                    "CREATE TABLE " + schema +
                            "." + testTable +
                            " ("
                            + "ID int, "
                            + "C1 varchar(255), "
                            + "C2 varchar(255) DEFAULT 'X', "
                            + "C3 double, "
                            + "C4 timestamp, "
                            + "C5 variant)");
        }
        catch (Exception ex) {
            throw new Exception("Make sure the system properties are correctly set as they might have caused this error", ex);
        }
    }


    @AfterClass
    public static void teardownDatabase() throws SQLException {
        if (false) {
            testConnection.createStatement().execute(
                    "DROP TABLE IF EXISTS " + schema +
                            "." + testTable);
            testConnection.createStatement().execute(
                    "DROP SCHEMA IF EXISTS " + schema);
            testConnection.close();
        }
    }

    public Schema getMakeRowSchema() {
        SchemaBuilder.FieldAssembler<Schema> fa = SchemaBuilder.builder().record("MakeRowRecord").fields() //
                .name("ID").type().nullable().intType().noDefault() //
                .name("C1").type().nullable().stringType().noDefault() //
                .name("C2").type().nullable().stringType().noDefault() //
                .name("C3").type().nullable().doubleType().noDefault() //
                // timestamp
                .name("C4").type().nullable().stringType().noDefault() //
                // variant
                .name("C5").type().nullable().stringType().noDefault();
        return fa.endRecord();
    }

    public List<IndexedRecord> makeRows(int count) {
        List<IndexedRecord> outputRows = new ArrayList<>();
        Random rnd = new Random();
        for (int i = 0; i < count; i++) {
            GenericData.Record row = new GenericData.Record(getMakeRowSchema());

            final String json = "{\"key\":" + String.valueOf(rnd.nextInt()) + ","
                    + "\"bar\":" + i + "}";
            row.put("ID", i);
            row.put("C1", "foo_" + i);
            row.put("C2", rnd.nextInt() / 3);
            row.put("C3", new Date());
            row.put("C4", json);

            outputRows.add(row);
        }
        return outputRows;
    }

    protected List<IndexedRecord> checkRows(String random, List<IndexedRecord> rows, int count) {
        List<IndexedRecord> checkedRows = new ArrayList<>();

        Schema rowSchema = null;
        int iName = 0;
        int iId = 0;
        int iBillingPostalCode = 0;
        int iBillingStreet = 0;
        int iShippingStreet = 0;
        int iShippingState = 0;
        int iBillingState = 0;

        int checkCount = 0;
        for (IndexedRecord row : rows) {
            if (rowSchema == null) {
                rowSchema = row.getSchema();
                iName = rowSchema.getField("Name").pos();
                iId = rowSchema.getField("Id").pos();
                iBillingPostalCode = rowSchema.getField("BillingPostalCode").pos();
                iBillingStreet = rowSchema.getField("BillingStreet").pos();
                iBillingState = rowSchema.getField("BillingState").pos();
                iShippingStreet = rowSchema.getField("ShippingStreet").pos();
                if (rowSchema.getField("ShippingState") != null) {
                    iShippingState = rowSchema.getField("ShippingState").pos();
                }
            }

            LOGGER.debug("check: " + row.get(iName) + " id: " + row.get(iId) + " post: " + row.get(iBillingPostalCode) + " st: "
                    + " post: " + row.get(iBillingStreet));
            String check = (String) row.get(iShippingStreet);
            if (check == null) {
                continue;
            }
            check = (String) row.get(iBillingPostalCode);
            if (check == null || !check.equals(random)) {
                continue;
            }
            checkCount++;
            if (rowSchema.getField("ShippingState") != null) {
                assertEquals("CA", row.get(iShippingState));
            }
            assertEquals("TestName", row.get(iName));
            assertEquals("123 Main Street", row.get(iBillingStreet));
            assertEquals("CA", row.get(iBillingState));
            checkedRows.add(row);
        }
        assertEquals(count, checkCount);
        return checkedRows;
    }

    public List<String> getDeleteIds(List<IndexedRecord> rows) {
        List<String> ids = new ArrayList<>();
        for (IndexedRecord row : rows) {
            LOGGER.debug("del: " + row.get(row.getSchema().getField("Name").pos()) + " id: "
                    + row.get(row.getSchema().getField("Id").pos()) + " post: "
                    + row.get(row.getSchema().getField("BillingPostalCode").pos()) + " st: " + " post: "
                    + row.get(row.getSchema().getField("BillingStreet").pos()));
            String check = (String) row.get(row.getSchema().getField("ShippingStreet").pos());
            if (check == null) {
                continue;
            }
            ids.add((String) row.get(row.getSchema().getField("Id").pos()));
        }
        return ids;
    }


    protected List<IndexedRecord> readRows(SnowflakeConnectionTableProperties inputProps) throws IOException {
        BoundedReader<IndexedRecord> reader = createBoundedReader(inputProps);
        boolean hasRecord = reader.start();
        List<IndexedRecord> rows = new ArrayList<>();
        while (hasRecord) {
            org.apache.avro.generic.IndexedRecord unenforced = reader.getCurrent();
            rows.add(unenforced);
            hasRecord = reader.advance();
        }
        return rows;
    }

    List<IndexedRecord> readAndCheckRows(String random, SnowflakeConnectionTableProperties props, int count) throws Exception {
        List<IndexedRecord> inputRows = readRows(props);
        return checkRows(random, inputRows, count);
    }

    protected void checkRows(List<IndexedRecord> outputRows, SnowflakeConnectionTableProperties props) throws Exception {
        List<IndexedRecord> inputRows = readRows(props);
        assertThat(inputRows, containsInAnyOrder(outputRows.toArray()));
    }

    protected void checkAndDelete(String random, SnowflakeConnectionTableProperties props, int count) throws Exception {
        List<IndexedRecord> inputRows = readAndCheckRows(random, props, count);
        deleteRows(inputRows, props);
        readAndCheckRows(random, props, 0);
    }

    public <T> T writeRows(Writer<T> writer, List<IndexedRecord> outputRows) throws IOException {
        T result;
        writer.open("foo");
        try {
            for (IndexedRecord row : outputRows) {
                writer.write(row);
            }
        } finally {
            result = writer.close();
        }
        return result;
    }

    // Returns the rows written (having been re-read so they have their Ids)
    protected Result doWriteRows(SnowflakeConnectionTableProperties props, List<IndexedRecord> outputRows) throws Exception {
        SnowflakeSink SnowflakeSink = new SnowflakeSink();
        SnowflakeSink.initialize(adaptor, props);
        SnowflakeSink.validate(adaptor);
        SnowflakeWriteOperation writeOperation = SnowflakeSink.createWriteOperation();
        Writer<Result> saleforceWriter = writeOperation.createWriter(adaptor);
        return writeRows(saleforceWriter, outputRows);
    }

    // Returns the rows written (having been re-read so they have their Ids)
    protected List<IndexedRecord> writeRows(String random, SnowflakeConnectionTableProperties props,
                                            List<IndexedRecord> outputRows) throws Exception {
        TSnowflakeOutputProperties outputProps = new TSnowflakeOutputProperties("output"); //$NON-NLS-1$
        outputProps.copyValuesFrom(props);
        outputProps.outputAction.setValue(TSnowflakeOutputProperties.OutputAction.INSERT);
        doWriteRows(outputProps, outputRows);
        return readAndCheckRows(random, props, outputRows.size());
    }

    protected void deleteRows(List<IndexedRecord> rows, SnowflakeConnectionTableProperties props) throws Exception {
        TSnowflakeOutputProperties deleteProperties = new TSnowflakeOutputProperties("delete"); //$NON-NLS-1$
        deleteProperties.copyValuesFrom(props);
        deleteProperties.outputAction.setValue(SnowflakeOutputProperties.OutputAction.DELETE);
        LOGGER.debug("deleting " + rows.size() + " rows");
        doWriteRows(deleteProperties, rows);
    }


    @Test
    public void testLogin() throws Throwable {
        SnowflakeConnectionProperties props = (SnowflakeConnectionProperties) setupProps(null);
        System.out.println(props);
        Form f = props.getForm(SnowflakeConnectionProperties.FORM_WIZARD);
        props = (SnowflakeConnectionProperties) PropertiesTestUtils.checkAndValidate(getComponentService(), f, "testConnection",
                props);
        LOGGER.debug(props.getValidationResult().toString());
        assertEquals(ValidationResult.Result.OK, props.getValidationResult().getStatus());
    }

    protected void checkAndSetupTable(SnowflakeConnectionTableProperties props) throws Throwable {

        assertEquals(2, props.getForms().size());
        Form f = props.table.getForm(Form.REFERENCE);
        assertTrue(f.getWidget("tableName").isCallBeforeActivate());

        SnowflakeTableProperties moduleProps = (SnowflakeTableProperties) f.getProperties();
        moduleProps = (SnowflakeTableProperties) PropertiesTestUtils.checkAndBeforeActivate(getComponentService(), f, "tableName",
                moduleProps);
        Property prop = (Property) f.getWidget("tableName").getContent();
        LOGGER.debug(prop.getPossibleValues().toString());
        LOGGER.debug(moduleProps.getValidationResult().toString());
        assertEquals(ValidationResult.Result.OK, moduleProps.getValidationResult().getStatus());
        assertTrue(prop.getPossibleValues().size() > 10);

        moduleProps.tableName.setValue(testTable);
        moduleProps = (SnowflakeTableProperties) PropertiesTestUtils.checkAndAfter(getComponentService(), f, "tableName", moduleProps);
        Schema schema = moduleProps.main.schema.getValue();
        LOGGER.debug(schema.toString());
        for (Schema.Field child : schema.getFields()) {
            LOGGER.debug(child.name());
        }
    }


    protected void populateOutput() throws Throwable {
        TSnowflakeOutputProperties props = (TSnowflakeOutputProperties) getComponentService()
                .getComponentProperties(TSnowflakeOutputDefinition.COMPONENT_NAME);
        setupProps(props.getConnectionProperties());
        checkAndSetupTable(props);
        props.outputAction.setStoredValue(SnowflakeOutputProperties.OutputAction.INSERT);
        props.afterOutputAction();

        Result result = doWriteRows(props, makeRows(100));
        assertEquals(100, result.getSuccessCount());
        assertEquals(0, result.getRejectCount());
    }


    @Test
    public void testTableNames() throws Throwable {
        TSnowflakeInputProperties props = (TSnowflakeInputProperties) getComponentService()
                .getComponentProperties(TSnowflakeInputDefinition.COMPONENT_NAME);
        setupProps(props.getConnectionProperties());
        ComponentTestUtils.checkSerialize(props, errorCollector);
        checkAndSetupTable(props);
    }

    @Test
    public void testGetSchema() throws IOException {
        SnowflakeConnectionProperties scp = (SnowflakeConnectionProperties) setupProps(null);
        Schema schema = SnowflakeSourceOrSink.getSchema(null, scp, testTable);
        assertNotNull(schema);
        assertThat(schema.getFields(), hasSize(5));
    }

    @Test
    public void testOutputInsert() throws Throwable {
        populateOutput();
    }

    @Test
    public void testOutputDelete() throws Throwable {
        populateOutput();
    }


    @Ignore("test not finished")
    @Test
    public void testOutputUpsert() throws Throwable {
        TSnowflakeOutputProperties props = (TSnowflakeOutputProperties) getComponentService()
                .getComponentProperties(TSnowflakeOutputDefinition.COMPONENT_NAME);
        setupProps(props.getConnectionProperties());
        props.outputAction.setStoredValue(SnowflakeOutputProperties.OutputAction.UPSERT);
        props.afterOutputAction();

        Property se = (Property) props.getProperty("upsertKeyColumn");
        assertTrue(se.getPossibleValues().size() > 10);

        Writer<Result> saleforceWriter = createSnowflakeOutputWriter(props);

        Map<String, Object> row = new HashMap<>();
        row.put("Name", "TestName");
        row.put("BillingStreet", "123 Main Street");
        row.put("BillingState", "CA");
        List<Map<String, Object>> outputRows = new ArrayList<>();
        outputRows.add(row);
        // FIXME - finish this test
        // WriterResult writeResult = SnowflakeTestHelper.writeRows(saleforceWriter, outputRows);
    }


}
