package org.talend.components.snowflake;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.junit.*;
import org.junit.rules.ErrorCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.api.component.runtime.*;
import org.talend.components.api.container.DefaultComponentRuntimeContainerImpl;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.api.service.ComponentService;
import org.talend.components.api.service.internal.ComponentRegistry;
import org.talend.components.api.service.internal.ComponentServiceImpl;
import org.talend.components.api.test.AbstractComponentTest;
import org.talend.components.api.test.ComponentTestUtils;
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
    public static void setupDatabase() throws ClassNotFoundException, SQLException {
        Class.forName("com.snowflake.client.jdbc.SnowflakeDriver");

        String accountStr = System.getProperty("snowflake.account");
        String user = System.getProperty("snowflake.user");
        String password = System.getProperty("snowflake.password");

        String connectionUrl = "jdbc:snowflake://" + accountStr +
                ".snowflakecomputing.com";

        connectionUrl +=
                "/?user=" +
                        user + "&password=" +
                        password + "&schema=LOADER&db=TEST_DB";

        Properties properties = new Properties();

        testConnection = DriverManager.getConnection(connectionUrl, properties);
        testConnection.createStatement().execute(
                "CREATE OR REPLACE SCHEMA LOADER");
        testConnection.createStatement().execute(
                "USE SCHEMA LOADER");
        testConnection.createStatement().execute(
                "DROP TABLE IF EXISTS LOADER." + testTable +
                        " CASCADE");
        testConnection.createStatement().execute(
                "CREATE TABLE LOADER." + testTable +
                        " ("
                        + "ID int, "
                        + "C1 varchar(255), "
                        + "C2 varchar(255) DEFAULT 'X', "
                        + "C3 double, "
                        + "C4 timestamp, "
                        + "C5 variant)");
    }


    @AfterClass
    public static void teardownDatabase() throws SQLException {
        if (!false) {
            testConnection.createStatement().execute(
                    "DROP TABLE IF EXISTS LOADER." + testTable);
            testConnection.createStatement().execute(
                    "DROP SCHEMA IF EXISTS LOADER");
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
            row.put("ID",i);
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


    protected List<IndexedRecord> readRows(TSnowflakeInputProperties inputProps) throws IOException {
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

    protected List<IndexedRecord> readRows(SnowflakeConnectionTableProperties props) throws IOException {
        TSnowflakeInputProperties inputProps = (TSnowflakeInputProperties) new TSnowflakeInputProperties("bar").init();
        inputProps.connection = props.connection;
        inputProps.table = props.table;
        List<IndexedRecord> inputRows = readRows(inputProps);
        return inputRows;
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
    protected void doWriteRows(SnowflakeConnectionTableProperties props, List<IndexedRecord> outputRows) throws Exception {
        SnowflakeSink SnowflakeSink = new SnowflakeSink();
        SnowflakeSink.initialize(adaptor, props);
        SnowflakeSink.validate(adaptor);
        SnowflakeWriteOperation writeOperation = SnowflakeSink.createWriteOperation();
        Writer<Result> saleforceWriter = writeOperation.createWriter(adaptor);
        writeRows(saleforceWriter, outputRows);
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

    @Test
    public void testTableNames() throws Throwable {
        TSnowflakeInputProperties props = (TSnowflakeInputProperties) getComponentService()
                .getComponentProperties(TSnowflakeInputDefinition.COMPONENT_NAME);
        setupProps(props.getConnectionProperties());
        ComponentTestUtils.checkSerialize(props, errorCollector);

        assertEquals(2, props.getForms().size());
        Form f = props.table.getForm(Form.REFERENCE);
        assertTrue(f.getWidget("tableName").isCallBeforeActivate());
        // The Form is bound to a Properties object that created it. The Forms might not always be associated with the
        // properties object
        // they came from.
        ComponentProperties moduleProps = (ComponentProperties) f.getProperties();
        moduleProps = (ComponentProperties) PropertiesTestUtils.checkAndBeforeActivate(getComponentService(), f, "tableName",
                moduleProps);
        Property prop = (Property) f.getWidget("tableName").getContent();
        LOGGER.debug(prop.getPossibleValues().toString());
        LOGGER.debug(moduleProps.getValidationResult().toString());
        assertEquals(ValidationResult.Result.OK, moduleProps.getValidationResult().getStatus());
        assertTrue(prop.getPossibleValues().size() > 10);
    }

    @Test
    public void testGetSchema() throws IOException {
        SnowflakeConnectionProperties scp = (SnowflakeConnectionProperties) setupProps(null);
        Schema schema = SnowflakeSourceOrSink.getSchema(null, scp, testTable);
        assertNotNull(schema);
        assertThat(schema.getFields(), hasSize(5));
    }

    @Test
    @Ignore("not finished")
    public void testOutput() throws Throwable {
        TSnowflakeOutputProperties props = (TSnowflakeOutputProperties) getComponentService()
                .getComponentProperties(TSnowflakeOutputDefinition.COMPONENT_NAME);
        setupProps(props.getConnectionProperties());
        props.table.tableName.setStoredValue(testTable);
        props.outputAction.setStoredValue(SnowflakeOutputProperties.OutputAction.INSERT);
        props.afterOutputAction();

        doWriteRows(props, makeRows(100));
        Writer<Result> writer = createSnowflakeOutputWriter(props);
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
