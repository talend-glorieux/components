package org.talend.components.snowflake.test;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.math.BigDecimal;
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
import org.junit.*;
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
public class SnowflakeTestIT extends AbstractComponentTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeTestIT.class);

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


    public SnowflakeTestIT() {
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
                            + "ID int PRIMARY KEY, "
                            + "C1 varchar(255), "
                            + "C2 varchar(255) DEFAULT 'X', "
                            + "C3 double, "
                            + "C4 timestamp, "
                            + "C5 variant)");
        } catch (Exception ex) {
            throw new Exception("Make sure the system properties are correctly set as they might have caused this error", ex);
        }
    }

    @AfterClass
    public static void teardownDatabase() throws SQLException {
        if (!false) {
            testConnection.createStatement().execute(
                    "DROP TABLE IF EXISTS " + schema +
                            "." + testTable);
            testConnection.createStatement().execute(
                    "DROP SCHEMA IF EXISTS " + schema);
            testConnection.close();
        }
    }


    @After
    public void tearDown() throws SQLException {
        if (!false) {
            testConnection.createStatement().execute(
                    "DELETE FROM " + schema +
                            "." + testTable);
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

    public IndexedRecord makeRow(int i, Random rnd) {
        GenericData.Record row = new GenericData.Record(getMakeRowSchema());

        final String json = "{\"key\":" + String.valueOf(rnd.nextInt()) + ","
                + "\"bar\":" + i + "}";
        row.put("ID", i);
        row.put("C1", "foo_" + i);
        row.put("C3", rnd.nextInt() / 3);
        row.put("C4", new Date());
        row.put("C5", json);
        return row;
    }

    public List<IndexedRecord> makeRows(int count) {
        List<IndexedRecord> outputRows = new ArrayList<>();
        Random rnd = new Random();
        for (int i = 0; i < count; i++) {
            GenericData.Record row = (GenericData.Record) makeRow(i, rnd);
            outputRows.add(row);
        }
        return outputRows;
    }

    protected List<IndexedRecord> checkRows(List<IndexedRecord> rows, int count) {
        List<IndexedRecord> checkedRows = new ArrayList<>();

        Schema rowSchema = null;
        int iId = 0;
        int iC1 = 0;
        int iC2 = 0;
        int iC3 = 0;
        int iC4 = 0;
        int iC5 = 0;

        int checkCount = 0;
        for (IndexedRecord row : rows) {
            if (rowSchema == null) {
                rowSchema = row.getSchema();
                iId = rowSchema.getField("ID").pos();
                iC1 = rowSchema.getField("C1").pos();
                iC2 = rowSchema.getField("C2").pos();
                iC3 = rowSchema.getField("C3").pos();
                iC4 = rowSchema.getField("C4").pos();
                iC5 = rowSchema.getField("C5").pos();
            }

            if (false) {
                LOGGER.debug("check - id: " + row.get(iId) + " C1: " + row.get(iC1) + " C2: " + row.get(iC2) + " C3: " + row.get(iC3) + " C4: " + row.get(iC4) + " C5: " + row.get(iC5));
            }
            assertEquals(BigDecimal.valueOf(checkCount++), row.get(iId));
            assertNotNull(row.get(iC1));
            assertNotNull(row.get(iC3));
            assertNotNull(row.get(iC4));
            assertNotNull(row.get(iC5));
            checkedRows.add(row);
        }
        assertEquals(count, checkCount);
        return checkedRows;
    }

    public List<String> getDeleteIds(List<IndexedRecord> rows) {
        List<String> ids = new ArrayList<>();
        for (IndexedRecord row : rows) {
            String check = (String) row.get(row.getSchema().getField("ID").pos());
            if (check == null) {
                continue;
            }
            ids.add((String) row.get(row.getSchema().getField("ID").pos()));
        }
        return ids;
    }


    protected List<IndexedRecord> readRows(SnowflakeConnectionTableProperties props) throws IOException {
        TSnowflakeInputProperties inputProps = (TSnowflakeInputProperties) new TSnowflakeInputProperties("bar").init();
        inputProps.connection = props.connection;
        inputProps.table = props.table;
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

    List<IndexedRecord> readAndCheckRows(SnowflakeConnectionTableProperties props, int count) throws Exception {
        List<IndexedRecord> inputRows = readRows(props);
        return checkRows(inputRows, count);
    }

    protected void checkRows(List<IndexedRecord> outputRows, SnowflakeConnectionTableProperties props) throws Exception {
        List<IndexedRecord> inputRows = readRows(props);
        assertThat(inputRows, containsInAnyOrder(outputRows.toArray()));
    }

    protected void checkAndDelete(String random, SnowflakeConnectionTableProperties props, int count) throws Exception {
        List<IndexedRecord> inputRows = readAndCheckRows(props, count);
        deleteRows(inputRows, props);
        readAndCheckRows(props, 0);
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

    public <T> T makeAndWriteRows(Writer<T> writer, int count) throws IOException {
        Random rnd = new Random();
        T result;
        writer.open("foo");
        try {
            for (int i = 0; i < count; i++) {
                IndexedRecord row = makeRow(i, rnd);
                writer.write(row);
            }
        } finally {
            result = writer.close();
        }
        return result;
    }


    // Returns the rows written (having been re-read so they have their Ids)
    protected Writer<Result> makeWriter(SnowflakeConnectionTableProperties props) throws Exception {
        SnowflakeSink SnowflakeSink = new SnowflakeSink();
        SnowflakeSink.initialize(adaptor, props);
        SnowflakeSink.validate(adaptor);
        SnowflakeWriteOperation writeOperation = SnowflakeSink.createWriteOperation();
        return writeOperation.createWriter(adaptor);
    }

    // Returns the rows written (having been re-read so they have their Ids)
    protected List<IndexedRecord> writeRows(SnowflakeConnectionTableProperties props,
                                            List<IndexedRecord> outputRows) throws Exception {
        TSnowflakeOutputProperties outputProps = new TSnowflakeOutputProperties("output"); //$NON-NLS-1$
        outputProps.copyValuesFrom(props);
        outputProps.outputAction.setValue(TSnowflakeOutputProperties.OutputAction.INSERT);
        writeRows(makeWriter(outputProps), outputRows);
        return readAndCheckRows(props, outputRows.size());
    }

    protected void deleteRows(List<IndexedRecord> rows, SnowflakeConnectionTableProperties props) throws Exception {
        TSnowflakeOutputProperties deleteProperties = new TSnowflakeOutputProperties("delete"); //$NON-NLS-1$
        deleteProperties.copyValuesFrom(props);
        deleteProperties.outputAction.setValue(SnowflakeOutputProperties.OutputAction.DELETE);
        LOGGER.debug("deleting " + rows.size() + " rows");
        writeRows(makeWriter(deleteProperties), rows);
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
        assertTrue(prop.getPossibleValues().size() == 1);

        moduleProps.tableName.setValue(testTable);
        moduleProps = (SnowflakeTableProperties) PropertiesTestUtils.checkAndAfter(getComponentService(), f, "tableName", moduleProps);
        Schema schema = moduleProps.main.schema.getValue();
        LOGGER.debug(schema.toString());
        for (Schema.Field child : schema.getFields()) {
            LOGGER.debug(child.name());
        }
    }


    protected SnowflakeConnectionTableProperties populateOutput(int count) throws Throwable {
        TSnowflakeOutputProperties props = (TSnowflakeOutputProperties) getComponentService()
                .getComponentProperties(TSnowflakeOutputDefinition.COMPONENT_NAME);
        setupProps(props.getConnectionProperties());
        checkAndSetupTable(props);
        props.outputAction.setStoredValue(SnowflakeOutputProperties.OutputAction.INSERT);
        props.afterOutputAction();

        long time = System.currentTimeMillis();
        System.out.println("Start loading: " + count + " rows");
        Result result = makeAndWriteRows(makeWriter(props), count);
        assertEquals(count, result.getSuccessCount());
        assertEquals(0, result.getRejectCount());
        long elapsed = System.currentTimeMillis() - time;
        System.out.println("time (ms): " + elapsed + " rows/sec: " + ((float) count / (float) (elapsed / 1000)));
        return props;
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
        assertThat(schema.getFields(), hasSize(6));
    }

    @Test
    public void testOutputInsert() throws Throwable {
        SnowflakeConnectionTableProperties props = populateOutput(100);
        readAndCheckRows(props, 100);
    }

    @Test
    public void testOutputDelete() throws Throwable {
        SnowflakeConnectionTableProperties props = populateOutput(100);
        deleteRows(makeRows(100), props);
        assertEquals(0, readRows(props).size());
    }

    @Test
    @Ignore
    public void testOutputLoad() throws Throwable {
        populateOutput(5000000);
    }

}
