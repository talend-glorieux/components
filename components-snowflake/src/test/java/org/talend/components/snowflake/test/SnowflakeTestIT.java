package org.talend.components.snowflake.test;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.junit.*;
import org.junit.rules.ErrorCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.api.component.ComponentDefinition;
import org.talend.components.api.component.Connector;
import org.talend.components.api.component.runtime.BoundedReader;
import org.talend.components.api.component.runtime.Result;
import org.talend.components.api.component.runtime.Writer;
import org.talend.components.api.container.DefaultComponentRuntimeContainerImpl;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.api.properties.ComponentReferenceProperties;
import org.talend.components.api.service.ComponentService;
import org.talend.components.api.service.common.ComponentRegistry;
import org.talend.components.api.service.common.ComponentServiceImpl;
import org.talend.components.api.test.AbstractComponentTest;
import org.talend.components.api.test.ComponentTestUtils;
import org.talend.components.api.wizard.ComponentWizard;
import org.talend.components.api.wizard.ComponentWizardDefinition;
import org.talend.components.api.wizard.WizardImageType;
import org.talend.components.api.wizard.WizardNameComparator;
import org.talend.components.common.CommonTestUtils;
import org.talend.components.snowflake.*;
import org.talend.components.snowflake.runtime.SnowflakeSink;
import org.talend.components.snowflake.runtime.SnowflakeSource;
import org.talend.components.snowflake.runtime.SnowflakeSourceOrSink;
import org.talend.components.snowflake.runtime.SnowflakeWriteOperation;
import org.talend.components.snowflake.tsnowflakeconnection.TSnowflakeConnectionDefinition;
import org.talend.components.snowflake.tsnowflakeinput.TSnowflakeInputDefinition;
import org.talend.components.snowflake.tsnowflakeinput.TSnowflakeInputProperties;
import org.talend.components.snowflake.tsnowflakeoutput.TSnowflakeOutputDefinition;
import org.talend.components.snowflake.tsnowflakeoutput.TSnowflakeOutputProperties;
import org.talend.daikon.NamedThing;
import org.talend.daikon.properties.*;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.property.Property;
import org.talend.daikon.properties.service.Repository;
import org.talend.daikon.properties.test.PropertiesTestUtils;

@SuppressWarnings("nls")
public class SnowflakeTestIT extends AbstractComponentTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeTestIT.class);

    protected RuntimeContainer adaptor;

    @Rule
    public ErrorCollector errorCollector = new ErrorCollector();

    private static Connection testConnection;

    private ComponentServiceImpl componentService;

    private static String accountStr = System.getProperty("snowflake.account");
    private static String user = System.getProperty("snowflake.user");
    private static String password = System.getProperty("snowflake.password");
    private static String warehouse = System.getProperty("snowflake.warehouse");
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
        props.userPassword.userId.setStoredValue(user);
        props.userPassword.password.setStoredValue(password);
        props.account.setStoredValue(accountStr);
        props.warehouse.setStoredValue(warehouse);
        props.db.setStoredValue(db);
        props.schemaName.setStoredValue(schema);
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
                            "&db=" + db + "&warehouse=" + warehouse;

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


    protected void resetUser() throws SQLException {
        // Make sure the user is unlocked if locked. Snowflake will lock the user if too many logins
        // So this unlocks it
        testConnection.createStatement().execute(
                "alter user " + user + " set mins_to_unlock=0");
    }


    @Before
    public void setUp() throws SQLException {
        resetUser();
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
        deleteProperties.outputAction.setValue(TSnowflakeOutputProperties.OutputAction.DELETE);
        LOGGER.debug("deleting " + rows.size() + " rows");
        writeRows(makeWriter(deleteProperties), rows);
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
        props.outputAction.setStoredValue(TSnowflakeOutputProperties.OutputAction.INSERT);
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

    protected void setupTable(SnowflakeTableProperties tableProps, String table) throws Throwable {
        Form f = tableProps.getForm(Form.REFERENCE);
        tableProps = (SnowflakeTableProperties) PropertiesTestUtils.checkAndBeforeActivate(getComponentService(), f,
                "tableName", tableProps);
        tableProps.tableName.setValue(table);
        tableProps = (SnowflakeTableProperties) PropertiesTestUtils.checkAndAfter(getComponentService(), f, "tableName", tableProps);
    }


    @Test
    public void testConnectionProps() throws Throwable {
        SnowflakeConnectionProperties props = (SnowflakeConnectionProperties) new TSnowflakeConnectionDefinition()
                .createProperties();
        assertTrue(props.userPassword.userId.isRequired());
        assertTrue(props.userPassword.password.isRequired());
    }

    @Test
    @Ignore
    public void testInputProps() throws Throwable {
        TSnowflakeInputProperties props = (TSnowflakeInputProperties) new TSnowflakeInputDefinition().createProperties();
        assertFalse(props.manualQuery.getValue());
        Property[] returns = new TSnowflakeInputDefinition().getReturnProperties();
        assertEquals(ComponentDefinition.RETURN_TOTAL_RECORD_COUNT, returns[1].getName());

        props.manualQuery.setValue(true);
        fail("finish me");
        //props.after();
    }

    static class RepoProps {

        org.talend.daikon.properties.Properties props;

        String name;

        String repoLocation;

        Schema schema;

        String schemaPropertyName;

        RepoProps(org.talend.daikon.properties.Properties props, String name, String repoLocation, String schemaPropertyName) {
            this.props = props;
            this.name = name;
            this.repoLocation = repoLocation;
            this.schemaPropertyName = schemaPropertyName;
            if (schemaPropertyName != null) {
                this.schema = (Schema) props.getValuedProperty(schemaPropertyName).getValue();
            }
        }

        @Override
        public String toString() {
            return "RepoProps: " + repoLocation + "/" + name + " props: " + props;
        }
    }

    class TestRepository implements Repository {

        private int locationNum;

        public String componentIdToCheck;

        public ComponentProperties properties;

        public List<RepoProps> repoProps;

        TestRepository(List<RepoProps> repoProps) {
            this.repoProps = repoProps;
        }

        @Override
        public String storeProperties(org.talend.daikon.properties.Properties properties, String name, String repositoryLocation, String schemaPropertyName) {
            RepoProps rp = new RepoProps(properties, name, repositoryLocation, schemaPropertyName);
            repoProps.add(rp);
            LOGGER.debug(rp.toString());
            return repositoryLocation + ++locationNum;
        }
    }

    class TestRuntimeContainer extends DefaultComponentRuntimeContainerImpl {
    }

    @Test
    public void testFamily() {
        ComponentDefinition cd = getComponentService().getComponentDefinition("tSnowflakeConnection");
        assertEquals(1, cd.getFamilies().length);
        assertEquals("Cloud/Snowflake", cd.getFamilies()[0]);
    }

    @Test
    public void testWizard() throws Throwable {
        final List<RepoProps> repoProps = new ArrayList<>();

        Repository repo = new TestRepository(repoProps);
        getComponentService().setRepository(repo);

        Set<ComponentWizardDefinition> wizards = getComponentService().getTopLevelComponentWizards();
        int count = 0;
        ComponentWizardDefinition wizardDef = null;
        for (ComponentWizardDefinition wizardDefinition : wizards) {
            if (wizardDefinition instanceof SnowflakeConnectionWizardDefinition) {
                wizardDef = wizardDefinition;
                count++;
            }
        }
        assertEquals(1, count);
        assertEquals("Create Snowflake Connection", wizardDef.getMenuItemName());
        ComponentWizard wiz = getComponentService().getComponentWizard(SnowflakeConnectionWizardDefinition.COMPONENT_WIZARD_NAME,
                "nodeSnowflake");
        assertNotNull(wiz);
        assertEquals("nodeSnowflake", wiz.getRepositoryLocation());
        SnowflakeConnectionWizard swiz = (SnowflakeConnectionWizard) wiz;
        List<Form> forms = wiz.getForms();
        Form connFormWizard = forms.get(0);
        assertEquals("Wizard", connFormWizard.getName());
        assertFalse(connFormWizard.isAllowBack());
        assertFalse(connFormWizard.isAllowForward());
        assertFalse(connFormWizard.isAllowFinish());
        // Main from SnowflakeTableListProperties
        assertEquals("Main", forms.get(1).getName());
        assertEquals("Snowflake Connection Settings", connFormWizard.getTitle());
        assertEquals("Complete these fields in order to connect to your Snowflake account.", connFormWizard.getSubtitle());

        SnowflakeConnectionProperties connProps = (SnowflakeConnectionProperties) connFormWizard.getProperties();

        Form af = connProps.getForm(Form.ADVANCED);
        assertTrue(
                ((PresentationItem) connFormWizard.getWidget("advanced").getContent()).getFormtoShow() + " should be == to " + af,
                ((PresentationItem) connFormWizard.getWidget("advanced").getContent()).getFormtoShow() == af);

        Object image = getComponentService().getWizardPngImage(SnowflakeConnectionWizardDefinition.COMPONENT_WIZARD_NAME,
                WizardImageType.TREE_ICON_16X16);
        assertNotNull(image);
        image = getComponentService().getWizardPngImage(SnowflakeConnectionWizardDefinition.COMPONENT_WIZARD_NAME,
                WizardImageType.WIZARD_BANNER_75X66);
        assertNotNull(image);

        // Check the non-top-level wizard

        // check password i18n
        assertEquals("Name", connProps.getProperty("name").getDisplayName());
        connProps.name.setValue("connName");
        setupProps(connProps);
        Form userPassword = (Form) connFormWizard.getWidget("userPassword").getContent();
        Property passwordSe = (Property) userPassword.getWidget("password").getContent();
        assertEquals("Password", passwordSe.getDisplayName());
        // check name i18n
        NamedThing nameProp = connFormWizard.getWidget("name").getContent(); //$NON-NLS-1$
        assertEquals("Name", nameProp.getDisplayName());
        connProps = (SnowflakeConnectionProperties) PropertiesTestUtils.checkAndValidate(getComponentService(), connFormWizard,
                "testConnection", connProps);
        assertTrue(connFormWizard.isAllowForward());

        Form modForm = forms.get(1);
        SnowflakeTableListProperties mlProps = (SnowflakeTableListProperties) modForm.getProperties();
        assertFalse(modForm.isCallAfterFormBack());
        assertFalse(modForm.isCallAfterFormNext());
        assertTrue(modForm.isCallAfterFormFinish());
        assertTrue(modForm.isCallBeforeFormPresent());
        assertFalse(modForm.isAllowBack());
        assertFalse(modForm.isAllowForward());
        assertFalse(modForm.isAllowFinish());
        mlProps = (SnowflakeTableListProperties) getComponentService().beforeFormPresent(modForm.getName(), mlProps);
        assertTrue(modForm.isAllowBack());
        assertFalse(modForm.isAllowForward());
        assertTrue(modForm.isAllowFinish());
        @SuppressWarnings("unchecked")
        List<NamedThing> all = mlProps.selectedTableNames.getValue();
        assertNull(all);
        List<NamedThing> possibleValues = (List<NamedThing>) mlProps.selectedTableNames.getPossibleValues();
        System.out.println("possibleValues: " + possibleValues);
        assertEquals(1, possibleValues.size());
        List<NamedThing> selected = new ArrayList<>();
        selected.add(possibleValues.get(0));

        mlProps.selectedTableNames.setValue(selected);
        getComponentService().afterFormFinish(modForm.getName(), mlProps);
        LOGGER.debug(repoProps.toString());
        assertEquals(2, repoProps.size());
        int i = 0;
        for (RepoProps rp : repoProps) {
            if (i == 0) {
                assertEquals("connName", rp.name);
                SnowflakeConnectionProperties storedConnProps = (SnowflakeConnectionProperties) rp.props;
                assertEquals(user, storedConnProps.userPassword.userId.getValue());
                assertEquals(password, storedConnProps.userPassword.password.getValue());
            } else {
                SnowflakeTableProperties storedModule = (SnowflakeTableProperties) rp.props;
                assertEquals(selected.get(i - 1).getName(), storedModule.tableName.getValue());
                assertTrue(rp.schema.getFields().size() == 6);
                assertThat(storedModule.main.schema.getStringValue(), is(rp.schema.toString()));
            }
            i++;
        }
    }

    @Test
    public void testModuleWizard() throws Throwable {
        ComponentWizard wiz = getComponentService().getComponentWizard(SnowflakeConnectionWizardDefinition.COMPONENT_WIZARD_NAME,
                "nodeSnowflake");
        List<Form> forms = wiz.getForms();
        Form connFormWizard = forms.get(0);
        SnowflakeConnectionProperties connProps = (SnowflakeConnectionProperties) connFormWizard.getProperties();

        ComponentWizard[] subWizards = getComponentService().getComponentWizardsForProperties(connProps, "location")
                .toArray(new ComponentWizard[3]);
        Arrays.sort(subWizards, new WizardNameComparator());
        assertEquals(3, subWizards.length);
        // Edit connection wizard - we copy the connection properties, as we present the UI, so we use the
        // connection properties object created by the new wizard
        assertFalse(connProps == subWizards[1].getForms().get(0).getProperties());
        // Add module wizard - we refer to the existing connection properties as we don't present the UI
        // for them.
        assertTrue(connProps == ((SnowflakeTableListProperties) subWizards[2].getForms().get(0).getProperties())
                .getConnectionProps());
        assertFalse(subWizards[1].getDefinition().isTopLevel());
        assertEquals("Edit Snowflake Connection", subWizards[1].getDefinition().getMenuItemName());
        assertTrue(subWizards[0].getDefinition().isTopLevel());
        assertEquals("Create Snowflake Connection", subWizards[0].getDefinition().getMenuItemName());
        assertFalse(subWizards[2].getDefinition().isTopLevel());
        assertEquals("Add Snowflake Tables", subWizards[2].getDefinition().getMenuItemName());
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
    public void testLoginFail() throws Throwable {
        SnowflakeConnectionProperties props = (SnowflakeConnectionProperties) setupProps(null);
        props.userPassword.userId.setValue("blah");
        Form f = props.getForm(SnowflakeConnectionProperties.FORM_WIZARD);
        props = (SnowflakeConnectionProperties) PropertiesTestUtils.checkAndValidate(getComponentService(), f, "testConnection",
                props);
        LOGGER.debug(props.getValidationResult().toString());
        assertEquals(ValidationResult.Result.ERROR, props.getValidationResult().getStatus());
    }

    @Test
    public void testSchema() throws Throwable {
        TSnowflakeInputProperties props = (TSnowflakeInputProperties) getComponentService()
                .getComponentProperties(TSnowflakeInputDefinition.COMPONENT_NAME);
        setupProps(props.connection);

        Form f = props.table.getForm(Form.REFERENCE);
        SnowflakeTableProperties moduleProps = (SnowflakeTableProperties) f.getProperties();
        moduleProps = (SnowflakeTableProperties) PropertiesTestUtils.checkAndBeforeActivate(getComponentService(), f,
                "tableName", moduleProps);
        moduleProps.tableName.setValue(testTable);
        moduleProps = (SnowflakeTableProperties) PropertiesTestUtils.checkAndAfter(getComponentService(), f, "tableName", moduleProps);
        Schema schema = moduleProps.main.schema.getValue();
        LOGGER.debug(schema.toString());
        for (Schema.Field child : schema.getFields()) {
            LOGGER.debug(child.name());
        }
        assertEquals("ID", schema.getFields().get(0).name());
        LOGGER.debug("Table \"" + testTable +
                "\" column size:" + schema.getFields().size());
        assertTrue(schema.getFields().size() == 6);
    }

    @Test
    public void testOutputActionType() throws Throwable {
        ComponentDefinition definition = getComponentService().getComponentDefinition(TSnowflakeOutputDefinition.COMPONENT_NAME);
        TSnowflakeOutputProperties outputProps = (TSnowflakeOutputProperties) getComponentService()
                .getComponentProperties(TSnowflakeOutputDefinition.COMPONENT_NAME);
        setupProps(outputProps.connection);

        outputProps.outputAction.setValue(TSnowflakeOutputProperties.OutputAction.DELETE);
        setupTable(outputProps.table, testTable);

        ComponentTestUtils.checkSerialize(outputProps, errorCollector);
        List<IndexedRecord> rows = new ArrayList<>();
        try {
            writeRows(outputProps, rows);
        } catch (Exception ex) {
            if (ex instanceof ClassCastException) {
                LOGGER.debug("Exception: " + ex.getMessage());
                fail("Get error before delete!");
            }
        }
    }

    @Test
    public void testInputConnectionRef() throws Throwable {
        ComponentDefinition definition = getComponentService().getComponentDefinition(TSnowflakeInputDefinition.COMPONENT_NAME);
        TSnowflakeInputProperties props = (TSnowflakeInputProperties) getComponentService()
                .getComponentProperties(TSnowflakeInputDefinition.COMPONENT_NAME);
        setupProps(props.connection);

        SnowflakeSourceOrSink SnowflakeSourceOrSink = new SnowflakeSourceOrSink();
        SnowflakeSourceOrSink.initialize(null, props);
        assertEquals(ValidationResult.Result.OK, SnowflakeSourceOrSink.validate(null).getStatus());

        // Referenced properties simulating Snowflake connect component
        SnowflakeConnectionProperties cProps = (SnowflakeConnectionProperties) getComponentService()
                .getComponentProperties(TSnowflakeConnectionDefinition.COMPONENT_NAME);
        setupProps(cProps);
        cProps.userPassword.password.setValue("xxx");

        String compId = "comp1";
        // Use the connection props of the Snowflake connect component
        props.connection.referencedComponent.referenceType
                .setValue(ComponentReferenceProperties.ReferenceType.COMPONENT_INSTANCE);
        props.connection.referencedComponent.componentInstanceId.setValue(compId);
        props.connection.referencedComponent.componentProperties = cProps;
        PropertiesTestUtils.checkAndAfter(getComponentService(), props.connection.getForm(Form.REFERENCE), "referencedComponent", props.connection);

        SnowflakeSourceOrSink = new SnowflakeSourceOrSink();
        SnowflakeSourceOrSink.initialize(null, props);
        SnowflakeSourceOrSink.validate(null);
        assertEquals(ValidationResult.Result.ERROR, SnowflakeSourceOrSink.validate(null).getStatus());

        // Back to using the connection props of the Snowflake input component
        props.connection.referencedComponent.referenceType.setValue(ComponentReferenceProperties.ReferenceType.THIS_COMPONENT);
        props.connection.referencedComponent.componentInstanceId.setValue(null);
        props.connection.referencedComponent.componentProperties = null;
        // Check that the null referenced component works.
        PropertiesTestUtils.checkAndAfter(getComponentService(), props.connection.getForm(Form.REFERENCE), "referencedComponent", props.connection);

        resetUser();

        SnowflakeSourceOrSink = new SnowflakeSourceOrSink();
        SnowflakeSourceOrSink.initialize(null, props);
        ValidationResult result = SnowflakeSourceOrSink.validate(null);
        System.out.println(result);
        assertEquals(ValidationResult.Result.OK, result.getStatus());
    }

    @Test
    public void testUseExistingConnection() throws Throwable {
        SnowflakeConnectionProperties connProps = (SnowflakeConnectionProperties) getComponentService()
                .getComponentProperties(TSnowflakeConnectionDefinition.COMPONENT_NAME);
        setupProps(connProps);

        final String currentComponentName = TSnowflakeConnectionDefinition.COMPONENT_NAME + "_1";
        RuntimeContainer connContainer = new DefaultComponentRuntimeContainerImpl() {

            @Override
            public String getCurrentComponentId() {
                return currentComponentName;
            }
        };

        SnowflakeSourceOrSink SnowflakeSourceOrSink = new SnowflakeSourceOrSink();
        SnowflakeSourceOrSink.initialize(connContainer, connProps);
        assertEquals(ValidationResult.Result.OK, SnowflakeSourceOrSink.validate(connContainer).getStatus());

        // Input component get connection from the tSnowflakeConnection
        ComponentDefinition inputDefinition = getComponentService()
                .getComponentDefinition(TSnowflakeInputDefinition.COMPONENT_NAME);
        TSnowflakeInputProperties inProps = (TSnowflakeInputProperties) getComponentService()
                .getComponentProperties(TSnowflakeInputDefinition.COMPONENT_NAME);
        inProps.connection.referencedComponent.componentInstanceId.setValue(currentComponentName);

        SnowflakeSourceOrSink SnowflakeInputSourceOrSink = new SnowflakeSourceOrSink();
        SnowflakeInputSourceOrSink.initialize(connContainer, inProps);
        assertEquals(ValidationResult.Result.OK, SnowflakeInputSourceOrSink.validate(connContainer).getStatus());
    }

    @Test
    public void generateJavaNestedCompPropClassNames() {
        Set<ComponentDefinition> allComponents = getComponentService().getAllComponents();
        for (ComponentDefinition cd : allComponents) {
            ComponentProperties props = cd.createProperties();
            String javaCode = PropertiesTestUtils.generatedNestedComponentCompatibilitiesJavaCode(props);
            LOGGER.debug("Nested Props for (" + cd.getClass().getSimpleName() + ".java:1)" + javaCode);
        }
    }

    @Override
    @Test
    public void testAlli18n() {
        ComponentTestUtils.testAlli18n(getComponentService(), errorCollector);
    }

    @Override
    @Test
    public void testAllImages() {
        ComponentTestUtils.testAllImages(getComponentService());
    }

    @Test
    public void checkConnectorsSchema() {
        CommonTestUtils.checkAllSchemaPathAreSchemaTypes(getComponentService(), errorCollector);
    }

    @Test
    public void testSchemaSerialized() throws Throwable {
        // ComponentDefinition definition =
        // getComponentService().getComponentDefinition(TSnowflakeOutputDefinition.COMPONENT_NAME);
        TSnowflakeOutputProperties outputProps = (TSnowflakeOutputProperties) getComponentService()
                .getComponentProperties(TSnowflakeOutputDefinition.COMPONENT_NAME);

        Schema reject = SchemaBuilder.record("Reject").fields().name("A").type().stringType().noDefault().name("B").type()
                .stringType().noDefault().endRecord();

        Schema main = SchemaBuilder.record("Main").fields().name("C").type().stringType().noDefault().name("D").type()
                .stringType().noDefault().endRecord();

        assertEquals(2, outputProps.getAvailableConnectors(null, true).size());
        for (Connector connector : outputProps.getAvailableConnectors(null, true)) {
            if (connector.getName().equals(Connector.MAIN_NAME)) {
                outputProps.setConnectedSchema(connector, main, true);
            } else {
                outputProps.setConnectedSchema(connector, reject, true);
            }
        }

        String serialized = outputProps.toSerialized();

        TSnowflakeOutputProperties afterSerialized = org.talend.daikon.properties.Properties.Helper.fromSerializedPersistent(serialized,
                TSnowflakeOutputProperties.class).object;
        assertEquals(2, afterSerialized.getAvailableConnectors(null, true).size());
        for (Connector connector : afterSerialized.getAvailableConnectors(null, true)) {
            if (connector.getName().equals(Connector.MAIN_NAME)) {
                Schema main2 = afterSerialized.getSchema(connector, true);
                assertEquals(main.toString(), main2.toString());
            } else {
                Schema reject2 = afterSerialized.getSchema(connector, true);
                assertEquals(reject.toString(), reject2.toString());
            }
        }
    }

    @Test
    public void testSchemaSerialized2() throws Throwable {
        ComponentDefinition definition = getComponentService().getComponentDefinition(TSnowflakeOutputDefinition.COMPONENT_NAME);
        TSnowflakeOutputProperties outputProps = (TSnowflakeOutputProperties) getComponentService()
                .getComponentProperties(TSnowflakeOutputDefinition.COMPONENT_NAME);

        Schema reject = SchemaBuilder.record("Reject").fields().name("A").type().stringType().noDefault().name("B").type()
                .stringType().noDefault().endRecord();

        Schema main = SchemaBuilder.record("Main").fields().name("C").type().stringType().noDefault().name("D").type()
                .stringType().noDefault().endRecord();

        outputProps.setValue("table.main.schema", main);
        outputProps.setValue("schemaReject.schema", reject);

        Schema main2 = (Schema) outputProps.getValuedProperty("table.main.schema").getValue();
        Schema reject2 = (Schema) outputProps.getValuedProperty("schemaReject.schema").getValue();
        assertEquals(main.toString(), main2.toString());
        assertEquals(reject.toString(), reject2.toString());

        String serialized = outputProps.toSerialized();

        TSnowflakeOutputProperties afterSerialized = org.talend.daikon.properties.Properties.Helper.fromSerializedPersistent(serialized,
                TSnowflakeOutputProperties.class).object;

        main2 = (Schema) afterSerialized.getValuedProperty("table.main.schema").getValue();
        reject2 = (Schema) afterSerialized.getValuedProperty("schemaReject.schema").getValue();
        assertEquals(main.toString(), main2.toString());
        assertEquals(reject.toString(), reject2.toString());
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
