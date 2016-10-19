/**
 *
 */
package org.talend.components.snowflake.runtime;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.api.component.runtime.SourceOrSink;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.exception.ComponentException;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.snowflake.SnowflakeConnectionProperties;
import org.talend.components.snowflake.SnowflakeConnectionTableProperties;
import org.talend.components.snowflake.SnowflakeProvideConnectionProperties;
import org.talend.components.snowflake.SnowflakeTableProperties;
import org.talend.components.snowflake.connection.SnowflakeNativeConnection;
import org.talend.daikon.NamedThing;
import org.talend.daikon.SimpleNamedThing;
import org.talend.daikon.avro.SchemaConstants;
import org.talend.daikon.properties.ValidationResult;
import org.talend.daikon.properties.ValidationResult.Result;

public class SnowflakeSourceOrSink implements SourceOrSink {

    private static final long serialVersionUID = 1L;

    private transient static final Logger LOG = LoggerFactory.getLogger(SnowflakeSourceOrSink.class);

    protected SnowflakeProvideConnectionProperties properties;

    protected static final String KEY_CONNECTION = "Connection";

    @Override
    public ValidationResult initialize(RuntimeContainer container, ComponentProperties properties) {
        this.properties = (SnowflakeProvideConnectionProperties) properties;
        return ValidationResult.OK;
    }

    @Override
    public ValidationResult validate(RuntimeContainer container) {
        ValidationResult vr = new ValidationResult();
        try {
            if (null != connect(container)) {
                vr.setStatus(Result.OK);
                vr.setMessage("Connection Successful");
            } else {
                vr.setStatus(Result.ERROR);
                vr.setMessage("Could not establish connection to the Snowflake DB");
            }
        } catch (Exception ex) {
            return exceptionToValidationResult(ex);
        }
        return vr;
    }

    protected static ValidationResult exceptionToValidationResult(Exception ex) {
        ValidationResult vr = new ValidationResult();
        vr.setMessage(ex.getMessage());
        vr.setStatus(ValidationResult.Result.ERROR);
        return vr;
    }

    public static ValidationResult validateConnection(SnowflakeProvideConnectionProperties properties) {
        SnowflakeSourceOrSink sss = new SnowflakeSourceOrSink();
        sss.initialize(null, (ComponentProperties) properties);
        return sss.validate(null);
    }

    public SnowflakeConnectionProperties getConnectionProperties() {
        return this.properties.getConnectionProperties();
    }

    protected SnowflakeNativeConnection connect(RuntimeContainer container) throws IOException {

        SnowflakeNativeConnection nativeConn = null;

        SnowflakeConnectionProperties connProps = properties.getConnectionProperties();
        String refComponentId = connProps.getReferencedComponentId();
        Object sharedConn = null;
        // Using another component's connection
        if (refComponentId != null) {
            // In a runtime container
            if (container != null) {
                sharedConn = container.getComponentData(refComponentId, KEY_CONNECTION);
                if (sharedConn != null) {
                    if (sharedConn instanceof SnowflakeNativeConnection) {
                        nativeConn = (SnowflakeNativeConnection) sharedConn;
                    }
                    return nativeConn;
                }
                throw new IOException("Referenced component: " + refComponentId + " not connected");
            }
            // Design time
            connProps = connProps.getReferencedConnectionProperties();
        }

        // Establish a new connection
        nativeConn = new SnowflakeNativeConnection();
        Connection conn = null;
        String queryString = "";

        String user = connProps.userPassword.userId.getStringValue();
        String password = connProps.userPassword.password.getStringValue();
        String account = connProps.account.getStringValue();

        String warehouse = connProps.warehouse.getStringValue();
        String db = connProps.db.getStringValue();
        String schema = connProps.schemaName.getStringValue();

        String role = connProps.role.getStringValue();
        String tracing = connProps.tracing.getStringValue();

        if (null != warehouse && !"".equals(warehouse)) {
            queryString = queryString + "warehouse=" + warehouse;
        }
        if (null != db && !"".equals(db)) {
            queryString = queryString + "&db=" + db;
        }
        if (null != schema && !"".equals(schema)) {
            queryString = queryString + "&schema=" + schema;
        }

        if (null != role && !"".equals(role)) {
            queryString = queryString + "&role=" + role;
        }
        if (null != tracing && !"".equals(tracing)) {
            queryString = queryString + "&tracing=" + tracing;
        }
        String connectionURL = "jdbc:snowflake://" + account + ".snowflakecomputing.com" + "/?" + queryString;
        String JDBC_DRIVER = "com.snowflake.client.jdbc.SnowflakeDriver";

        try {
            Driver driver = (Driver) Class.forName(JDBC_DRIVER).newInstance();
            DriverManager.registerDriver(new DriverWrapper(driver));

            conn = DriverManager.getConnection(connectionURL, user, password);
        } catch (Exception e) {
            throw new ComponentException(exceptionToValidationResult(e));
        }

        nativeConn.setConnection(conn);

        if (container != null) {
            container.setComponentData(container.getCurrentComponentId(), KEY_CONNECTION, nativeConn);
        }

        return nativeConn;
    }

    public static List<NamedThing> getSchemaNames(RuntimeContainer container, SnowflakeConnectionProperties properties)
            throws IOException {
        SnowflakeSourceOrSink ss = new SnowflakeSourceOrSink();
        ss.initialize(null, properties);
        try {
            ss.connect(container);
            return ss.getSchemaNames(container);
        } catch (Exception ex) {
            throw new ComponentException(exceptionToValidationResult(ex));
        }
    }

    @Override
    public List<NamedThing> getSchemaNames(RuntimeContainer container) throws IOException {
        return getSchemaNames(connect(container).getConnection());
    }

    /**
     * Fetches the list of tables names in a database connection
     *
     * @param connection
     * @return
     * @throws IOException
     */
    protected List<NamedThing> getSchemaNames(Connection connection) throws IOException {

        SnowflakeConnectionProperties connProps = properties.getConnectionProperties();
        String catalog = null  /*connProps.db.getStringValue()*/;
        String dbSchema = null  /*connProps.schema.getStringValue()*/;

        // Returns the list with a table names (for the wh, db and schema)
        List<NamedThing> returnList = new ArrayList<>();
        try {
            DatabaseMetaData metaData = connection.getMetaData();

            // Fetch all tables in the db and schema provided
            String[] types = { "TABLE" };

            ResultSet resultIter = metaData.getTables(catalog, dbSchema, null, types);

            // ResultSet resultIter = metaData.getCatalogs();
            String tableName = null;
            while (resultIter.next()) {
                tableName = resultIter.getString("TABLE_NAME");
                returnList.add(new SimpleNamedThing(tableName, tableName));
            }
        } catch (SQLException se) {
            // TODO: Handle this
        }
        return returnList;
    }

    public static Schema getSchema(RuntimeContainer container, SnowflakeProvideConnectionProperties properties, String table)
            throws IOException {
        SnowflakeSourceOrSink ss = new SnowflakeSourceOrSink();
        ss.initialize(null, (ComponentProperties) properties);
        Connection connection = null;
        try {
            connection = ss.connect(container).getConnection();
        } catch (Exception ex) {
            throw new ComponentException(exceptionToValidationResult(ex));
        }
        return ss.getSchema(connection, table);
    }

    @Override
    public Schema getEndpointSchema(RuntimeContainer container, String schemaName) throws IOException {
        return getSchema(connect(container).getConnection(), schemaName);
    }

    protected Schema getSchema(Connection connection, String tableName) throws IOException {
        String catalog = null/*properties.getConnectionProperties().db.getStringValue()*/;
        String dbSchema = null/*properties.getConnectionProperties().schema.getStringValue()*/;

        Schema tableSchema = null;

        try {
            DatabaseMetaData metaData = connection.getMetaData();

            ResultSet resultIter = metaData.getColumns(catalog, dbSchema, tableName, null);
            if (resultIter.next()) {
                tableSchema = SnowflakeAvroRegistry.get().inferSchema(resultIter);

                // Update the schema with Primary Key details
                if (null != tableSchema) {
                    ResultSet keysIter = metaData.getPrimaryKeys(catalog, dbSchema, tableName);

                    List<String> pkColumns = new ArrayList<>(); // List of Primary Key columns for this table
                    while (keysIter.next()) {
                        pkColumns.add(keysIter.getString("COLUMN_NAME"));
                    }

                    for (Field f : tableSchema.getFields()) {
                        if (pkColumns.contains(f.schema().getName())) {
                            f.schema().addProp(SchemaConstants.TALEND_COLUMN_IS_KEY, true);
                        }
                    }
                }

            }

        } catch (SQLException se) {
            // TODO: Handle this.. logger
        }

        return tableSchema;

    }

    // -------------------------------------------------------------------------------------

    /**
     * Inner class.<br>
     * Custom driver wrapper class
     */

    public class DriverWrapper implements Driver {

        private Driver driver;

        public DriverWrapper(Driver d) {
            this.driver = d;
        }

        public boolean acceptsURL(String u) throws SQLException {
            return this.driver.acceptsURL(u);
        }

        public Connection connect(String u, Properties p) throws SQLException {
            return this.driver.connect(u, p);
        }

        public int getMajorVersion() {
            return this.driver.getMajorVersion();
        }

        public int getMinorVersion() {
            return this.driver.getMinorVersion();
        }

        public DriverPropertyInfo[] getPropertyInfo(String u, Properties p) throws SQLException {
            return this.driver.getPropertyInfo(u, p);
        }

        public boolean jdbcCompliant() {
            return this.driver.jdbcCompliant();
        }

        @Override
        public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return this.driver.getParentLogger();
        }
    }

}
