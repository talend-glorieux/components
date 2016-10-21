// ============================================================================
//
// Copyright (C) 2006-2015 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.jdbc.runtime;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.talend.components.api.component.runtime.SourceOrSink;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.api.exception.ComponentException;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.jdbc.ComponentConstants;
import org.talend.components.jdbc.JDBCConnectionInfoProperties;
import org.talend.components.jdbc.ReferAnotherComponent;
import org.talend.components.jdbc.tjdbcconnection.TJDBCConnectionProperties;
import org.talend.components.jdbc.tjdbcoutput.TJDBCOutputProperties;
import org.talend.daikon.NamedThing;
import org.talend.daikon.SimpleNamedThing;
import org.talend.daikon.properties.ValidationResult;

public class JDBCSourceOrSink implements SourceOrSink {

    private static final long serialVersionUID = -1730391293657968628L;

    public JDBCConnectionInfoProperties properties;

    private Connection conn;

    @Override
    public ValidationResult initialize(RuntimeContainer runtime, ComponentProperties properties) {
        this.properties = (JDBCConnectionInfoProperties) properties;
        return ValidationResult.OK;
    }

    private static ValidationResult fillValidationResult(ValidationResult vr, Exception ex) {
        if (vr == null) {
            return null;
        }

        if (ex.getMessage() == null || ex.getMessage().isEmpty()) {
            vr.setMessage(ex.toString());
        } else {
            vr.setMessage(ex.getMessage());
        }
        vr.setStatus(ValidationResult.Result.ERROR);
        return vr;
    }

    @Override
    public ValidationResult validate(RuntimeContainer runtime) {
        ValidationResult vr = new ValidationResult();
        try {
            conn = connect(runtime);
        } catch (Exception ex) {
            fillValidationResult(vr, ex);
        }
        return vr;
    }

    private void closeQuietly(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            // close quietly
        }
    }

    @Override
    public List<NamedThing> getSchemaNames(RuntimeContainer runtime) throws IOException {
        List<NamedThing> result = new ArrayList<>();
        Connection conn = null;
        try {
            conn = connect(runtime);
            DatabaseMetaData metadata = conn.getMetaData();
            ResultSet resultset = metadata.getTables(null, null, null, new String[] { "TABLE" });
            while (resultset.next()) {
                String tablename = resultset.getString("TABLE_NAME");
                result.add(new SimpleNamedThing(tablename, tablename));
            }
        } catch (Exception e) {
            throw new ComponentException(fillValidationResult(new ValidationResult(), e));
        } finally {
            closeQuietly(conn);
        }
        return result;
    }

    @Override
    public Schema getEndpointSchema(RuntimeContainer runtime, String tableName) throws IOException {
        Connection conn = null;
        try {
            conn = connect(runtime);
            DatabaseMetaData metadata = conn.getMetaData();
            ResultSet resultset = metadata.getColumns(null, null, tableName, null);
            return JDBCAvroRegistry.get().inferSchema(resultset);
        } catch (Exception e) {
            throw new ComponentException(e);
        } finally {
            closeQuietly(conn);
        }
    }

    public Schema getSchemaFromQuery(RuntimeContainer runtime, String query) {
        Connection conn = null;
        try {
            conn = connect(runtime);
            Statement statement = conn.createStatement();
            ResultSet resultset = statement.executeQuery(query);
            ResultSetMetaData metadata = resultset.getMetaData();
            return JDBCAvroRegistry.get().inferSchema(metadata);
        } catch (Exception e) {
            throw new ComponentException(e);
        } finally {
            closeQuietly(conn);
        }
    }

    public Connection connect(RuntimeContainer runtime) throws ClassNotFoundException, SQLException {
        // TODO now we use routines.system.TalendDataSource to get the data connection from the ESB runtime, but now we
        // can't
        // refer it by the new framework, so will fix it later

        // TODO routines.system.SharedDBConnectionLog4j, the same with the TODO above

        if (properties instanceof ReferAnotherComponent) {
            ReferAnotherComponent rac = (ReferAnotherComponent) properties;
            String refComponentId = rac.getReferencedComponentId();
            // using another component's connection
            if (refComponentId != null) {
                if (runtime != null) {
                    Object existedConn = runtime.getComponentData(refComponentId, ComponentConstants.CONNECTION_KEY);
                    if (existedConn == null) {
                        throw new RuntimeException("Referenced component: " + refComponentId + " is not connected");
                    }
                    return (Connection) existedConn;
                }

                return JDBCTemplate.createConnection(
                        ((JDBCConnectionInfoProperties) rac.getReferencedComponentProperties()).getJDBCConnectionModule());
            } else {
                Connection conn = JDBCTemplate.createConnection(properties.getJDBCConnectionModule());

                if (properties instanceof TJDBCOutputProperties) {
                    TJDBCOutputProperties props = (TJDBCOutputProperties) properties;
                    Integer commitEvery = props.commitEvery.getValue();
                    if (commitEvery != null && commitEvery > 0) {
                        conn.setAutoCommit(false);
                    }
                }

                return conn;
            }
        } else {// connection component
            Connection conn = JDBCTemplate.createConnection(properties.getJDBCConnectionModule());

            if (properties instanceof TJDBCConnectionProperties) {
                TJDBCConnectionProperties jdbcConnectionProperties = (TJDBCConnectionProperties) properties;

                boolean useAutoCommit = jdbcConnectionProperties.useAutoCommit.getValue();
                if (useAutoCommit) {
                    boolean autoCommit = jdbcConnectionProperties.autocommit.getValue();
                    conn.setAutoCommit(autoCommit);
                }

                if (runtime != null) {
                    runtime.setComponentData(runtime.getCurrentComponentId(), ComponentConstants.CONNECTION_KEY, conn);
                }
            }

            return conn;
        }
    }

    public Connection getConnection(RuntimeContainer runtime) throws ClassNotFoundException, SQLException {
        if (conn == null) {
            conn = connect(runtime);
        }
        return conn;
    }

}
