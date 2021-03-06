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
package org.talend.components.jdbc;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.talend.components.api.component.runtime.WriteOperation;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.jdbc.common.DBTestUtils;
import org.talend.components.jdbc.module.JDBCConnectionModule;
import org.talend.components.jdbc.runtime.JDBCCommitSourceOrSink;
import org.talend.components.jdbc.runtime.JDBCSink;
import org.talend.components.jdbc.runtime.JDBCSourceOrSink;
import org.talend.components.jdbc.runtime.JDBCTemplate;
import org.talend.components.jdbc.runtime.writer.JDBCOutputInsertWriter;
import org.talend.components.jdbc.tjdbccommit.TJDBCCommitDefinition;
import org.talend.components.jdbc.tjdbccommit.TJDBCCommitProperties;
import org.talend.components.jdbc.tjdbcconnection.TJDBCConnectionDefinition;
import org.talend.components.jdbc.tjdbcconnection.TJDBCConnectionProperties;
import org.talend.components.jdbc.tjdbcoutput.TJDBCOutputDefinition;
import org.talend.components.jdbc.tjdbcoutput.TJDBCOutputProperties;
import org.talend.components.jdbc.tjdbcoutput.TJDBCOutputProperties.DataAction;
import org.talend.daikon.properties.ValidationResult;

public class JDBCCommitTestIT {

    public static String driverClass;

    private static String jdbcUrl;

    private static String userId;

    private static String password;

    private static String tablename;

    public static JDBCConnectionModule connectionInfo;

    private final String refComponentId = "tJDBCConnection1";

    RuntimeContainer container = new RuntimeContainer() {

        private Map<String, Object> map = new HashMap<>();

        @Override
        public Object getComponentData(String componentId, String key) {
            return map.get(componentId + "_" + key);
        }

        @Override
        public void setComponentData(String componentId, String key, Object data) {
            map.put(componentId + "_" + key, data);
        }

        @Override
        public String getCurrentComponentId() {
            return refComponentId;
        }

        @Override
        public Object getGlobalData(String key) {
            return null;
        }

    };

    @BeforeClass
    public static void init() throws Exception {
        java.util.Properties props = new java.util.Properties();
        try (InputStream is = JDBCCommitTestIT.class.getClassLoader().getResourceAsStream("connection.properties")) {
            props = new java.util.Properties();
            props.load(is);
        }

        driverClass = props.getProperty("driverClass");

        jdbcUrl = props.getProperty("jdbcUrl");

        userId = props.getProperty("userId");

        password = props.getProperty("password");

        tablename = props.getProperty("tablename");

        connectionInfo = new JDBCConnectionModule("connection");

        connectionInfo.driverClass.setValue(driverClass);
        connectionInfo.jdbcUrl.setValue(jdbcUrl);
        connectionInfo.userPassword.userId.setValue(userId);
        connectionInfo.userPassword.password.setValue(password);
    }

    @AfterClass
    public static void clean() throws ClassNotFoundException, SQLException {
        DBTestUtils.releaseResource(connectionInfo);
    }

    @Before
    public void before() throws ClassNotFoundException, SQLException, Exception {
        DBTestUtils.prepareTableAndData(connectionInfo);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testCommit() throws IOException, ClassNotFoundException, SQLException {
        // connection part
        TJDBCConnectionDefinition connectionDefinition = new TJDBCConnectionDefinition();
        TJDBCConnectionProperties connectionProperties = createCommonJDBCConnectionProperties(connectionDefinition);

        JDBCSourceOrSink sourceOrSink = new JDBCSourceOrSink();
        sourceOrSink.initialize(null, connectionProperties);

        ValidationResult result = sourceOrSink.validate(container);
        assertTrue(result.getStatus() == ValidationResult.Result.OK);

        // output part
        TJDBCOutputDefinition outputDefinition = new TJDBCOutputDefinition();
        TJDBCOutputProperties outputProperties = (TJDBCOutputProperties) outputDefinition.createRuntimeProperties();

        outputProperties.main.schema.setValue(DBTestUtils.createTestSchema());
        outputProperties.updateOutputSchemas();

        outputProperties.tableSelection.tablename.setValue(tablename);

        outputProperties.dataAction.setValue(DataAction.INSERT);

        outputProperties.referencedComponent.componentInstanceId.setValue(refComponentId);

        JDBCSink sink = new JDBCSink();
        sink.initialize(container, outputProperties);

        WriteOperation writerOperation = sink.createWriteOperation();
        writerOperation.initialize(container);
        JDBCOutputInsertWriter writer = (JDBCOutputInsertWriter) writerOperation.createWriter(container);

        try {
            writer.open("wid");

            IndexedRecord r1 = new GenericData.Record(outputProperties.main.schema.getValue());
            r1.put(0, 4);
            r1.put(1, "xiaoming");
            writer.write(r1);

            DBTestUtils.assertSuccessRecord(writer, r1);

            IndexedRecord r2 = new GenericData.Record(outputProperties.main.schema.getValue());
            r2.put(0, 5);
            r2.put(1, "xiaobai");
            writer.write(r2);

            DBTestUtils.assertSuccessRecord(writer, r2);

            writer.close();
        } finally {
            writer.close();
        }

        // commit part
        TJDBCCommitDefinition commitDefinition = new TJDBCCommitDefinition();
        TJDBCCommitProperties commitProperties = (TJDBCCommitProperties) commitDefinition.createRuntimeProperties();

        commitProperties.referencedComponent.componentInstanceId.setValue(refComponentId);
        commitProperties.closeConnection.setValue(false);

        JDBCCommitSourceOrSink commitSourceOrSink = new JDBCCommitSourceOrSink();
        commitSourceOrSink.initialize(container, commitProperties);
        commitSourceOrSink.validate(container);

        // create another session and check if the data is inserted
        Connection conn = JDBCTemplate.createConnection(connectionInfo);
        Statement statement = conn.createStatement();
        ResultSet resultset = statement.executeQuery("select count(*) from TEST");
        int count = -1;
        if (resultset.next()) {
            count = resultset.getInt(1);
        }
        statement.close();
        conn.close();

        Assert.assertEquals(5, count);

        java.sql.Connection refConnection = (java.sql.Connection) container.getComponentData(refComponentId,
                ComponentConstants.CONNECTION_KEY);

        assertTrue(refConnection != null);
        Assert.assertTrue(!refConnection.isClosed());
        refConnection.close();

    }

    @Test
    public void testClose() throws IOException, ClassNotFoundException, SQLException {
        // connection part
        TJDBCConnectionDefinition connectionDefinition = new TJDBCConnectionDefinition();
        TJDBCConnectionProperties connectionProperties = createCommonJDBCConnectionProperties(connectionDefinition);

        JDBCSourceOrSink sourceOrSink = new JDBCSourceOrSink();
        sourceOrSink.initialize(null, connectionProperties);

        ValidationResult result = sourceOrSink.validate(container);
        assertTrue(result.getStatus() == ValidationResult.Result.OK);

        // commit part
        TJDBCCommitDefinition commitDefinition = new TJDBCCommitDefinition();
        TJDBCCommitProperties commitProperties = (TJDBCCommitProperties) commitDefinition.createRuntimeProperties();

        commitProperties.referencedComponent.componentInstanceId.setValue(refComponentId);
        commitProperties.closeConnection.setValue(true);

        JDBCCommitSourceOrSink commitSourceOrSink = new JDBCCommitSourceOrSink();
        commitSourceOrSink.initialize(container, commitProperties);
        commitSourceOrSink.validate(container);

        java.sql.Connection refConnection = (java.sql.Connection) container.getComponentData(refComponentId,
                ComponentConstants.CONNECTION_KEY);

        assertTrue(refConnection != null);
        Assert.assertTrue(refConnection.isClosed());
    }

    private TJDBCConnectionProperties createCommonJDBCConnectionProperties(TJDBCConnectionDefinition connectionDefinition) {
        TJDBCConnectionProperties connectionProperties = (TJDBCConnectionProperties) connectionDefinition
                .createRuntimeProperties();

        // TODO now framework doesn't support to load the JDBC jar by the setting
        // properties.connection.driverJar.setValue("port", props.getProperty("port"));
        connectionProperties.connection.driverClass.setValue(driverClass);
        connectionProperties.connection.jdbcUrl.setValue(jdbcUrl);
        connectionProperties.connection.userPassword.userId.setValue(userId);
        connectionProperties.connection.userPassword.password.setValue(password);
        return connectionProperties;
    }

}
