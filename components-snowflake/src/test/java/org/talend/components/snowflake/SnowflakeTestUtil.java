package org.talend.components.snowflake;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.talend.components.api.properties.ComponentProperties;

public class SnowflakeTestUtil {

    private final Schema schema1 = SchemaBuilder.builder().record("Schema").fields().name("FirstName").type().nullable()
            .stringType().noDefault().name("LastName").type().nullable().stringType().noDefault().name("Phone").type().nullable()
            .stringType().noDefault().endRecord();

    private final Schema schema2 = SchemaBuilder.builder().record("Schema").fields().name("Id").type().stringType().noDefault()
            .name("FirstName").type().nullable().stringType().noDefault().name("LastName").type().nullable().stringType()
            .noDefault().name("Phone").type().nullable().stringType().noDefault().name("salesforce_id").type().stringType()
            .noDefault().endRecord();

    private final Schema schema3 = SchemaBuilder.builder().record("Schema").fields().name("Id").type().stringType().noDefault()
            .endRecord();

    private final Schema schema4 = SchemaBuilder.builder().record("Schema").fields().name("Id").type().stringType().noDefault()
            .name("FirstName").type().nullable().stringType().noDefault().name("LastName").type().nullable().stringType()
            .noDefault().name("Phone").type().nullable().stringType().noDefault().endRecord();

    private final String module = "Contact";

    private final List<Map<String, String>> testData = new ArrayList<Map<String, String>>();

    {
        Map<String, String> row = new HashMap<String, String>();
        row.put("FirstName", "Wei");
        row.put("LastName", "Wang");
        row.put("Phone", "010-11111111");
        testData.add(row);

        row = new HashMap<String, String>();
        row.put("FirstName", "Jin");
        row.put("LastName", "Zhao");
        row.put("Phone", "010-11111112");
        testData.add(row);

        row = new HashMap<String, String>();
        row.put("FirstName", "Wei");
        row.put("LastName", "Yuan");
        row.put("Phone", null);
        testData.add(row);
    }

    private final String username = System.getProperty("snowflake.user");

    private final String password = System.getProperty("snowflake.password");

    private final String account = System.getProperty("snowflake.account");

    private final String warehouse = System.getProperty("snowflake.warehouse");

    private final String db = System.getProperty("snowflake.db");

    private final String schema = System.getProperty("snowflake.schema");

    public Schema getTestSchema1() {
        return schema1;
    }

    public Schema getTestSchema2() {
        return schema2;
    }

    public Schema getTestSchema3() {
        return schema3;
    }

    public Schema getTestSchema4() {
        return schema4;
    }

    public List<Map<String, String>> getTestData() {
        return testData;
    }

    public String getTestModuleName() {
        return module;
    }

    public void initConnectionProps(SnowflakeConnectionProperties props) {
        props.userPassword.userId.setStoredValue(username);
        props.userPassword.password.setStoredValue(password);
        props.account.setStoredValue(account);
        props.warehouse.setStoredValue(warehouse);
        props.db.setStoredValue(db);
        props.schemaName.setStoredValue(schema);
    }

}
