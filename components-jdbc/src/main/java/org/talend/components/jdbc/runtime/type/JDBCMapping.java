package org.talend.components.jdbc.runtime.type;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.avro.Schema;
import org.talend.daikon.avro.AvroUtils;

public class JDBCMapping {

    public static void setValue(int index, final PreparedStatement statement, final Schema.Field f, final Object value)
            throws SQLException {
        Schema basicSchema = AvroUtils.unwrapIfNullable(f.schema());
        if (value == null) {
            if (AvroUtils.isSameType(basicSchema, AvroUtils._string())) {
                statement.setNull(index, java.sql.Types.VARCHAR);
            } else if (AvroUtils.isSameType(basicSchema, AvroUtils._int())) {
                statement.setNull(index, java.sql.Types.INTEGER);
            } else if (AvroUtils.isSameType(basicSchema, AvroUtils._date())) {
                statement.setNull(index, java.sql.Types.TIMESTAMP);
            } else if (AvroUtils.isSameType(basicSchema, AvroUtils._decimal())) {
                statement.setNull(index, java.sql.Types.DECIMAL);
            } else if (AvroUtils.isSameType(basicSchema, AvroUtils._long())) {
                statement.setNull(index, java.sql.Types.BIGINT);
            } else if (AvroUtils.isSameType(basicSchema, AvroUtils._double())) {
                statement.setNull(index, java.sql.Types.DOUBLE);
            } else if (AvroUtils.isSameType(basicSchema, AvroUtils._float())) {
                statement.setNull(index, java.sql.Types.FLOAT);
            } else if (AvroUtils.isSameType(basicSchema, AvroUtils._boolean())) {
                statement.setNull(index, java.sql.Types.BOOLEAN);
            } else if (AvroUtils.isSameType(basicSchema, AvroUtils._short())) {
                statement.setNull(index, java.sql.Types.SMALLINT);
            } else if (AvroUtils.isSameType(basicSchema, AvroUtils._character())) {
                statement.setNull(index, java.sql.Types.CHAR);
            } else if (AvroUtils.isSameType(basicSchema, AvroUtils._byte())) {
                statement.setNull(index, java.sql.Types.SMALLINT);
            } else {
                statement.setNull(index, java.sql.Types.JAVA_OBJECT);
            }

            return;
        }

        if (AvroUtils.isSameType(basicSchema, AvroUtils._string())) {
            statement.setString(index, (String) value);
        } else if (AvroUtils.isSameType(basicSchema, AvroUtils._int())) {
            statement.setInt(index, (Integer) value);
        } else if (AvroUtils.isSameType(basicSchema, AvroUtils._date())) {
            java.util.Date date = (java.util.Date) value;
            statement.setTimestamp(index, new java.sql.Timestamp((date).getTime()));
        } else if (AvroUtils.isSameType(basicSchema, AvroUtils._decimal())) {
            statement.setBigDecimal(index, (BigDecimal) value);
        } else if (AvroUtils.isSameType(basicSchema, AvroUtils._long())) {
            statement.setLong(index, (Long) value);
        } else if (AvroUtils.isSameType(basicSchema, AvroUtils._double())) {
            statement.setDouble(index, (Double) value);
        } else if (AvroUtils.isSameType(basicSchema, AvroUtils._float())) {
            statement.setFloat(index, (Float) value);
        } else if (AvroUtils.isSameType(basicSchema, AvroUtils._boolean())) {
            statement.setBoolean(index, (Boolean) value);
        } else if (AvroUtils.isSameType(basicSchema, AvroUtils._short())) {
            statement.setShort(index, (Short) value);
        } else if (AvroUtils.isSameType(basicSchema, AvroUtils._character())) {
            statement.setInt(index, (Character) value);
        } else if (AvroUtils.isSameType(basicSchema, AvroUtils._byte())) {
            statement.setByte(index, (Byte) value);
        } else {
            statement.setObject(index, value);
        }
    }
}
