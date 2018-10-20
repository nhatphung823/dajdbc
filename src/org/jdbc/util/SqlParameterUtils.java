package org.jdbc.util;

import com.jolbox.bonecp.ConnectionHandle;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleTypes;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import org.apache.log4j.Logger;
import org.jdbc.DataType;
import org.jdbc.array.PassThroughArray;
import org.jdbc.lob.PassThroughBlob;
import org.jdbc.lob.PassThroughClob;
import org.jdbc.paramater.SqlParameter;
import org.jdbc.paramater.SqlParameterValue;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Nguyen Van Nhat
 * Date: 11/16/12
 * Time: 9:47 AM
 * To change this template use File | Settings | File Templates.
 */
public class SqlParameterUtils {
  static Logger logger = Logger.getLogger(SqlParameterUtils.class);
  static Map<String, List<SqlParameter>> cachedSqlParameters = new Hashtable<>();

  public static List<SqlParameter> getProcedureParameters(Connection conn, String catalog,
                                                          String schema, String procedure) throws SQLException {
    return getParameters(conn, catalog, schema, procedure, false);
  }

  public static List<SqlParameter> getFunctionParameters(Connection conn, String catalog,
                                                         String schema, String function) throws SQLException {
    return getParameters(conn, catalog, schema, function, true);
  }

  public static void addParameters(Connection conn, CallableStatement cs, List<SqlParameterValue> list) throws SQLException {
    for (SqlParameterValue spv : list) {
      addParameter(conn, cs, spv);
      logger.trace(spv.getSqlParameter().getColumnName() + " >> " + spv.getValue());
    }
  }

  static final String COLUMN_TYPE = "COLUMN_TYPE";
  static final String COLUMN_NAME = "COLUMN_NAME";
  static final String TYPE_NAME = "TYPE_NAME";
  static final String DATA_TYPE = "DATA_TYPE";

  private static List<SqlParameter> getParameters(Connection conn, String catalog,
                                                  String schema, String procedure, boolean function) throws SQLException {
    DatabaseMetaData dmd;
    ResultSet rs = null, rrss = null;
    try {
      dmd = conn.getMetaData();
      if (schema == null || schema.trim().equals("")) {
        if (null != catalog && !catalog.trim().equals("")) {
          schema = dmd.getUserName().toUpperCase();
        }
      }

      String key = catalog + "." + schema + "." + procedure;
      List<SqlParameter> list = cachedSqlParameters.get(key);
      if (cachedSqlParameters.containsKey(key) && list != null) {
        logger.trace("SqlParameter of " + key + "is cached");
        return list;
      } else {
        rrss = dmd.getProcedures(catalog, schema, procedure);
        if (!rrss.isBeforeFirst())
          throw new SQLException("(" + schema + ", " + catalog + ", " + procedure + ") >> not exists");

        rs = dmd.getProcedureColumns(catalog, schema, procedure, null);
        list = new ArrayList<>();
        int index = 1;
        while (rs.next()) {
          int colType = rs.getInt(COLUMN_TYPE);
          String colName = rs.getString(COLUMN_NAME);
          String tyName = rs.getString(TYPE_NAME);
          int daType = rs.getInt(DATA_TYPE);
          if (colType == 5 && !function) {
            continue;
          }
/*
        else if ("PL/SQL TABLE".equals(tyName)
            && "NAMES".equals(colName) && colType == 4) {
          continue;
        } else if ("PL/SQL TABLE".equals(tyName)
            && "VALS".equals(colName) && colType == 4) {
          continue;
        } else if ("NUMBER".equals(tyName)
            && "NUM_VALS".equals(colName) && colType == 4) {
          continue;
        }
*/
          SqlParameter para = new SqlParameter();
          para.setIndex(index);
          para.setColumnName(colName);
          para.setColumnType(colType);
          para.setDataType(daType);
          para.setTypeName(tyName);
          logger.trace(para);
          list.add(para);
          index++;
        }
        cachedSqlParameters.put(key, list);
      }

      return list;
    } finally {
      if (rrss != null)
        rrss.close();
      if (rs != null)
        rs.close();
    }
  }

  //  static final String TABLE_RETURN_VALUE = "TABLE_RETURN_VALUE";
//  static final String table_return_value = "table_return_value";
  static final String RS = "rs";
  static final String RETURN_VALUE = "return_value";

  private static void addParameter(Connection conn, CallableStatement cs, SqlParameterValue spv) throws SQLException {
    SqlParameter pa = spv.getSqlParameter();
    if (pa.getColumnType() == 1) { //in
      setInPrams(conn, cs, pa, spv.getValue());
    } else if (pa.getColumnType() == 4) {  //out
      setOutParams(cs, pa);
    } else if (pa.getColumnType() == 2) { //in-out
      if (spv.getValue() == null) {
        setOutParams(cs, pa);
      } else {
        setInPrams(conn, cs, pa, spv.getValue());
        setOutParams(cs, pa);
      }
    } else if (pa.getColumnType() == 5) { //return
      if (spv.getSqlParameter().getTypeName().equals(DataType.REF_CURSOR)) {
        pa.setColumnName(RS);
      } else {
        pa.setColumnName(RETURN_VALUE);
      }
      setOutParams(cs, pa);
    }
/*
    else if (pa.getColumnType() == 3) {
      if (spv.getSqlParameter().getTypeName().toUpperCase().equals(TABLE_RETURN_VALUE)) {
        pa.setColumnName(table_return_value);
      }
      setOutParams(cs, pa);
    }
*/
    else {
      throw new SQLException("dajdbc not support column-type : " + pa.getColumnType());
    }
  }

  static final String NUMBER_FORMAT = "[\\+-]?\\d+([.]\\d+)?";

  private static void setInPrams(Connection conn, CallableStatement cs, SqlParameter pa, Object val) throws SQLException {
    if (pa.getDataType() == OracleTypes.VARCHAR
        || pa.getDataType() == OracleTypes.CHAR
        || pa.getDataType() == OracleTypes.LONGVARCHAR
        || pa.getDataType() == OracleTypes.NVARCHAR
        || pa.getDataType() == OracleTypes.NCHAR
        || pa.getDataType() == OracleTypes.LONGNVARCHAR) {
      cs.setString(pa.getIndex(), val == null ? "" : String.valueOf(val));
    } else if (pa.getDataType() == OracleTypes.OTHER
        && (pa.getTypeName().equals(DataType.NCHAR) ||
        pa.getTypeName().equals(DataType.NVARCHAR2))) {
      cs.setNString(pa.getIndex(), val == null ? "" : String.valueOf(val));
    } else if (pa.getDataType() == OracleTypes.BINARY
        || pa.getDataType() == OracleTypes.VARBINARY
        || pa.getDataType() == OracleTypes.LONGVARBINARY) {
      cs.setBytes(pa.getIndex(), (byte[]) val);
    } else if (pa.getDataType() == OracleTypes.DECIMAL) {
      if (val instanceof BigDecimal) {
        cs.setBigDecimal(pa.getIndex(), (BigDecimal) val);
      } else if (val instanceof Long) {
        cs.setBigDecimal(pa.getIndex(), BigDecimal.valueOf((Long) val));
      } else if (val instanceof Double) {
        cs.setBigDecimal(pa.getIndex(), BigDecimal.valueOf((Double) val));
      } else if (val instanceof Integer) {
        cs.setBigDecimal(pa.getIndex(), BigDecimal.valueOf((Integer) val));
      } else if (val instanceof Float) {
        cs.setBigDecimal(pa.getIndex(), BigDecimal.valueOf((Float) val));
      } else if (val instanceof Byte) {
        cs.setBigDecimal(pa.getIndex(), BigDecimal.valueOf((Byte) val));
      } else if (val instanceof Short) {
        cs.setBigDecimal(pa.getIndex(), BigDecimal.valueOf((Short) val));
      } else if (val == null) {
        cs.setBigDecimal(pa.getIndex(), null);
      } else if (val instanceof String) {
        if (((String) val).matches(NUMBER_FORMAT)) {
          cs.setBigDecimal(pa.getIndex(), new BigDecimal((String) val));
        } else {
          throw new SQLException("BigDecimal not a number : " + val);
        }
      } else {
        throw new SQLException("BigDecimal invalid value-type : " + val.getClass());
      }
    } else if (pa.getDataType() == OracleTypes.TIMESTAMP
        || pa.getTypeName().equals(DataType.DATE)) {
      java.util.Date date = (java.util.Date) val;
      cs.setTimestamp(pa.getIndex(), date != null ? new Timestamp(date.getTime()) : null);
    } else if (pa.getDataType() == OracleTypes.TIMESTAMP
        || pa.getTypeName().equals(DataType.TIMESTAMP)) {
      java.util.Date date = (java.util.Date) val;
      cs.setTimestamp(pa.getIndex(), date != null ? new Timestamp(date.getTime()) : null);
    } else if (pa.getDataType() == OracleTypes.TIME
        || pa.getTypeName().equals(DataType.TIME)) {
      java.util.Date date = (java.util.Date) val;
      cs.setTimestamp(pa.getIndex(), date != null ? new Timestamp(date.getTime()) : null);
    } else if (pa.getDataType() == OracleTypes.DATE) {
      java.util.Date date = (java.util.Date) val;
      cs.setTimestamp(pa.getIndex(), date != null ? new Timestamp(date.getTime()) : null);
    } else if (pa.getDataType() == OracleTypes.INTEGER) {
      cs.setInt(pa.getIndex(), (Integer) val);
    } else if (pa.getDataType() == OracleTypes.BIGINT) {
      cs.setLong(pa.getIndex(), (Long) val);
    } else if (pa.getDataType() == OracleTypes.FLOAT) {
      cs.setFloat(pa.getIndex(), (Float) val);
    } else if (pa.getDataType() == OracleTypes.DOUBLE) {
      cs.setDouble(pa.getIndex(), (Double) val);
    } else if (pa.getDataType() == OracleTypes.TINYINT ||
        pa.getDataType() == OracleTypes.SMALLINT) {
      cs.setShort(pa.getIndex(), (Short) val);
    } else if (pa.getDataType() == OracleTypes.REAL) {
      cs.setFloat(pa.getIndex(), (Float) val);
    } else if (pa.getDataType() == OracleTypes.BIT) {
      cs.setBoolean(pa.getIndex(), (Boolean) val);
    } else if (pa.getDataType() == OracleTypes.BOOLEAN) {
      cs.setBoolean(pa.getIndex(), (Boolean) val);
    } else if (pa.getDataType() == OracleTypes.OTHER
        && pa.getTypeName().equals(DataType.ROW_ID)) {
      cs.setRowId(pa.getIndex(), (RowId) val);
    } else if (pa.getDataType() == OracleTypes.OTHER
        && pa.getTypeName().equals(DataType.CLOB)) {
      PassThroughClob clob = (PassThroughClob) val;
      cs.setClob(pa.getIndex(), clob != null ? clob.getCharacterStream() : null, clob != null ? clob.length() : 0);
    } else if (pa.getDataType() == OracleTypes.OTHER
        && pa.getTypeName().equals(DataType.BLOB)) {
      PassThroughBlob blob = (PassThroughBlob) val;
      cs.setBlob(pa.getIndex(), blob != null ? blob.getBinaryStream() : null, blob != null ? blob.length() : 0);
    } else if (pa.getDataType() == OracleTypes.OTHER
        && pa.getTypeName().equals(DataType.TABLE)) {
      PassThroughArray passArr = (PassThroughArray) val;
      if (ConnectionHandle.equals(conn.getClass().getName())) {
        ARRAY array = new ARRAY(
            new ArrayDescriptor(passArr.getNameType(), ((ConnectionHandle) conn).getInternalConnection()),
            ((ConnectionHandle) conn).getInternalConnection(), passArr.getArray());
        cs.setArray(pa.getIndex(), array);
      } else if (T4CConnection.equals(conn.getClass().getName())) {
        ARRAY array = new ARRAY(new ArrayDescriptor(passArr.getNameType(), conn), conn, passArr.getArray());
        cs.setArray(pa.getIndex(), array);
      } else {
        ARRAY array = new ARRAY(
            new ArrayDescriptor(passArr.getNameType(), conn.getMetaData().getConnection()),
            conn.getMetaData().getConnection(), passArr.getArray());
        cs.setArray(pa.getIndex(), array);
      }
    } else {
      throw new SQLException("dajdbc not support data-type : " + pa.getTypeName() + "-" + pa.getDataType());
    }
  }

  static final String ConnectionHandle = "com.jolbox.bonecp.ConnectionHandle";
  static final String T4CConnection = "oracle.jdbc.driver.T4CConnection";

  private static void setOutParams(CallableStatement cs, SqlParameter pa) throws SQLException {
    if (pa.getDataType() == OracleTypes.VARCHAR
        || pa.getDataType() == OracleTypes.CHAR
        || pa.getDataType() == OracleTypes.LONGVARCHAR) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.VARCHAR);
    } else if (pa.getDataType() == OracleTypes.NVARCHAR
        || pa.getDataType() == OracleTypes.NCHAR
        || pa.getDataType() == OracleTypes.LONGNVARCHAR) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.NVARCHAR);
    } else if (pa.getDataType() == OracleTypes.OTHER
        && (pa.getTypeName().equals(DataType.NCHAR) ||
        pa.getTypeName().equals(DataType.NVARCHAR2))) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.VARCHAR);
    } else if (pa.getDataType() == OracleTypes.DECIMAL) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.DECIMAL);
    } else if (pa.getDataType() == OracleTypes.TIMESTAMP
        || pa.getTypeName().equals(DataType.DATE)) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.TIMESTAMP);
    } else if (pa.getDataType() == OracleTypes.TIMESTAMP
        || pa.getTypeName().equals(DataType.TIMESTAMP)) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.TIMESTAMP);
    } else if (pa.getDataType() == OracleTypes.TIME ||
        pa.getTypeName().equals(DataType.TIME)) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.TIMESTAMP);
    } else if (pa.getDataType() == OracleTypes.DATE) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.TIMESTAMP);
    } else if (pa.getDataType() == OracleTypes.OTHER
        && pa.getTypeName().equals(DataType.REF_CURSOR)) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.CURSOR);
    } else if (pa.getDataType() == OracleTypes.INTEGER) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.INTEGER);
    } else if (pa.getDataType() == OracleTypes.BIGINT) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.BIGINT);
    } else if (pa.getDataType() == OracleTypes.FLOAT) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.FLOAT);
    } else if (pa.getDataType() == OracleTypes.DOUBLE) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.DOUBLE);
    } else if (pa.getDataType() == OracleTypes.TINYINT) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.TINYINT);
    } else if (pa.getDataType() == OracleTypes.REAL) {
      cs.setFloat(pa.getIndex(), OracleTypes.REAL);
    } else if (pa.getDataType() == OracleTypes.SMALLINT) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.SMALLINT);
    } else if (pa.getDataType() == OracleTypes.BIT) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.BIT);
    } else if (pa.getDataType() == OracleTypes.BOOLEAN) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.BOOLEAN);
    } else if (pa.getDataType() == OracleTypes.OTHER
        && pa.getTypeName().equals(DataType.CLOB)) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.CLOB);
    } else if (pa.getDataType() == OracleTypes.OTHER
        && pa.getTypeName().equals(DataType.BLOB)) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.BLOB);
    } else if (pa.getDataType() == OracleTypes.OTHER
        && pa.getTypeName().equals(DataType.TABLE)) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.ARRAY);
    } else if (pa.getDataType() == OracleTypes.BINARY) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.BINARY);
    } else if (pa.getDataType() == OracleTypes.VARBINARY) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.VARBINARY);
    } else if (pa.getDataType() == OracleTypes.LONGVARBINARY) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.LONGVARBINARY);
    } else if (pa.getDataType() == OracleTypes.OTHER
        && pa.getTypeName().equals(DataType.ROW_ID)) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.ROWID);
    } else if (pa.getDataType() == OracleTypes.NULL &&
        pa.getTypeName().equals(DataType.TABLE)) {
      cs.registerOutParameter(pa.getIndex(), OracleTypes.OTHER);
    } else {
      throw new SQLException("dajdbc not support data-type : " + pa.getTypeName() + "-" + pa.getDataType());
    }
  }
}
