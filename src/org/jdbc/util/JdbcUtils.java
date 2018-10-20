package org.jdbc.util;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Collection;

/**
 * Created with IntelliJ IDEA.
 * User: Nguyen Van Nhat
 * Date: 11/23/12
 * Time: 4:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class JdbcUtils {
  public static Object getResultSetValue(ResultSet rs, int index) throws SQLException {
    Object obj = rs.getObject(index);
    String className = null;
    if (obj != null) {
      className = obj.getClass().getName();
    }
    if (obj instanceof Blob) {
      obj = rs.getBytes(index);
    } else if (obj instanceof Clob) {
      obj = rs.getString(index);
    } else if (className != null &&
        ("oracle.sql.TIMESTAMP".equals(className) ||
            "oracle.sql.TIMESTAMPTZ".equals(className))) {
      obj = rs.getTimestamp(index) != null ? new Date(rs.getTimestamp(index).getTime()) : null;
    } else if (className != null && className.startsWith("oracle.sql.DATE")) {
      String metaDataClassName = rs.getMetaData().getColumnClassName(index);
      if ("java.sql.Timestamp".equals(metaDataClassName) ||
          "oracle.sql.TIMESTAMP".equals(metaDataClassName)) {
        obj = rs.getTimestamp(index) != null ? new Date(rs.getTimestamp(index).getTime()) : null;
      } else {
        obj = rs.getTimestamp(index) != null ? new Date(rs.getTimestamp(index).getTime()) : null;
      }
    } else if (obj != null && obj instanceof java.sql.Date) {
      if ("java.sql.Timestamp".equals(rs.getMetaData().getColumnClassName(index))) {
        obj = rs.getTimestamp(index) != null ? new Date(rs.getTimestamp(index).getTime()) : null;
      }
    }

    return obj;
  }

  public static Object getResultSetValue(ResultSet rs, int index, Class requiredType) throws SQLException {
    if (requiredType == null) {
      return getResultSetValue(rs, index);
    }

    Object value;
    boolean wasNullCheck = false;

    // Explicitly extract typed value, as far as possible.
    if (String.class.equals(requiredType)) {
      value = rs.getString(index);
    } else if (boolean.class.equals(requiredType) || Boolean.class.equals(requiredType)) {
      value = rs.getBoolean(index);
      wasNullCheck = true;
    } else if (byte.class.equals(requiredType) || Byte.class.equals(requiredType)) {
      value = rs.getByte(index);
      wasNullCheck = true;
    } else if (short.class.equals(requiredType) || Short.class.equals(requiredType)) {
      value = rs.getShort(index);
      wasNullCheck = true;
    } else if (int.class.equals(requiredType) || Integer.class.equals(requiredType)) {
      value = rs.getInt(index);
      wasNullCheck = true;
    } else if (long.class.equals(requiredType) || Long.class.equals(requiredType)) {
      value = rs.getLong(index);
      wasNullCheck = true;
    } else if (float.class.equals(requiredType) || Float.class.equals(requiredType)) {
      value = rs.getFloat(index);
      wasNullCheck = true;
    } else if (double.class.equals(requiredType) || Double.class.equals(requiredType) ||
        Number.class.equals(requiredType)) {
      value = rs.getDouble(index);
      wasNullCheck = true;
    } else if (byte[].class.equals(requiredType)) {
      value = rs.getBytes(index);
    } else if (java.sql.Date.class.equals(requiredType)) {
      value = rs.getTimestamp(index) != null ? new Date(rs.getTimestamp(index).getTime()) : null;
    } else if (java.sql.Time.class.equals(requiredType)) {
      value = rs.getTimestamp(index) != null ? new Date(rs.getTimestamp(index).getTime()) : null;
    } else if (java.sql.Timestamp.class.equals(requiredType) || java.util.Date.class.equals(requiredType)) {
      value = rs.getTimestamp(index) != null ? new Date(rs.getTimestamp(index).getTime()) : null;
    } else if (BigDecimal.class.equals(requiredType)) {
      value = rs.getBigDecimal(index);
    } else if (Blob.class.equals(requiredType)) {
      value = rs.getBlob(index);
    } else if (Clob.class.equals(requiredType)) {
      value = rs.getClob(index);
    } else {
      value = getResultSetValue(rs, index);
    }
    if (wasNullCheck && rs.wasNull()) {
      value = null;
    }
    return value;
  }

  public static <T> T requiredSingleResult(Collection<T> results) throws SQLException {
    int size = (results != null ? results.size() : 0);
    if (size == 0) {
      throw new SQLException("no data found");
    }
    if (results.size() > 1) {
      throw new SQLException("too many rows");
    }
    return results.iterator().next();
  }

  public static String lookupColumnName(ResultSetMetaData resultSetMetaData, int columnIndex) throws SQLException {
    String name = resultSetMetaData.getColumnLabel(columnIndex);
    if (name == null || name.length() < 1) {
      name = resultSetMetaData.getColumnName(columnIndex);
    }
    return name;
  }

  public static void addParameter(PreparedStatement ps, Object... params) throws SQLException {
    if (params != null && params.length > 0) {
      int i = 1;
      for (Object o : params) {
        if (o instanceof String) {
          ps.setString(i, (String) o);
        } else if (o instanceof Number) {
          if (o instanceof Integer) {
            ps.setInt(i, (Integer) o);
          } else if (o instanceof Long) {
            ps.setLong(i, (Long) o);
          } else if (o instanceof Float) {
            ps.setFloat(i, (Float) o);
          } else if (o instanceof Double) {
            ps.setDouble(i, (Double) o);
          } else {
            ps.setByte(i, (Byte) o);
          }
        } else if (o instanceof Date) {
          ps.setTimestamp(i, new Timestamp(((Date) o).getTime()));
        } else {
          ps.setObject(i, o);
        }
        i++;
      }
    } else {
      throw new SQLException("Array parameters is empty");
    }
  }
}
