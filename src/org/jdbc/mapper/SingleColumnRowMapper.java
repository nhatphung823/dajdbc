package org.jdbc.mapper;

import org.jdbc.RowMapper;
import org.jdbc.util.JdbcUtils;
import org.jdbc.util.NumberUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: Nguyen Van Nhat
 * Date: 11/23/12
 * Time: 4:38 PM
 * To change this template use File | Settings | File Templates.
 */
public class SingleColumnRowMapper<T> implements RowMapper<T> {
  private Class<T> requiredType;

  public SingleColumnRowMapper() {
  }

  public SingleColumnRowMapper(Class<T> requiredType) {
    this.requiredType = requiredType;
  }

  public void setRequiredType(Class<T> requiredType) {
    this.requiredType = requiredType;
  }

  public T mapRow(ResultSet rs, int rowNum) throws SQLException {
    ResultSetMetaData rsmd = rs.getMetaData();
    int nrOfColumns = rsmd.getColumnCount();
    if (nrOfColumns != 1) {
      throw new SQLException("sql-query get more than one column");
    }

    Object result = getColumnValue(rs, 1, this.requiredType);
    if (result != null && this.requiredType != null && !this.requiredType.isInstance(result)) {
      try {
        return (T) convertValueToRequiredType(result, this.requiredType);
      } catch (IllegalArgumentException ex) {
        throw new SQLException(
            "Type mismatch affecting row number " + rowNum + " and column type '" +
                rsmd.getColumnTypeName(1) + "': " + ex.getMessage());
      }
    }
    return (T) result;
  }

  protected Object getColumnValue(ResultSet rs, int index, Class requiredType) throws SQLException {
    if (requiredType != null) {
      return JdbcUtils.getResultSetValue(rs, index, requiredType);
    } else {
      return getColumnValue(rs, index);
    }
  }

  protected Object getColumnValue(ResultSet rs, int index) throws SQLException {
    return JdbcUtils.getResultSetValue(rs, index);
  }

  protected Object convertValueToRequiredType(Object value, Class requiredType) {
    if (String.class.equals(requiredType)) {
      return value.toString();
    } else if (Number.class.isAssignableFrom(requiredType)) {
      if (value instanceof Number) {
        return NumberUtils.convertNumberToTargetClass(((Number) value), requiredType);
      } else {
        return NumberUtils.parseNumber(value.toString(), requiredType);
      }
    } else {
      throw new IllegalArgumentException(
          "Value [" + value + "] is of type [" + value.getClass().getName() +
              "] and cannot be converted to required type [" + requiredType.getName() + "]");
    }
  }

}
