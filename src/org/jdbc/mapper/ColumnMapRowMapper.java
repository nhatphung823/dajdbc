package org.jdbc.mapper;

import org.jdbc.RowMapper;
import org.jdbc.util.JdbcUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Nguyen Van Nhat
 * Date: 11/24/12
 * Time: 12:38 PM
 * To change this template use File | Settings | File Templates.
 */
public class ColumnMapRowMapper implements RowMapper<Map<String, Object>> {
  public Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
    ResultSetMetaData rsmd = rs.getMetaData();
    int columnCount = rsmd.getColumnCount();
    Map<String, Object> mapOfColValues = createColumnMap(columnCount);
    for (int i = 1; i <= columnCount; i++) {
      String key = getColumnKey(JdbcUtils.lookupColumnName(rsmd, i));
      Object obj = getColumnValue(rs, i);
      mapOfColValues.put(key, obj);
    }
    return mapOfColValues;
  }

  protected Map<String, Object> createColumnMap(int columnCount) {
    return new LinkedCaseInsensitiveMap<>(columnCount);
  }

  protected String getColumnKey(String columnName) {
    return columnName;
  }

  protected Object getColumnValue(ResultSet rs, int index) throws SQLException {
    return JdbcUtils.getResultSetValue(rs, index);
  }
}
