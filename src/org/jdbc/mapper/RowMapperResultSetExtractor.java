package org.jdbc.mapper;

import org.jdbc.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Nguyen Van Nhat
 * Date: 11/16/12
 * Time: 5:13 PM
 * To change this template use File | Settings | File Templates.
 */
public class RowMapperResultSetExtractor<T> implements ResultSetExtractor<List<T>> {
  private RowMapper<T> rowMapper;

  public RowMapperResultSetExtractor(RowMapper<T> rowMapper) {
    this.rowMapper = rowMapper;
  }

  public RowMapper<T> getRowMapper() {
    return rowMapper;
  }

  public void setRowMapper(RowMapper<T> rowMapper) {
    this.rowMapper = rowMapper;
  }

  public List<T> extractData(ResultSet rs) throws SQLException {
    List<T> results = new ArrayList<T>();
    int rowNum = 0;
    while (rs.next()) {
      results.add(this.rowMapper.mapRow(rs, rowNum++));
    }

    rs.close();

    return results;
  }
}
