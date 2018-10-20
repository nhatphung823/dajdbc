package org.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: Nguyen Van Nhat
 * Date: 11/16/12
 * Time: 4:27 PM
 * To change this template use File | Settings | File Templates.
 */
public interface RowMapper<T> {
  T mapRow(ResultSet rs, int rowNum) throws SQLException;
}
