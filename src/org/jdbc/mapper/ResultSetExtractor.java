package org.jdbc.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: Nguyen Van Nhat
 * Date: 11/16/12
 * Time: 5:15 PM
 * To change this template use File | Settings | File Templates.
 */
public interface ResultSetExtractor<T>  {
  T extractData(ResultSet rs) throws SQLException;
}
