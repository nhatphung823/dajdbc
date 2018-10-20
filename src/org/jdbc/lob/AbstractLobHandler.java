package org.jdbc.lob;

import java.io.InputStream;
import java.io.Reader;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: Nguyen Van Nhat
 * Date: 7/3/13
 * Time: 8:01 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractLobHandler implements LobHandler {
  public byte[] getBlobAsBytes(ResultSet rs, String columnName) throws SQLException {
    return getBlobAsBytes(rs, rs.findColumn(columnName));
  }

  public InputStream getBlobAsBinaryStream(ResultSet rs, String columnName) throws SQLException {
    return getBlobAsBinaryStream(rs, rs.findColumn(columnName));
  }

  public String getClobAsString(ResultSet rs, String columnName) throws SQLException {
    return getClobAsString(rs, rs.findColumn(columnName));
  }

  public InputStream getClobAsAsciiStream(ResultSet rs, String columnName) throws SQLException {
    return getClobAsAsciiStream(rs, rs.findColumn(columnName));
  }

  public Reader getClobAsCharacterStream(ResultSet rs, String columnName) throws SQLException {
    return getClobAsCharacterStream(rs, rs.findColumn(columnName));

  }
}
