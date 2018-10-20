package org.jdbc.lob;

import java.io.InputStream;
import java.io.Reader;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: Nguyen Van Nhat
 * Date: 7/3/13
 * Time: 7:54 AM
 * To change this template use File | Settings | File Templates.
 */
public interface LobHandler {
  byte[] getBlobAsBytes(ResultSet rs, String columnName) throws SQLException;

  byte[] getBlobAsBytes(ResultSet rs, int columnIndex) throws SQLException;

  InputStream getBlobAsBinaryStream(ResultSet rs, String columnName) throws SQLException;

  InputStream getBlobAsBinaryStream(ResultSet rs, int columnIndex) throws SQLException;

  String getClobAsString(ResultSet rs, String columnName) throws SQLException;

  String getClobAsString(ResultSet rs, int columnIndex) throws SQLException;

  InputStream getClobAsAsciiStream(ResultSet rs, String columnName) throws SQLException;

  InputStream getClobAsAsciiStream(ResultSet rs, int columnIndex) throws SQLException;

  Reader getClobAsCharacterStream(ResultSet rs, String columnName) throws SQLException;

  Reader getClobAsCharacterStream(ResultSet rs, int columnIndex) throws SQLException;

  LobCreator getLobCreator();
}
