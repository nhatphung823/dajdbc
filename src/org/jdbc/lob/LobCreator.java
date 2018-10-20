package org.jdbc.lob;

import java.io.InputStream;
import java.io.Reader;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: Nguyen Van Nhat
 * Date: 7/3/13
 * Time: 7:58 AM
 * To change this template use File | Settings | File Templates.
 */
public interface LobCreator {
  void setBlobAsBytes(PreparedStatement ps, int paramIndex, byte[] content)
      throws SQLException;

  void setBlobAsBinaryStream(
      PreparedStatement ps, int paramIndex, InputStream contentStream, int contentLength)
      throws SQLException;

  void setClobAsString(PreparedStatement ps, int paramIndex, String content)
      throws SQLException;

  void setClobAsAsciiStream(
      PreparedStatement ps, int paramIndex, InputStream asciiStream, int contentLength)
      throws SQLException;

  void setClobAsCharacterStream(
      PreparedStatement ps, int paramIndex, Reader characterStream, int contentLength)
      throws SQLException;


  void close();
}
