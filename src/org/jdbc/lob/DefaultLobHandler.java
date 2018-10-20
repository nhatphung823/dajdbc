package org.jdbc.lob;

import java.io.*;
import java.sql.*;

/**
 * Created with IntelliJ IDEA.
 * User: Nguyen Van Nhat
 * Date: 7/3/13
 * Time: 7:54 AM
 * To change this template use File | Settings | File Templates.
 */
public class DefaultLobHandler extends AbstractLobHandler {
  private boolean wrapAsLob = false;

  private boolean streamAsLob = false;

  public void setWrapAsLob(boolean wrapAsLob) {
    this.wrapAsLob = wrapAsLob;
  }

  public void setStreamAsLob(boolean streamAsLob) {
    this.streamAsLob = streamAsLob;
  }

  @Override
  public byte[] getBlobAsBytes(ResultSet rs, int columnIndex) throws SQLException {
    if (this.wrapAsLob) {
      Blob blob = rs.getBlob(columnIndex);
      return blob.getBytes(1, (int) blob.length());
    } else {
      return rs.getBytes(columnIndex);
    }
  }

  @Override
  public InputStream getBlobAsBinaryStream(ResultSet rs, int columnIndex) throws SQLException {
    if (this.wrapAsLob) {
      Blob blob = rs.getBlob(columnIndex);
      return blob.getBinaryStream();
    } else {
      return rs.getBinaryStream(columnIndex);
    }
  }

  @Override
  public String getClobAsString(ResultSet rs, int columnIndex) throws SQLException {
    if (this.wrapAsLob) {
      Clob clob = rs.getClob(columnIndex);
      return clob.getSubString(1, (int) clob.length());
    } else {
      return rs.getString(columnIndex);
    }
  }

  @Override
  public InputStream getClobAsAsciiStream(ResultSet rs, int columnIndex) throws SQLException {
    if (this.wrapAsLob) {
      Clob clob = rs.getClob(columnIndex);
      return clob.getAsciiStream();
    } else {
      return rs.getAsciiStream(columnIndex);
    }
  }

  @Override
  public Reader getClobAsCharacterStream(ResultSet rs, int columnIndex) throws SQLException {
    if (this.wrapAsLob) {
      Clob clob = rs.getClob(columnIndex);
      return clob.getCharacterStream();
    } else {
      return rs.getCharacterStream(columnIndex);
    }
  }

  @Override
  public LobCreator getLobCreator() {
    return new DefaultLobCreator();
  }

  class DefaultLobCreator implements LobCreator {
    private boolean wrapAsLob = false;

    private boolean streamAsLob = false;

    public void setBlobAsBytes(PreparedStatement ps, int paramIndex, byte[] content)
        throws SQLException {

      if (streamAsLob) {
        if (content != null) {
          ps.setBlob(paramIndex, new ByteArrayInputStream(content), content.length);
        } else {
          ps.setBlob(paramIndex, (Blob) null);
        }
      } else if (wrapAsLob) {
        if (content != null) {
          ps.setBlob(paramIndex, new PassThroughBlob(content));
        } else {
          ps.setBlob(paramIndex, (Blob) null);
        }
      } else {
        ps.setBytes(paramIndex, content);
      }
    }

    public void setBlobAsBinaryStream(
        PreparedStatement ps, int paramIndex, InputStream binaryStream, int contentLength)
        throws SQLException {

      if (streamAsLob) {
        if (binaryStream != null) {
          ps.setBlob(paramIndex, binaryStream, contentLength);
        } else {
          ps.setBlob(paramIndex, (Blob) null);
        }
      } else if (wrapAsLob) {
        if (binaryStream != null) {
          ps.setBlob(paramIndex, new PassThroughBlob(binaryStream, contentLength));
        } else {
          ps.setBlob(paramIndex, (Blob) null);
        }
      } else {
        ps.setBinaryStream(paramIndex, binaryStream, contentLength);
      }
    }

    public void setClobAsString(PreparedStatement ps, int paramIndex, String content)
        throws SQLException {

      if (streamAsLob) {
        if (content != null) {
          ps.setClob(paramIndex, new StringReader(content), content.length());
        } else {
          ps.setClob(paramIndex, (Clob) null);
        }
      } else if (wrapAsLob) {
        if (content != null) {
          ps.setClob(paramIndex, new PassThroughClob(content));
        } else {
          ps.setClob(paramIndex, (Clob) null);
        }
      } else {
        ps.setString(paramIndex, content);
      }
    }

    public void setClobAsAsciiStream(
        PreparedStatement ps, int paramIndex, InputStream asciiStream, int contentLength)
        throws SQLException {

      if (streamAsLob || wrapAsLob) {
        if (asciiStream != null) {
          try {
            if (streamAsLob) {
              ps.setClob(paramIndex, new InputStreamReader(asciiStream, "US-ASCII"), contentLength);
            } else {
              ps.setClob(paramIndex, new PassThroughClob(asciiStream, contentLength));
            }
          } catch (UnsupportedEncodingException ex) {
            throw new SQLException("US-ASCII encoding not supported: " + ex);
          }
        } else {
          ps.setClob(paramIndex, (Clob) null);
        }
      } else {
        ps.setAsciiStream(paramIndex, asciiStream, contentLength);
      }
    }

    public void setClobAsCharacterStream(
        PreparedStatement ps, int paramIndex, Reader characterStream, int contentLength)
        throws SQLException {

      if (streamAsLob) {
        if (characterStream != null) {
          ps.setClob(paramIndex, characterStream, contentLength);
        } else {
          ps.setClob(paramIndex, (Clob) null);
        }
      } else if (wrapAsLob) {
        if (characterStream != null) {
          ps.setClob(paramIndex, new PassThroughClob(characterStream, contentLength));
        } else {
          ps.setClob(paramIndex, (Clob) null);
        }
      } else {
        ps.setCharacterStream(paramIndex, characterStream, contentLength);
      }
    }

    public void close() {
      // nothing to do here
    }
  }
}
