package org.jdbc.lob;

import org.jdbc.util.FileCopyUtils;

import java.io.*;
import java.sql.Clob;
import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: Nguyen Van Nhat
 * Date: 7/3/13
 * Time: 8:18 AM
 * To change this template use File | Settings | File Templates.
 */
public class PassThroughClob implements Clob {
  private String content;

  private Reader characterStream;

  private InputStream asciiStream;

  private long contentLength;


  public PassThroughClob(String content) {
    if (content == null || "".equals(this.content)) {
      this.content = null;
      this.contentLength = 0;
    } else {
      this.content = content;
      this.contentLength = content.length();
    }
  }

  public PassThroughClob(Reader characterStream, long contentLength) {
    this.characterStream = characterStream;
    this.contentLength = contentLength;
  }

  public PassThroughClob(InputStream asciiStream, long contentLength) {
    this.asciiStream = asciiStream;
    this.contentLength = contentLength;
  }


  public long length() throws SQLException {
    return this.contentLength;
  }

  public Reader getCharacterStream() throws SQLException {
    try {
      if (this.content == null && this.characterStream == null && this.asciiStream == null) {
        return null;
      } else {
        if (this.content != null) {
          return new StringReader(this.content);
        } else if (this.characterStream != null) {
          return this.characterStream;
        } else {
          return new InputStreamReader(this.asciiStream, "US-ASCII");
        }
      }
    } catch (UnsupportedEncodingException ex) {
      throw new SQLException("US-ASCII encoding not supported: " + ex);
    }
  }

  public InputStream getAsciiStream() throws SQLException {
    try {
      if (this.content == null && this.characterStream == null && this.asciiStream == null) {
        return null;
      } else {
        if (this.content != null) {
          return new ByteArrayInputStream(this.content.getBytes("US-ASCII"));
        } else if (this.characterStream != null) {
          String tempContent = FileCopyUtils.copyToString(this.characterStream);
          return new ByteArrayInputStream(tempContent.getBytes("US-ASCII"));
        } else {
          return this.asciiStream;
        }
      }
    } catch (UnsupportedEncodingException ex) {
      throw new SQLException("US-ASCII encoding not supported: " + ex);
    } catch (IOException ex) {
      throw new SQLException("Failed to read stream content: " + ex);
    }
  }


  public Reader getCharacterStream(long pos, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public Writer setCharacterStream(long pos) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public OutputStream setAsciiStream(long pos) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public String getSubString(long pos, int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public int setString(long pos, String str) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public int setString(long pos, String str, int offset, int len) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public long position(String searchstr, long start) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public long position(Clob searchstr, long start) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void truncate(long len) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void free() throws SQLException {
    // no-op
  }
}
