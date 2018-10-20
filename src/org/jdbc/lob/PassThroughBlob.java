package org.jdbc.lob;


import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: Nguyen Van Nhat
 * Date: 7/3/13
 * Time: 8:17 AM
 * To change this template use File | Settings | File Templates.
 */
public class PassThroughBlob implements Blob {
  private byte[] content;

  private InputStream binaryStream;

  private long contentLength;


  public PassThroughBlob(byte[] content) {
    if(content == null || content.length == 0) {
      this.content = null;
      this.contentLength = 0;
    }else{
      this.content = content;
      this.contentLength = content.length;
    }
  }

  public PassThroughBlob(InputStream binaryStream, long contentLength) {
    this.binaryStream = binaryStream;
    this.contentLength = contentLength;
  }


  public long length() throws SQLException {
    return this.contentLength;
  }

  public InputStream getBinaryStream() throws SQLException {
    if (this.content == null && this.binaryStream == null)
      return null;
    else
      return (this.content != null ? new ByteArrayInputStream(this.content) : this.binaryStream);
  }


  public InputStream getBinaryStream(long pos, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public OutputStream setBinaryStream(long pos) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public byte[] getBytes(long pos, int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public int setBytes(long pos, byte[] bytes) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public long position(byte pattern[], long start) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public long position(Blob pattern, long start) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void truncate(long len) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void free() throws SQLException {
    // no-op
  }
}
