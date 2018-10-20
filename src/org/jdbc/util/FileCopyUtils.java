package org.jdbc.util;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: Nguyen Van Nhat
 * Date: 7/3/13
 * Time: 8:19 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class FileCopyUtils {
  public static final int BUFFER_SIZE = 4096;

  public static int copy(File in, File out) throws IOException {
    Assert.notNull(in, "No input File specified");
    Assert.notNull(out, "No output File specified");
    return copy(new BufferedInputStream(new FileInputStream(in)),
        new BufferedOutputStream(new FileOutputStream(out)));
  }

  public static void copy(byte[] in, File out) throws IOException {
    Assert.notNull(in, "No input byte array specified");
    Assert.notNull(out, "No output File specified");
    ByteArrayInputStream inStream = new ByteArrayInputStream(in);
    OutputStream outStream = new BufferedOutputStream(new FileOutputStream(out));
    copy(inStream, outStream);
  }

  public static byte[] copyToByteArray(File in) throws IOException {
    Assert.notNull(in, "No input File specified");
    return copyToByteArray(new BufferedInputStream(new FileInputStream(in)));
  }

  public static int copy(InputStream in, OutputStream out) throws IOException {
    Assert.notNull(in, "No InputStream specified");
    Assert.notNull(out, "No OutputStream specified");
    try {
      int byteCount = 0;
      byte[] buffer = new byte[BUFFER_SIZE];
      int bytesRead = -1;
      while ((bytesRead = in.read(buffer)) != -1) {
        out.write(buffer, 0, bytesRead);
        byteCount += bytesRead;
      }
      out.flush();
      return byteCount;
    } finally {
      try {
        in.close();
      } catch (IOException ex) {
      }
      try {
        out.close();
      } catch (IOException ex) {
      }
    }
  }

  public static void copy(byte[] in, OutputStream out) throws IOException {
    Assert.notNull(in, "No input byte array specified");
    Assert.notNull(out, "No OutputStream specified");
    try {
      out.write(in);
    } finally {
      try {
        out.close();
      } catch (IOException ex) {
      }
    }
  }

  public static byte[] copyToByteArray(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream(BUFFER_SIZE);
    copy(in, out);
    return out.toByteArray();
  }

  public static int copy(Reader in, Writer out) throws IOException {
    Assert.notNull(in, "No Reader specified");
    Assert.notNull(out, "No Writer specified");
    try {
      int byteCount = 0;
      char[] buffer = new char[BUFFER_SIZE];
      int bytesRead = -1;
      while ((bytesRead = in.read(buffer)) != -1) {
        out.write(buffer, 0, bytesRead);
        byteCount += bytesRead;
      }
      out.flush();
      return byteCount;
    } finally {
      try {
        in.close();
      } catch (IOException ex) {
      }
      try {
        out.close();
      } catch (IOException ex) {
      }
    }
  }

  public static void copy(String in, Writer out) throws IOException {
    Assert.notNull(in, "No input String specified");
    Assert.notNull(out, "No Writer specified");
    try {
      out.write(in);
    } finally {
      try {
        out.close();
      } catch (IOException ex) {
      }
    }
  }

  public static String copyToString(Reader in) throws IOException {
    StringWriter out = new StringWriter();
    copy(in, out);
    return out.toString();
  }
}
