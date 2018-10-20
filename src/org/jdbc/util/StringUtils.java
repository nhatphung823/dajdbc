package org.jdbc.util;

/**
 * Created with IntelliJ IDEA.
 * User: Nguyen Van Nhat
 * Date: 11/23/12
 * Time: 4:46 PM
 * To change this template use File | Settings | File Templates.
 */
public class StringUtils {
  public static String trimAllWhitespace(String str) {
    if (!hasLength(str)) {
      return str;
    }
    StringBuilder sb = new StringBuilder(str);
    int index = 0;
    while (sb.length() > index) {
      if (Character.isWhitespace(sb.charAt(index))) {
        sb.deleteCharAt(index);
      } else {
        index++;
      }
    }
    return sb.toString();
  }

  public static boolean hasLength(String str) {
    return hasLength((CharSequence) str);
  }

  public static boolean hasLength(CharSequence str) {
    return (str != null && str.length() > 0);
  }
}
