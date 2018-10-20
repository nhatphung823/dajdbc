package org.jdbc.array;

/**
 * Created with IntelliJ IDEA.
 * User: Nguyen Van Nhat
 * Date: 7/3/13
 * Time: 11:37 AM
 * To change this template use File | Settings | File Templates.
 */
public class PassThroughArray{
  private String nameType;
  private Object[] array;

  public PassThroughArray() {
  }

  public PassThroughArray(String nameType) {
    this.nameType = nameType;
  }

  public PassThroughArray(Object[] array) {
    this.array = array;
  }

  public PassThroughArray(String nameType, Object[] array) {
    this.nameType = nameType;
    this.array = array;
  }

  public String getNameType() {
    return nameType;
  }

  public void setNameType(String nameType) {
    this.nameType = nameType;
  }

  public Object[] getArray() {
    return array;
  }

  public void setArray(Object[] array) {
    this.array = array;
  }
}
