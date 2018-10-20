package org.jdbc.paramater;

/**
 * Created with IntelliJ IDEA.
 * User: Nguyen Van Nhat
 * Date: 11/16/12
 * Time: 8:54 AM
 * To change this template use File | Settings | File Templates.
 */
public class SqlParameter {
  private int index;
  private String columnName;
  private int columnType;
  private String typeName;
  private int dataType;

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public int getColumnType() {
    return columnType;
  }

  public void setColumnType(int columnType) {
    this.columnType = columnType;
  }

  public String getTypeName() {
    return typeName;
  }

  public void setTypeName(String typeName) {
    this.typeName = typeName;
  }

  public int getDataType() {
    return dataType;
  }

  public void setDataType(int dataType) {
    this.dataType = dataType;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("SqlParameter{");
    sb.append("index=").append(index);
    sb.append(", columnName='").append(columnName).append('\'');
    sb.append(", columnType=").append(columnType);
    sb.append(", typeName='").append(typeName).append('\'');
    sb.append(", dataType=").append(dataType);
    sb.append('}');
    return sb.toString();
  }
}
