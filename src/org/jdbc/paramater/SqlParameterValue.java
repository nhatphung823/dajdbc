package org.jdbc.paramater;

/**
 * Created with IntelliJ IDEA.
 * User: Nguyen Van Nhat
 * Date: 11/16/12
 * Time: 9:40 AM
 * To change this template use File | Settings | File Templates.
 */
public class SqlParameterValue {
  private SqlParameter sqlParameter;
  private Object value;

  public SqlParameter getSqlParameter() {
    return sqlParameter;
  }

  public void setSqlParameter(SqlParameter sqlParameter) {
    this.sqlParameter = sqlParameter;
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("SqlParameterValue{");
    sb.append("sqlParameter=").append(sqlParameter);
    sb.append(", value=").append(value);
    sb.append('}');
    return sb.toString();
  }
}
