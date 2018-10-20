package org.jdbc.metadata;

import org.apache.log4j.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: Nguyen Van Nhat
 * Date: 11/16/12
 * Time: 9:05 AM
 * To change this template use File | Settings | File Templates.
 */
public class CallMetadataContext {
  private String catalog;
  private String schema;
  private String procedure;
  private int parameterCount = 0;
  private boolean function;
  static Logger logger = Logger.getLogger(CallMetadataContext.class);

  public int getParameterCount() {
    return parameterCount;
  }

  public void setParameterCount(int parameterCount) {
    this.parameterCount = parameterCount;
  }

  public CallMetadataContext(String catalog, String schema, String procedure) {
    this.catalog = catalog;
    this.schema = schema;
    this.procedure = procedure;
  }

  public String createCallString() {
    StringBuilder sb = new StringBuilder();
    sb.append(isFunction() ? "{? = call " : "{call ");
    if (schema != null && !schema.trim().equals("")) {
      sb.append(schema).append(".");
    }
    if (catalog != null && !catalog.trim().equals("")) {
      sb.append(catalog).append(".");
    }
    sb.append(procedure).append("(");
    if (isFunction()) {
      if (parameterCount > 1) {
        for (int i = 1; i < parameterCount; i++) {
          sb.append("?, ");
        }
        sb = sb.delete(sb.length() - 2, sb.length());
      }
    } else {
      if (parameterCount > 0) {
        for (int i = 0; i < parameterCount; i++) {
          sb.append("?, ");
        }
        sb = sb.delete(sb.length() - 2, sb.length());
      }
    }
    sb.append(")}");
    logger.trace("build sql call : " + sb.toString());

    return sb.toString();
  }

  public String getCatalog() {
    return catalog;
  }

  public void setCatalog(String catalog) {
    this.catalog = catalog;
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public boolean isFunction() {
    return function;
  }

  public void setFunction(boolean function) {
    this.function = function;
  }

  public String getProcedure() {
    return procedure;
  }

  public void setProcedure(String procedure) {
    this.procedure = procedure;
  }
}
