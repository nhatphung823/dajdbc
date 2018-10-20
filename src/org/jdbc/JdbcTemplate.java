package org.jdbc;

import oracle.jdbc.OracleTypes;
import org.apache.log4j.Logger;
import org.jdbc.mapper.ColumnMapRowMapper;
import org.jdbc.mapper.LinkedCaseInsensitiveMap;
import org.jdbc.mapper.RowMapperResultSetExtractor;
import org.jdbc.mapper.SingleColumnRowMapper;
import org.jdbc.metadata.CallMetadataContext;
import org.jdbc.paramater.SqlParameter;
import org.jdbc.paramater.SqlParameterValue;
import org.jdbc.util.JdbcUtils;
import org.jdbc.util.SqlParameterUtils;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: Nguyen Van Nhat
 * Date: 11/15/12
 * Time: 10:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class JdbcTemplate implements Cloneable {
  private DataSource dataSource = null;
  protected static final Logger logger = Logger.getLogger(JdbcTemplate.class);
  protected static final String RS = "rs";

  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  public JdbcTemplate(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public JdbcTemplate() {
  }

  public Map callProcedure(String procedure, Map<String, Object> inParams) throws SQLException {
    return callProcedure(procedure, inParams, null);
  }

  public Map callProcedure(String procedure, Map<String, Object> inParams, RowMapper rowMapper) throws SQLException {
    if (dataSource == null)
      throw new SQLException("DataSource is null");
    long start = System.currentTimeMillis();
    CallableStatement cs = null;
    Map<String, Object> out;
    Connection conn = null;
    try {
      CallMetadataContext cmc = getCallMetadataContext(procedure);
      cmc.setFunction(false);
      conn = dataSource.getConnection();
      logger.debug(procedure + " >> getConnection : " + (System.currentTimeMillis() - start) + " milliseconds");
      List<SqlParameter> parameters = SqlParameterUtils.getProcedureParameters(conn, cmc.getCatalog(),
          cmc.getSchema(), cmc.getProcedure());
      logger.debug(procedure + " >> getParameters : " + (System.currentTimeMillis() - start) + " milliseconds");
      cmc.setParameterCount(parameters.size());
      cs = conn.prepareCall(cmc.createCallString());
      List<SqlParameterValue> sqlParameterValues = createSqlParameterValues(parameters, inParams);
      SqlParameterUtils.addParameters(conn, cs, sqlParameterValues);
      logger.debug(procedure + " >> setParameters : " + (System.currentTimeMillis() - start) + " milliseconds");
      boolean bl = cs.execute();
      out = extractOutParameter(cs, parameters, rowMapper);
      if (bl) {
        ResultSet rs = cs.getResultSet();
        if (rs != null) {
          RowMapperResultSetExtractor rm = new RowMapperResultSetExtractor(
              rowMapper != null ? rowMapper : new ColumnMapRowMapper()
          );
          List li = rm.extractData(rs);
          out.put(RS, li);
          rs.close();
        }
      }
      logger.debug(procedure + " >> execute : " + (System.currentTimeMillis() - start) + " milliseconds");
    } finally {
      closeAll(cs, conn);
    }
    logger.trace(out);

    return out;
  }

  public Map callFunction(String function, Map<String, Object> inParams) throws SQLException {
    return callFunction(function, inParams, null);
  }

  public Map callFunction(String function, Map<String, Object> inParams, RowMapper rowMapper) throws SQLException {
    if (dataSource == null)
      throw new SQLException("DataSource is null");
    long start = System.currentTimeMillis();
    CallableStatement cs = null;
    Map<String, Object> out;
    ResultSet rs = null;
    Connection conn = null;
    try {
      CallMetadataContext cmc = getCallMetadataContext(function);
      cmc.setFunction(true);
      conn = dataSource.getConnection();
      logger.debug(function + " >> getConnection : " + (System.currentTimeMillis() - start) + " milliseconds");
      List<SqlParameter> parameters = SqlParameterUtils.getFunctionParameters(conn, cmc.getCatalog(),
          cmc.getSchema(), cmc.getProcedure());
      logger.debug(function + " >> getParameters : " + (System.currentTimeMillis() - start) + " milliseconds");
      cmc.setParameterCount(parameters.size());
      cs = conn.prepareCall(cmc.createCallString());
      List<SqlParameterValue> sqlParameterValues = createSqlParameterValues(parameters, inParams);
      SqlParameterUtils.addParameters(conn, cs, sqlParameterValues);
      logger.debug(function + " >> setParameters : " + (System.currentTimeMillis() - start) + " milliseconds");
      boolean bl = cs.execute();
      out = extractOutParameter(cs, parameters, rowMapper);
      if (bl) {
        rs = cs.getResultSet();
        if (rs != null) {
          RowMapperResultSetExtractor rm = new RowMapperResultSetExtractor(
              rowMapper != null ? rowMapper : new ColumnMapRowMapper()
          );
          List li = rm.extractData(rs);
          out.put(RS, li);
        }
      }
      logger.debug(function + " >> execute : " + (System.currentTimeMillis() - start) + " milliseconds");
    } finally {
      closeAll(rs, cs, conn);
    }
    logger.trace(out);

    return out;
  }

  public int executeUpdate(String sql) throws SQLException {
    if (dataSource == null)
      throw new SQLException("DataSource is null");
    Statement s = null;
    Connection conn = null;
    try {
      conn = dataSource.getConnection();
      s = conn.createStatement();
      return s.executeUpdate(sql);
    } finally {
      closeAll(s, conn);
    }
  }

  public int executeUpdate(String sql, Object... params) throws SQLException {
    if (dataSource == null)
      throw new SQLException("DataSource is null");
    PreparedStatement ps = null;
    Connection conn = null;
    try {
      conn = dataSource.getConnection();
      ps = conn.prepareStatement(sql);
      JdbcUtils.addParameter(ps, params);
      return ps.executeUpdate();
    } finally {
      closeAll(ps, conn);
    }
  }

  public List<Map> executeQuery(String sql) throws SQLException {
    return executeQuery(sql, new ColumnMapRowMapper());
  }

  public List executeQuery(String sql, RowMapper rowMapper) throws SQLException {
    if (dataSource == null)
      throw new SQLException("DataSource is null");
    Statement s = null;
    ResultSet rs = null;
    List<Map> list;
    Connection conn = null;
    try {
      conn = dataSource.getConnection();
      s = conn.createStatement();
      rs = s.executeQuery(sql);
      RowMapperResultSetExtractor rm = new RowMapperResultSetExtractor(
          rowMapper != null ? rowMapper : new ColumnMapRowMapper()
      );
      list = rm.extractData(rs);
    } finally {
      closeAll(rs, s, conn);
    }

    return list;
  }

  public List<Map> executeQuery(String sql, Object... params) throws SQLException {
    return executeQuery(sql, new ColumnMapRowMapper(), params);
  }

  public List executeQuery(String sql, RowMapper rowMapper, Object... params) throws SQLException {
    if (dataSource == null)
      throw new SQLException("DataSource is null");
    PreparedStatement ps = null;
    ResultSet rs = null;
    List<Map> list = null;
    Connection conn = null;
    try {
      conn = dataSource.getConnection();
      ps = conn.prepareStatement(sql);
      JdbcUtils.addParameter(ps, params);
      rs = ps.executeQuery();
      RowMapperResultSetExtractor rm = new RowMapperResultSetExtractor(
          rowMapper != null ? rowMapper : new ColumnMapRowMapper()
      );
      list = rm.extractData(rs);
    } finally {
      closeAll(rs, ps, conn);
    }

    return list;
  }

  public int queryForInt(String sql) throws SQLException {
    return queryForObject(sql, Integer.class);
  }

  public int queryForInt(String sql, Object... params) throws SQLException {
    return queryForObject(sql, Integer.class, params);
  }

  public long queryForLong(String sql) throws SQLException {
    return queryForObject(sql, Long.class);
  }

  public long queryForLong(String sql, Object... params) throws SQLException {
    return queryForObject(sql, Long.class, params);
  }

  public float queryForFloat(String sql) throws SQLException {
    return queryForObject(sql, Float.class);
  }

  public float queryForFloat(String sql, Object... params) throws SQLException {
    return queryForObject(sql, Float.class, params);
  }

  public double queryForDouble(String sql) throws SQLException {
    return queryForObject(sql, Double.class);
  }

  public double queryForDouble(String sql, Object... params) throws SQLException {
    return queryForObject(sql, Double.class, params);
  }

  public String queryForString(String sql) throws SQLException {
    return queryForObject(sql, String.class);
  }

  public String queryForString(String sql, Object... params) throws SQLException {
    return queryForObject(sql, String.class, params);
  }

  public <T> T queryForObject(String sql, Class<T> requiredType) throws SQLException {
    List<T> list = query(sql, new SingleColumnRowMapper<T>(requiredType));
    return JdbcUtils.requiredSingleResult(list);
  }

  public <T> T queryForObject(String sql, Class<T> requiredType, Object... params) throws SQLException {
    List<T> list = query(sql, new SingleColumnRowMapper<T>(requiredType), params);
    return JdbcUtils.requiredSingleResult(list);
  }

  public <T> Object queryForObject(String sql, RowMapper<T> rowMapper) throws SQLException {
    List list = executeQuery(sql, rowMapper);
    return JdbcUtils.requiredSingleResult(list);
  }

  public <T> Object queryForObject(String sql, RowMapper<T> rowMapper, Object... params) throws SQLException {
    List list = executeQuery(sql, rowMapper, params);
    return JdbcUtils.requiredSingleResult(list);
  }

  public <T> List<T> queryForList(String sql, Class<T> requiredType) throws SQLException {
    return query(sql, new SingleColumnRowMapper(requiredType));
  }

  public <T> List<T> queryForList(String sql, Class<T> requiredType, Object... params) throws SQLException {
    return query(sql, new SingleColumnRowMapper(requiredType), params);
  }

  private <T> List<T> query(String sql, RowMapper<T> rowMapper) throws SQLException {
    if (dataSource == null)
      throw new SQLException("DataSource is null");
    Statement s = null;
    ResultSet rs = null;
    List<T> list;
    Connection conn = null;
    try {
      conn = dataSource.getConnection();
      s = conn.createStatement();
      rs = s.executeQuery(sql);
      RowMapperResultSetExtractor rm = new RowMapperResultSetExtractor(
          rowMapper != null ? rowMapper : new ColumnMapRowMapper()
      );
      list = rm.extractData(rs);
    } finally {
      closeAll(rs, s, conn);
    }

    return list;
  }
  
  private <T> List<T> query(String sql, RowMapper<T> rowMapper, Object... params) throws SQLException {
    PreparedStatement ps = null;
    ResultSet rs = null;
    List<T> list;
    Connection conn = null;
    try {
      conn = dataSource.getConnection();
      ps = conn.prepareStatement(sql);
      JdbcUtils.addParameter(ps, params);
      rs = ps.executeQuery();
      RowMapperResultSetExtractor rm = new RowMapperResultSetExtractor(
          rowMapper != null ? rowMapper : new ColumnMapRowMapper()
      );
      list = rm.extractData(rs);
    } finally {
      closeAll(rs, ps, conn);
    }

    return list;
  }

  private Map<String, Object> extractOutParameter(CallableStatement cs, List<SqlParameter> list, RowMapper mapper) throws SQLException {
    Map<String, Object> map = new LinkedCaseInsensitiveMap<>();
    if (list.size() > 0) {
      for (SqlParameter pa : list) {
        if (pa.getColumnType() == 5 || pa.getColumnType() == 2 || pa.getColumnType() == 4) {
          if (pa.getDataType() == OracleTypes.VARCHAR
              || pa.getDataType() == OracleTypes.CHAR
              || pa.getDataType() == OracleTypes.LONGVARCHAR) {
            map.put(pa.getColumnName(), cs.getString(pa.getIndex()));
          } else if (pa.getDataType() == OracleTypes.OTHER
              && (pa.getTypeName().equals(DataType.NCHAR) || pa.getTypeName().equals(DataType.NVARCHAR2))) {
            map.put(pa.getColumnName(), cs.getString(pa.getIndex()));
          } else if (pa.getDataType() == OracleTypes.DECIMAL) {
            map.put(pa.getColumnName(), cs.getBigDecimal(pa.getIndex()));
          } else if (pa.getDataType() == OracleTypes.TIMESTAMP
              && pa.getTypeName().equals(DataType.DATE)) {
            Timestamp tt = cs.getTimestamp(pa.getIndex());
            Date d = (tt != null ? new Date(tt.getTime()) : null);
            map.put(pa.getColumnName(), d);
          } else if (pa.getDataType() == OracleTypes.TIMESTAMP
              && pa.getTypeName().equals(DataType.TIMESTAMP)) {
            Timestamp tt = cs.getTimestamp(pa.getIndex());
            Date d = (tt != null ? new Date(tt.getTime()) : null);
            map.put(pa.getColumnName(), d);
          } else if (pa.getDataType() == OracleTypes.OTHER
              && pa.getTypeName().equals(DataType.REF_CURSOR)) {
            ResultSet rs = (ResultSet) cs.getObject(pa.getIndex());
            RowMapperResultSetExtractor rm = new RowMapperResultSetExtractor(
                mapper != null ? mapper : new ColumnMapRowMapper()
            );
            List li = rm.extractData(rs);
            map.put(pa.getColumnName(), li);
            rs.close();
          } else if (pa.getDataType() == OracleTypes.INTEGER) {
            map.put(pa.getColumnName(), cs.getInt(pa.getIndex()));
          } else if (pa.getDataType() == OracleTypes.BIGINT) {
            map.put(pa.getColumnName(), cs.getLong(pa.getIndex()));
          } else if (pa.getDataType() == OracleTypes.FLOAT) {
            map.put(pa.getColumnName(), cs.getFloat(pa.getIndex()));
          } else if (pa.getDataType() == OracleTypes.DOUBLE) {
            map.put(pa.getColumnName(), cs.getDouble(pa.getIndex()));
          } else if (pa.getDataType() == OracleTypes.BIT) {
            map.put(pa.getColumnName(), cs.getBoolean(pa.getIndex()));
          } else if (pa.getDataType() == OracleTypes.BOOLEAN) {
            map.put(pa.getColumnName(), cs.getBoolean(pa.getIndex()));
          } else if (pa.getDataType() == OracleTypes.DATE) {
            Timestamp tt = cs.getTimestamp(pa.getIndex());
            Date d = (tt != null ? new Date(tt.getTime()) : null);
            map.put(pa.getColumnName(), d);
          } else if (pa.getDataType() == OracleTypes.TIME) {
            Timestamp tt = cs.getTimestamp(pa.getIndex());
            Date d = (tt != null ? new Date(tt.getTime()) : null);
            map.put(pa.getColumnName(), d);
          } else if (pa.getTypeName().equals(DataType.BLOB)) {
            Blob blob = cs.getBlob(pa.getIndex());
            if (blob != null) {
              ByteArrayOutputStream baos = new ByteArrayOutputStream();
              byte[] buff = new byte[4096];
              InputStream is = blob.getBinaryStream();
              int r = 0;
              try {
                while ((r = is.read(buff)) > 0) {
                  baos.write(buff, 0 , r);
                }
                map.put(pa.getColumnName(), baos.toByteArray());
              }catch (IOException ioe){
                logger.error(ioe);
                map.put(pa.getColumnName(), null);
              }finally {
                try {
                  baos.flush();
                  baos.close();
                  is.close();
                }catch (IOException e){
                  logger.error(e);
                }
              }
            } else {
              map.put(pa.getColumnName(), null);
            }
          } else if (pa.getTypeName().equals(DataType.CLOB)) {
            Clob clob = cs.getClob(pa.getIndex());
            if (clob != null) {
              BufferedReader reader = null;
              try {
                reader = new BufferedReader(cs.getClob(pa.getIndex()).getCharacterStream());
                String s;
                StringBuilder sb = new StringBuilder("");
                while ((s = reader.readLine()) != null) {
                  sb.append(s);
                }
                s = sb.toString();
                map.put(pa.getColumnName(), s.equals("") ? null : s);
              } catch (IOException ioe) {
                logger.error(ioe);
                map.put(pa.getColumnName(), null);
              } finally {
                if (reader != null)
                  try {
                    reader.close();
                  } catch (IOException e) {
                    logger.error(e);
                  }
              }
            } else {
              map.put(pa.getColumnName(), null);
            }
          } else {
            throw new SQLException("dajdbc not support data type : " + pa.getTypeName());
          }
        }
      }
    }
    return map;
  }

  static final Pattern p1 = Pattern.compile("^([a-z_A-Z0-9$-]+)\\.([a-z_A-Z0-9$-]+)\\.([a-z_A-Z0-9$-]+)$");
  static final Pattern p2 = Pattern.compile("^([a-z_A-Z0-9$-]+)\\.([a-z_A-Z0-9$-]+)$");

  private CallMetadataContext getCallMetadataContext(String procedureName) {
    String[] s = new String[3];
    Matcher m1 = p1.matcher(procedureName);
    Matcher m2 = p2.matcher(procedureName);
    if (m1.find()) {
      s[0] = m1.group(1).toUpperCase();
      s[1] = m1.group(2).toUpperCase();
      s[2] = m1.group(3).toUpperCase();
    } else if (m2.find()) {
      s[0] = null;
      s[1] = m2.group(1).toUpperCase();
      s[2] = m2.group(2).toUpperCase();
    } else {
      s[0] = null;
      s[1] = null;
      s[2] = procedureName.toUpperCase();
    }

    return new CallMetadataContext(s[1], s[0], s[2]);
  }

  private List<SqlParameterValue> createSqlParameterValues(List<SqlParameter> list, Map<String, Object> params) throws SQLException {
    if (params == null || params.size() == 0) {
      if (list.size() == 0) {
        return new ArrayList<>();
      } else {
        String s = "";
        String s1 = "";
        for (SqlParameter sp : list) {
          if (sp.getColumnType() == 5 || sp.getColumnType() == 4) continue;
          s += sp.getColumnName() + ";";
          s1 += sp.getTypeName() + ";";
        }
        if (!s.equals(""))
          throw new SQLException("Missing all parameter : " + s.replaceAll(";$", "") + " - " + s1.replaceAll(";$", ""));
        else
          params = new HashMap<>();
      }
    }
    List<SqlParameterValue> li = new ArrayList<>();
    Map<String, Object> map = new LinkedCaseInsensitiveMap<>();
    map.putAll(params);
    for (SqlParameter sp : list) {
      SqlParameterValue spv = new SqlParameterValue();
      spv.setSqlParameter(sp);
      if (sp.getColumnType() == 5) {
        spv.setValue(null);
      } else if (sp.getColumnType() == 1) {
        if (map.containsKey(sp.getColumnName())) {
          spv.setValue(map.get(sp.getColumnName()));
        } else {
          throw new SQLException("Missing input parameter " + sp.getColumnName() + "-" + sp.getTypeName() + "-" + sp.getDataType());
        }
      } else if (sp.getColumnType() == 4) {
        spv.setValue(null);
      } else if (sp.getColumnType() == 2) {
        if (map.containsKey(sp.getColumnName())) {
          spv.setValue(map.get(sp.getColumnName()));
        } else {
          spv.setValue(null);
        }
      }
      li.add(spv);
    }

    return li;
  }

  public void cachedSqlParameter(String[] procedures, String[] functions) throws SQLException {
    long t = System.currentTimeMillis();
    Connection conn = dataSource.getConnection();
    for (String proc : procedures) {
      CallMetadataContext cmc = getCallMetadataContext(proc);
      SqlParameterUtils.getProcedureParameters(conn, cmc.getCatalog(),
          cmc.getSchema(), cmc.getProcedure());
    }
    for (String func : functions) {
      CallMetadataContext cmc = getCallMetadataContext(func);
      SqlParameterUtils.getFunctionParameters(conn, cmc.getCatalog(),
          cmc.getSchema(), cmc.getProcedure());
    }
    conn.close();
    conn = null;
    logger.debug("cachedSqlParameter : " + (System.currentTimeMillis() - t) + " milliseconds");
  }

  public DataSource getDataSource() {
    return dataSource;
  }

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  private void closeAll(ResultSet rs, Statement s, Connection conn) {
    try {
      if (rs != null) {
        rs.close();
        rs = null;
      }
    } catch (SQLException e) {
      logger.error(e);
    }

    try {
      if (s != null) {
        s.close();
        s = null;
      }
    } catch (SQLException e) {
      logger.error(e);
    }

    try {
      if (conn != null) {
        conn.close();
        conn = null;
      }
    } catch (SQLException e) {
      logger.error(e);
    }
  }

  private void closeAll(Statement s, Connection conn) {
    try {
      if (s != null) {
        s.close();
        s = null;
      }
    } catch (SQLException e) {
      logger.error(e);
    }

    try {
      if (conn != null) {
        conn.close();
        conn = null;
      }
    } catch (SQLException e) {
      logger.error(e);
    }
  }

  private void closeAll(ResultSet rs, PreparedStatement s, Connection conn) {
    try {
      if (rs != null) {
        rs.close();
        rs = null;
      }
    } catch (SQLException e) {
      logger.error(e);
    }

    try {
      if (s != null) {
        s.close();
        s = null;
      }
    } catch (SQLException e) {
      logger.error(e);
    }

    try {
      if (conn != null) {
        conn.close();
        conn = null;
      }
    } catch (SQLException e) {
      logger.error(e);
    }
  }

  private void closeAll(PreparedStatement s, Connection conn) {
    try {
      if (s != null) {
        s.close();
        s = null;
      }
    } catch (SQLException e) {
      logger.error(e);
    }

    try {
      if (conn != null) {
        conn.close();
        conn = null;
      }
    } catch (SQLException e) {
      logger.error(e);
    }
  }

  private void closeAll(ResultSet rs, CallableStatement s, Connection conn) {
    try {
      if (rs != null) {
        rs.close();
        rs = null;
      }
    } catch (SQLException e) {
      logger.error(e);
    }

    try {
      if (s != null) {
        s.close();
        s = null;
      }
    } catch (SQLException e) {
      logger.error(e);
    }

    try {
      if (conn != null) {
        conn.close();
        conn = null;
      }
    } catch (SQLException e) {
      logger.error(e);
    }
  }

  private void closeAll(CallableStatement s, Connection conn) {
    try {
      if (s != null) {
        s.close();
        s = null;
      }
    } catch (SQLException e) {
      logger.error(e);
    }

    try {
      if (conn != null) {
        conn.close();
        conn = null;
      }
    } catch (SQLException e) {
      logger.error(e);
    }
  }
}
