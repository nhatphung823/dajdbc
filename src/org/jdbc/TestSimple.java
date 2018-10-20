package org.jdbc;

import oracle.jdbc.pool.OracleDataSource;
import org.jdbc.lob.PassThroughClob;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Joe on 9/29/2015.
 */
public class TestSimple {
  public static void main(String[] args) throws SQLException {
    OracleDataSource ds = new OracleDataSource();
    ds.setURL("jdbc:oracle:thin:@192.168.130.28:1521:edong");
    ds.setUser("edongz_dev");
    ds.setPassword("edongz_dev");
//    System.out.println(ds.getConnection());
    JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
    Map map1 = new HashMap();
    map1.put("param1", new PassThroughClob(" "));
    System.out.println(jdbcTemplate.callProcedure("pkg_test.test1",null));

//    List<Map> list1 = jdbcTemplate.executeQuery("select * from tb_area");
//    for(Map map : list1){
//      System.out.println(map);
//    }

//    int i = jdbcTemplate.executeUpdate("update tb_area set s_desc = 'fuck-off' where n_id=0");
//    System.out.println(i);
  }
}
