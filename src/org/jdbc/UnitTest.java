package org.jdbc;

import org.jdbc.array.PassThroughArray;
import org.jdbc.lob.PassThroughBlob;
import org.jdbc.lob.PassThroughClob;

import java.io.*;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Nguyen Van Nhat
 * Date: 11/17/12
 * Time: 9:17 AM
 * To change this template use File | Settings | File Templates.
 */
public class UnitTest {
  static com.jolbox.bonecp.BoneCPDataSource dataSource;

  private static void init() throws SQLException {
    dataSource = new com.jolbox.bonecp.BoneCPDataSource();
//    oracle.ucp.jdbc.PoolDataSource ucpDataSource = oracle.ucp.jdbc.PoolDataSourceFactory.getPoolDataSource();

    dataSource.setPartitionCount(4);
    dataSource.setDriverClass("oracle.jdbc.OracleDriver");
//    dataSource.setDriverClass("com.mysql.jdbc.Driver");
//    dataSource.setDriverClass("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    dataSource.setJdbcUrl("jdbc:oracle:thin:@10.10.13.13:1521:orcldb");
//    dataSource.setJdbcUrl("jdbc:mysql://192.168.1.107:3306/test");
//    dataSource.setJdbcUrl("jdbc:sqlserver://113.190.240.161:1433;databaseName=EVNPostPaid");
    dataSource.setUsername("evn_services");
//    dataSource.setUsername("nvnhat");
//    dataSource.setUsername("sa");
    dataSource.setPassword("evn_servicesls108");
//    dataSource.setPassword("nvnhat123");
//    dataSource.setPassword("admin12#$");
//    dataSource.setAcquireIncrement(1);
//    dataSource.setMinConnectionsPerPartition(1);
//    dataSource.setMaxConnectionsPerPartition(1);
//    dataSource.setIdleMaxAgeInMinutes(1);
//    dataSource.setIdleConnectionTestPeriodInMinutes(5);
//    jdbcTemplate.setDataSource(dataSource);

//    ucpDataSource.setConnectionPoolName("UCP");
//    ucpDataSource.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
//    ucpDataSource.setMinPoolSize(3);
//    ucpDataSource.setMaxPoolSize(10);
//    ucpDataSource.setInitialPoolSize(1);
//    ucpDataSource.setUser("eDong");
//    ucpDataSource.setPassword("eDong");
//    ucpDataSource.setURL("jdbc:oracle:thin:@10.1.101.112:1521:orcldb");
//    jdbcTemplate.setDataSource(ucpDataSource);
  }

  public static void doTest1() throws SQLException {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
//    int a = jdbcTemplate.queryForInt("select max(id) from cstb_account");
    int a = jdbcTemplate.queryForInt("select max(n_id) from tbl_person");
    System.out.println(a);
  }

  public static void doTest2() throws SQLException {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
//    List<Map> list = jdbcTemplate.executeQuery("select * from CSTB_ACCOUNT where accountid=? ", "0966923885");
    List<Map> list = jdbcTemplate.executeQuery("select * from tbl_person");
    for (Map map : list) {
      System.out.println(map);
    }
  }

  public static void doTest3() throws SQLException {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    Map<String, Object> inParams = new HashMap<String, Object>();
//    inParams.put("p_saccount","0966923885");
//    inParams.put("P_ICHANNELID",0);
//    Map out = jdbcTemplate.callProcedure("core_gateway.cssp_get_accountinfo",inParams);
    inParams.put("pi_b_gender", true);
//    Map out = jdbcTemplate.callProcedure("test.get_person", inParams);
    Map out = jdbcTemplate.callProcedure("test.get_person", inParams);
    System.out.println(out);
  }             //

  public static void doTest4() throws SQLException {
    long start = System.currentTimeMillis();
    try {
      JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
      Map<String, Object> inParams = new HashMap<String, Object>();
      inParams.put("psi_evn_code_list","'PD'");                  //get_customer_toquerycmis            get_bill
      Map out = jdbcTemplate.callProcedure("sync_bill_info.get_bill_list", inParams);
      System.out.println(out);
      long end = System.currentTimeMillis();
      System.out.println((end - start) / 1000 + "s");
    } catch (Exception ex) {
      long end = System.currentTimeMillis();
      System.out.println((end - start) / 1000 + "s");
      System.out.println(ex);
    }
  }

  public static void doTest5() {
    try {
      JdbcTemplate jdbcTemplate = new JdbcTemplate();
      jdbcTemplate.setDataSource(dataSource);
      jdbcTemplate.callProcedure("pkg_einvoice.insert_file_hdon", new HashMap<String, Object>());
    } catch (Exception ex) {
      System.out.println(ex);
    }
  }

  public static void doTest6() {
    try {
      JdbcTemplate jdbcTemplate = new JdbcTemplate();
      jdbcTemplate.setDataSource(dataSource);
      jdbcTemplate.callProcedure("pkg_test.test1", new HashMap<String, Object>());
    } catch (Exception ex) {
      System.out.println(ex);
    }
  }

  public static void doTest7() {
    try {
      JdbcTemplate jdbcTemplate = new JdbcTemplate();
      jdbcTemplate.setDataSource(dataSource);
      jdbcTemplate.callFunction("pkg_test.test2", new HashMap<String, Object>());
    } catch (Exception ex) {
      System.out.println(ex);
    }
  }

  public static void doTest8() {
    try {
      JdbcTemplate jdbcTemplate = new JdbcTemplate();
      jdbcTemplate.setDataSource(dataSource);
      Map<String, Object> in = new HashMap<String, Object>();
      in.put("@param1", 31);
      jdbcTemplate.callProcedure("test1", in);
    } catch (Exception ex) {
      System.out.println(ex);
    }
  }

  public static void doTest9() throws FileNotFoundException {
    JdbcTemplate jdbcTemplate = new JdbcTemplate();
    jdbcTemplate.setDataSource(dataSource);
    Map<String, Object> in = new HashMap<String, Object>();
    File file1 = new File("C:\\Users\\X61\\Pictures\\photo.jpg");
    FileInputStream fis1 = new FileInputStream(file1);
    PassThroughBlob passThroughBlob = new PassThroughBlob(fis1, file1.length());
    in.put("pi_file", passThroughBlob);
    File file2 = new File("C:\\Users\\X61\\Documents\\evn-billview-101-2-soapui-project.xml");
    FileInputStream fis2 = new FileInputStream(file2);
    PassThroughClob clob = new PassThroughClob(fis2, file2.length());
    in.put("pi_message", clob);
    in.put("pi_text", "457");
    in.put("pi_created", new java.util.Date());

    try {
      jdbcTemplate.callProcedure("insert_test", in);
    } catch (SQLException e) {
      System.out.println(e);
    }
  }


  public static void doTest10() throws SQLException, IOException {
    JdbcTemplate jdbcTemplate = new JdbcTemplate();
    jdbcTemplate.setDataSource(dataSource);
    Map<String, Object> in = new HashMap<String, Object>();
    in.put("pi_id", 1);

    Map out = jdbcTemplate.callProcedure("get_test", in);
    PassThroughBlob blob = (PassThroughBlob) out.get("po_file");
    File file = new File("D:\\blob.jpg");
    DataOutputStream dos = new DataOutputStream(
        new BufferedOutputStream(
            new FileOutputStream(file)
        )
    );
    int index;
    while ((index = blob.getBinaryStream().read()) != -1) {
      dos.writeByte(index);
    }
    dos.flush();
    dos.close();
  }

  static JdbcTemplate jdbcTemplate = new JdbcTemplate();

  public static void doTest11() throws SQLException {
    Map<String, Object> in = new HashMap<String, Object>();
//    in.put("trans_id", new Random().nextInt(999999));
//    in.put("trans_date", new Date());
    in.put("pi_names", new PassThroughArray("STRING_ARRAY", new String[]{"1", "2", "3"}));
    in.put("pi_values", new PassThroughArray("STRING_ARRAY", new String[]{"a", "b", "c"}));

    Map out = jdbcTemplate.callProcedure("pkg_edong.test__", in);
    System.out.println(out);
  }

  public static void doTest12() throws SQLException {
    Map out = jdbcTemplate.callFunction("ECPAY_GATEWAY.PKG_INIT.GET_ALL_PARTNER", null);
  }

  public static void main(String[] args) throws SQLException, InterruptedException, IllegalAccessException, IOException {
    init();
    doTest11();
//    ThreadPool pool = new ThreadPool(10,5);
//    for(int i = 0; i < 100; i++){
//      pool.pushJob(new Multi());
//    }
  }

  static class Multi implements Runnable {
    @Override
    public void run() {
      try {
        doTest11();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }
}
