package com.xiaoxj.sqlworkflow.datasource;


import com.xiaoxj.sqlworkflow.BaseTest;
import com.xiaoxj.sqlworkflow.dolphinscheduler.datasource.DataSourceQueryResp;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataSourceTest extends BaseTest {

  /** create datasource */

  /** list all datasource */
  @Test
  public void listDataSource() {
    System.out.println(getClient().opsForDataSource().list("采集元数据库-oracle"));
  }

  @Test
  public void getDataSource() {
    Integer id = getClient().opsForDataSource().getDatasource("采集元数据库-oracle").getId();
    System.out.println( id);
    System.out.println(getClient().opsForDataSource().getDatasource("采集元数据库-oracle"));
  }

  /** update datasource */

}
