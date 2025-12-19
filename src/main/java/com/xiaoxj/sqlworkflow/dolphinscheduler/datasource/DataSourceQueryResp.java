package com.xiaoxj.sqlworkflow.dolphinscheduler.datasource;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import java.util.Date;

/** query datasource info response,copied from org.apache.dolphinscheduler.dao.entity.DataSource */
@Data
public class DataSourceQueryResp {

  private Long id;

  /** user id */
  private int userId;

  /** user name */
  private String userName;

  /** data source name */
  private String name;

  /** note */
  private String note;

  /** data source type */
  private String type;

  /** connection parameters */
  private String connectionParams;

  /** create time */
  @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
  private Date createTime;

  /** update time */
  @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
  private Date updateTime;
}
