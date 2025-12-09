package com.xiaoxj.sqlworkflow.dolphinscheduler.workflow;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.Date;
import java.util.List;
import java.util.Map;

/** define workflow response,copied from org.apache.dolphinscheduler.dao.entity.workflowDefinition */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class WrokflowDefineResp {

  /** id */
  private int id;

  /** code */
  private long code;

  /** name */
  private String name;

  /** version */
  private int version;

  /** release state : online/offline */
  private String releaseState;

  /** project code */
  private long projectCode;

  /** description */
  private String description;

  /** user defined parameters */
  private String globalParams;

  /** user defined parameter list */
  private List<Parameter> globalParamList;

  /** user define parameter map */
  private Map<String, String> globalParamMap;

  /** create time */
  @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
  private Date createTime;

  /** update time */
  @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
  private Date updateTime;

  /** workflow is valid: yes/no */
  private String flag;

  /** workflow user id */
  private int userId;

  /** user name */
  private String userName;

  /** project name */
  private String projectName;

  /** locations array for web */
  private String locations;

  /** schedule release state : online/offline */
  private String scheduleReleaseState;

  /** schedule (optional, varies by DS version) */
  private String schedule;

  /** workflow warning time out. unit: minute */
  private int timeout;

  /** tenant id */
  private int tenantId;

  /** tenant code */
  private String tenantCode;

  /** modify user name */
  private String modifyBy;

  /** warningGroupId */
  private int warningGroupId;

  private String executionType;
}
