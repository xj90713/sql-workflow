package com.xiaoxj.sqlworkflow.task;


import com.xiaoxj.sqlworkflow.workflow.Parameter;
import com.xiaoxj.sqlworkflow.util.JacksonUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Collections;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
@Accessors(chain = true)
public class HttpTask extends AbstractTask {

  private List<Parameter> localParams = Collections.emptyList();

  /** http request param */
  private List<HttpParam> httpParams = Collections.emptyList();

  /** http request url */
  private String url;

  private String httpMethod;

  private String httpCheckCondition; // STATUS_CODE_DEFAULT
  private String condition;
  private Integer connectTimeout = 60000;
  private Integer socketTimeout = 60000;

  @Override
  public String getTaskType() {
    return "HTTP";
  }

  @Data
  @Accessors(chain = true)
  @AllArgsConstructor
  @NoArgsConstructor
  public static class HttpParam {
    private String prop;
    private String value;
    private String httpParametersType;

    /** create http form param instance */
    public static HttpParam newForm() {
      return new HttpParam(null, null, "PARAMETER");
    }

    /** create http headers param instance */
    public static HttpParam newHeader() {
      return new HttpParam(null, null, "HEADERS");
    }

    /** create http body param instance */
    public static HttpParam newBody() {
      return new HttpParam(null, null, "BODY");
    }

    /**
     *
     * @return object json string
     */
    @Override
    public String toString() {
      return JacksonUtils.toJSONString(this);
    }
  }
}
