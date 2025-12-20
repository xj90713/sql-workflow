package com.xiaoxj.sqlworkflow.common;

public class ResultStatus {

    /**
     * 成功时对应的状态码常量
     */
    public static final ResultStatus SUCCESS = new ResultStatus("000000", "成功");

    /**
     * 系统异常对应的状态码常量
     */
    public static final ResultStatus SYSTEM_ERROR = new ResultStatus("999999", "系统异常");

    /**
     * 参数错误对应的状态码常量
     */
    public static final ResultStatus PARAM_ERROR = new ResultStatus("999998", "参数错误");

    /**
     * 参数错误对应的状态码常量
     */
    public static final ResultStatus NETWORK_ERROR = new ResultStatus("999997", "网络异常");

    /**
     * 状态编码
     */
    private String code;

    /**
     * 对应默认状态信息
     */
    private String message;

    public ResultStatus(String code, String message) {
        this.code = code;
        this.message = message;
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

}