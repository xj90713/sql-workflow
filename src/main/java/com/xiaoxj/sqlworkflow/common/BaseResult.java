package com.xiaoxj.sqlworkflow.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

/**
 * 服务结果封装类
 *
 * @param <T>
 * @author luoxuetong
 * @date 2021年5月14日
 * @since 1.0.0
 */
@Data
public class BaseResult<T> {

    /**
     * 响应码
     */
    private String code;

    /**
     * 响应信息
     */
    private String message;

    /**
     * 业务对象是否加密：0-否，1-是，默认为0
     */
    private String encrypted = "0";

    /**
     * 业务对象
     */
    private T data;

    /**
     * 返回结果对象
     *
     * @param code    响应码
     * @param message 响应信息
     * @return 结果对象
     */
    public static <T> BaseResult<T> result(String code, String message) {
        return new BaseResult<>(code, message);
    }

    /**
     * 返回结果对象
     *
     * @param code    响应码
     * @param message 响应信息
     * @param data    业务对象
     * @param <T>     业务对象类型
     * @return
     */
    public static <T> BaseResult<T> result(String code, String message, T data) {
        return new BaseResult<>(code, message, data);
    }

    /**
     * 成功时的结果对象
     *
     * @param <T> 需要返回的业务对象类型
     * @return 结果对象
     */
    public static <T> BaseResult<T> success() {
        return new BaseResult<>(ResultStatus.SUCCESS.getCode(), ResultStatus.SUCCESS.getMessage());
    }

    /**
     * 成功时的结果对象
     *
     * @param <T>  需要返回的业务对象类型
     * @param data 需要返回的业务对象
     * @return 结果对象
     */
    public static <T> BaseResult<T> success(T data) {
        return new BaseResult<>(ResultStatus.SUCCESS.getCode(), ResultStatus.SUCCESS.getMessage(), data);
    }

    /**
     * 系统异常时的结果对象
     *
     * @param <T>  需要返回的业务对象类型
     * @param data 需要返回的业务对象
     * @return 结果对象
     */
    public static <T> BaseResult<T> systemError(T data) {
        return new BaseResult<>(ResultStatus.SYSTEM_ERROR.getCode(), ResultStatus.SYSTEM_ERROR.getMessage(), data);
    }

    /**
     * 参数错误时的结果对象
     *
     * @param <T>  需要返回的业务对象类型
     * @param data 需要返回的业务对象
     * @return 结果对象
     */
    public static <T> BaseResult<T> paramError(T data) {
        return new BaseResult<>(ResultStatus.PARAM_ERROR.getCode(), ResultStatus.PARAM_ERROR.getMessage(), data);
    }

    /**
     * 构造方法，默认初始化为成功状态
     */
    public BaseResult() {

    }

    /**
     * 构造方法
     *
     * @param code    响应码
     * @param message 响应信息
     */
    public BaseResult(String code, String message) {
        this(code, message, null);
    }

    /**
     * 构造方法
     *
     * @param code    响应码
     * @param message 响应信息
     * @param data    结果对象
     */
    public BaseResult(String code, String message, T data) {
        this.code = code;
        this.data = data;
        this.message = message;
    }

    /**
     * 判断是否成功。@JsonIgnore注解表示使用Jackson时不对该方法进行处理，否则转换为Json时会带上："successful":true
     *
     * @return 处理结果
     */
    @JsonIgnore
    public boolean isSuccessful() {
        return ResultStatus.SUCCESS.getCode().equals(code);
    }
}