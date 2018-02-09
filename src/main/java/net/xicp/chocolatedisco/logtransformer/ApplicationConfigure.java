package net.xicp.chocolatedisco.logtransformer;

import lombok.Data;

import java.io.Serializable;

/**
 * Created by SunYu on 2018/2/8.
 */
@Data
public class ApplicationConfigure implements Serializable {
    private RedisConfigure redis;
    private String model;

    @Data
    public static class RedisConfigure implements Serializable {
        private String ip;
        private Integer port;
    }
}
