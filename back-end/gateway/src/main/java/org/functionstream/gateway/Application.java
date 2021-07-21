package org.functionstream.gateway;

import io.vertx.core.Launcher;
import io.vertx.core.json.JsonObject;

/**
 * @author HALOXIAO
 * @since 2021-07-18 10:47
 **/
public class Application extends Launcher {



    /**
     * 获得默认的集群配置文件
     *
     * @return
     */
    public JsonObject getDefaultClusterConfig() {
        JsonObject json = new JsonObject();
        json.put("zookeeperHosts", "127.0.0.1");
        json.put("sessionTimeout", 20000);
        json.put("connectTimeout", 3000);
        json.put("rootPath", "io.vertx");
        json.put("vxApiConfPath", "/io.vertx/vx.api.gateway/conf");
        JsonObject retry = new JsonObject();
        retry.put("initialSleepTime", 100);
        retry.put("intervalTimes", 10000);
        retry.put("maxTimes", 5);
        json.put("retry", retry);
        return json;

    }




}
