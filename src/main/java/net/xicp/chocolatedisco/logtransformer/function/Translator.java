package net.xicp.chocolatedisco.logtransformer.function;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import net.xicp.chocolatedisco.logtransformer.ApplicationConfigure;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Created by SunYu on 2018/2/8.
 */
@Slf4j
public class Translator extends BaseFunction {

    private JedisPool jedisPool;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        ApplicationConfigure configure = JSONObject.parseObject((String) conf.get("app-configure"), ApplicationConfigure.class);
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPool = new JedisPool(jedisPoolConfig, configure.getRedis().getIp(), configure.getRedis().getPort());
    }

    @Override
    public void cleanup() {
        jedisPool.close();
        super.cleanup();
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        tuple.stream().findFirst().ifPresent(extracted -> {
            try {
                Jedis jedis = jedisPool.getResource();
                List<JSONObject> values = JSONArray.parseArray(
                        jedis.hget(
                                Optional.ofNullable(((JSONObject) extracted).getString("type")).orElseThrow(() -> new Exception("can not find [type]")),
                                "values"),
                        JSONObject.class);
                values.stream().forEach(value -> {
                    String field = value.getString("field");
                    String raw = value.getString("raw");
                    String store = value.getString("store");
                    String real = ((JSONObject) extracted).getString(field);
                    if (real != null && real.equals(raw)) {
                        ((JSONObject) extracted).put(field, store);
                    }
                    collector.emit(new Values(extracted));
                });
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        });
    }
}
