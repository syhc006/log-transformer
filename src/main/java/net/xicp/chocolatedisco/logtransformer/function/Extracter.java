package net.xicp.chocolatedisco.logtransformer.function;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.thekraken.grok.api.Grok;
import io.thekraken.grok.api.Match;
import io.thekraken.grok.api.exception.GrokException;
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

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Created by SunYu on 2018/2/8.
 */
@Slf4j
public class Extracter extends BaseFunction {

    private JedisPool jedisPool;
    private Map grokPatterns;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        ApplicationConfigure configure = JSONObject.parseObject((String) conf.get("app-configure"), ApplicationConfigure.class);
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPool = new JedisPool(jedisPoolConfig, configure.getRedis().getIp(), configure.getRedis().getPort());
        grokPatterns = (Map) conf.get("grok-pattern");
    }

    @Override
    public void cleanup() {
        jedisPool.close();
        super.cleanup();
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Jedis jedis = jedisPool.getResource();
        try {
            JSONObject json = JSONObject.parseObject((String) tuple.get(0));
            List<String> templates = JSONArray.parseArray(
                    jedis.hget(
                            Optional.ofNullable(json.getString("type")).orElseThrow(() -> new Exception("can not find [type]")),
                            "templates"),
                    String.class);
            templates.stream().anyMatch(template -> {
                Grok grok = new Grok();
                try {
                    grok.copyPatterns(grokPatterns);
                    grok.compile(template);
                    Match gm = grok.match(json.getString("message"));
                    gm.captures();
                    if (gm.toMap().size() != 0) {
                        JSONObject extracted = new JSONObject(gm.toMap());
                        extracted.put("type", json.getString("type"));
                        Optional.ofNullable((String) json.get("@timestamp"))
                                .ifPresent(time -> {
                                    Instant instant = Instant.parse(time);
                                    ZoneId china = ZoneId.of("+08:00");
                                    ZonedDateTime dateAndTimeInChina = ZonedDateTime.ofInstant(instant, china);
                                    extracted.put("generateTime", dateAndTimeInChina.toInstant().getEpochSecond() * 1000);
                                });
                        collector.emit(new Values(extracted));
                        return true;
                    } else {
                        return false;
                    }
                } catch (GrokException e) {
                    log.error(e.getMessage(), e);
                    return false;
                }
            });
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }
}
