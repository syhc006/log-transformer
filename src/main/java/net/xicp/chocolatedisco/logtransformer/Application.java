package net.xicp.chocolatedisco.logtransformer;

import com.alibaba.fastjson.JSONObject;
import io.thekraken.grok.api.Grok;
import io.thekraken.grok.api.exception.GrokException;
import kafka.api.OffsetRequest;
import net.xicp.chocolatedisco.logtransformer.function.Extracter;
import net.xicp.chocolatedisco.logtransformer.function.Printer;
import net.xicp.chocolatedisco.logtransformer.function.Translator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.shade.org.yaml.snakeyaml.Yaml;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;
import redis.clients.jedis.Jedis;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by SunYu on 2018/2/8.
 */
public class Application implements Serializable {

    public static void main(String[] args) throws GrokException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        //生成并配置拓扑
        TridentTopology topology = new TridentTopology();
        Config config = new Config();
        config.setNumWorkers(2);
        config.setDebug(false);
        ApplicationConfigure applicationConfigure = getApplicationConfigure();
        config.put("app-configure", JSONObject.toJSONString(applicationConfigure));
        config.put("grok-pattern", getGrokPatterns());
        Jedis jedis = new Jedis(applicationConfigure.getRedis().getIp(), applicationConfigure.getRedis().getPort());
        OpaqueTridentKafkaSpout kafkaSpout = getOpaqueTridentKafkaSpout(jedis);
        topology.newStream("log-transformer", kafkaSpout)
                .each(new Fields(StringScheme.STRING_SCHEME_KEY), new Extracter(), new Fields("extracted"))
                .each(new Fields("extracted"), new Translator(), new Fields("translated"))
                .each(new Fields("translated"), new Printer(), new Fields(""));
        //提交拓扑
        if (applicationConfigure.getModel().equals("local")) {
            //本地模式
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("123", config, topology.build());
        } else {
            //集群模式
            StormSubmitter.submitTopologyWithProgressBar("log-transformer-topology", config, topology.build());
        }

    }

    public static ApplicationConfigure getApplicationConfigure() {
        ClassLoader classLoader = Application.class.getClassLoader();
        return new Yaml().loadAs(classLoader.getResourceAsStream("application.yml"), ApplicationConfigure.class);
    }

    public static Map getGrokPatterns() throws GrokException {
        ClassLoader classLoader = Application.class.getClassLoader();
        Grok grok = Grok.create(classLoader.getResource("grokpatterns").getPath());
        return grok.getPatterns();
    }

    public static OpaqueTridentKafkaSpout getOpaqueTridentKafkaSpout(Jedis jedis) {
        BrokerHosts brokerHosts = new ZkHosts(jedis.hget("zookeeper", "brokers"));
        TridentKafkaConfig spoutConfig = new TridentKafkaConfig(brokerHosts, jedis.hget("zookeeper", "topic"));
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.ignoreZkOffsets = false;
        spoutConfig.startOffsetTime = OffsetRequest.LatestTime();
        return new OpaqueTridentKafkaSpout(spoutConfig);
    }
}
