package net.xicp.chocolatedisco.logtransformer.state.elastic;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import net.xicp.chocolatedisco.logtransformer.ApplicationConfigure;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.storm.Config;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.tuple.TridentTuple;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by SunYu on 2018/2/26.
 */
@Slf4j
public class ElasticState implements State {

    private RestHighLevelClient client;
    private String index;
    private String type;


    public ElasticState(ApplicationConfigure configure) {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        JedisPool jedisPool = new JedisPool(jedisPoolConfig, configure.getRedis().getIp(), configure.getRedis().getPort());
        Jedis jedis = jedisPool.getResource();
        String esNodes = jedis.hget("elastic", "nodes");
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(jedis.hget("elastic", "user"), jedis.hget("elastic", "password")));
        this.client = new RestHighLevelClient(
                RestClient.builder(
                        Arrays.asList(esNodes.split(",")).stream().map(es -> HttpHost.create(es)).toArray(HttpHost[]::new)
                ).setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)));
        this.index = jedis.hget("elastic", "index");
        this.type = jedis.hget("elastic", "type");
    }

    @Override
    public void beginCommit(Long txid) {

    }

    @Override
    public void commit(Long txid) {

    }

    public void updataState(List<TridentTuple> tuples) {
        BulkRequest request = new BulkRequest();
        tuples.stream().forEach(tuple -> {
            JSONObject log = (JSONObject) tuple.get(0);
            request.add(new IndexRequest(index, type).source(log.toJSONString(), XContentType.JSON));
        });
        try {
            BulkResponse response = client.bulk(request);
            System.err.println(response.hasFailures());
        } catch (IOException e) {
            log.error("es索引日志失败", e);
        }
    }

    public static class ElasticStateFactory implements StateFactory {

        private Config config;

        public ElasticStateFactory(Config config) {
            this.config = config;
        }

        public State makeState(Map map, IMetricsContext iMetricsContext, int i, int i1) {
            ApplicationConfigure configure = JSONObject.parseObject((String) config.get("app-configure"), ApplicationConfigure.class);
            ElasticState esState = new ElasticState(configure);
            return esState;
        }
    }
}
