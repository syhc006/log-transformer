package net.xicp.chocolatedisco.logtransformer.function;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * Created by SunYu on 2018/2/9.
 */
@Slf4j
public class Printer extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        log.debug(JSON.toJSONString(tuple.get(0)));
        System.out.println(JSON.toJSONString(tuple.get(0)));
    }
}
