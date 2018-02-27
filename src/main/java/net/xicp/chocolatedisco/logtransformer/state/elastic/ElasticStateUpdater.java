package net.xicp.chocolatedisco.logtransformer.state.elastic;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;

/**
 * Created by SunYu on 2018/2/26.
 */
public class ElasticStateUpdater extends BaseStateUpdater<ElasticState> {
    @Override
    public void updateState(ElasticState state, List<TridentTuple> tuples, TridentCollector collector) {
        state.updataState(tuples);
    }
}
