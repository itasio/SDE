package infore.SDE.transformations;

import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Datapoint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class StringMultiplierFlatMap implements FlatMapFunction<String, Datapoint> {

    int multiplier;

    public StringMultiplierFlatMap(int multiplier){
        this.multiplier = multiplier;
    }
    @Override
    public void flatMap(String node, Collector<Datapoint> out) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        Datapoint dp = objectMapper.readValue(node, Datapoint.class);
        for (int i = 0; i < multiplier; i++) {
            out.collect(dp);
        }
    }
}
