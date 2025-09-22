package infore.SDE.transformations;

import com.SpatialDatapoint.avro.SpatialDatapoint;
import infore.SDE.messages.Datapoint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.codehaus.jettison.json.JSONObject;

public class AvroMultiplierFlatMap implements FlatMapFunction<SpatialDatapoint, Datapoint> {
    private final int multiplier;

    public AvroMultiplierFlatMap(int multiplier) {
        this.multiplier = multiplier;
    }

    @Override
    public void flatMap(SpatialDatapoint sdp, Collector<Datapoint> collector) throws Exception {
        String dataSetKey = sdp.getDataSetkey().toString();
        String streamID = sdp.getStreamID().toString();
        int key = sdp.getKey();
        int xCell = sdp.getXCell();
        int yCell = sdp.getYCell();
        int value = sdp.getValue();

        JSONObject jObjData = new JSONObject();
        jObjData.put("x_pos", xCell);
        jObjData.put("y_pos", yCell);
        jObjData.put("value", value);

        JSONObject jObj = new JSONObject();
        jObj.put(streamID,key);
        jObj.put("dataParam",jObjData);
        String jsonStringSpatial = jObj.toString();

        Datapoint dp = new Datapoint(dataSetKey, String.valueOf(key), jsonStringSpatial);
        for (int i = 0; i < multiplier; i++) {
            collector.collect(dp);
        }
    }
}
