package infore.SDE.synopses;

import com.clearspring.analytics.stream.membership.BloomFilter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

import java.io.IOException;

public class Bloomfilter extends Synopsis{
    private BloomFilter bm;

    public Bloomfilter(int uid, String[] parameters) {
        super(uid,parameters[0],parameters[1],parameters[2]);
        bm = new BloomFilter( Integer.parseInt(parameters[3]), Double.parseDouble(parameters[4]));

    }

    private Bloomfilter(Bloomfilter bmSketch){
        super(bmSketch.SynopsisID, bmSketch.keyIndex, bmSketch.valueIndex, bmSketch.operationMode);
        bm = bmSketch.bm;
    }

    @Override
    public void add(Object k) {

        //ObjectMapper mapper = new ObjectMapper();
        JsonNode node = (JsonNode)k;
        /*try {
            node = mapper.readTree(j);
        } catch (IOException e) {
            e.printStackTrace();
        } */
        String key = node.get(this.keyIndex).asText();
        bm.add(key);

    }

    @Override
    public String estimate(Object k) {
        if(bm.isPresent((Double.toString((double)k))))
            return "1";
        return "0";

    }

    public Estimation estimate(Request rq) {
        if (rq.getRequestID() % 10 == 8){
            return new Estimation(rq,this,rq.getParam()[2]);	//param[2] is the original datasetKey of the request (needed in ReduceFlatMap)
        }
        return new Estimation(rq, bm.isPresent(rq.getParam()[0]), Integer.toString(rq.getUID()));
    }



    @Override
    public Synopsis merge(Synopsis sk) {
        Bloomfilter mergedSyn = new Bloomfilter(this);
        mergedSyn.bm = (BloomFilter) mergedSyn.bm.merge(((Bloomfilter) sk).bm);
        if (mergedSyn.bm == null){
            throw new IllegalArgumentException("Synopsis given for merge cannot be null");
        }
        return mergedSyn;
    }


}
