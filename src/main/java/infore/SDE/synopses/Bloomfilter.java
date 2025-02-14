package infore.SDE.synopses;

import com.clearspring.analytics.stream.membership.BloomFilter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;


import java.io.IOException;

public class Bloomfilter extends Synopsis{
    private BloomFilter bm;
    private final int numOFElem;
    private final double prob;
    public Bloomfilter(int uid, String[] parameters) {
        super(uid,parameters[0],parameters[1],parameters[2]);
        bm = new BloomFilter( Integer.parseInt(parameters[3]), Double.parseDouble(parameters[4]));
        numOFElem = Integer.parseInt(parameters[3]);
        prob = Double.parseDouble(parameters[4]);

    }

    private Bloomfilter(Bloomfilter bmSketch){
        super(bmSketch.SynopsisID, bmSketch.keyIndex, bmSketch.valueIndex, bmSketch.operationMode);
        bm = bmSketch.bm;
        numOFElem = bmSketch.numOFElem;
        prob = bmSketch.prob;
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
        boolean result;
        if (k instanceof String){
            result = bm.isPresent((String) k);
        } else if (k instanceof Number) {
            result = bm.isPresent(k.toString());
        }else {
            try {
                result = bm.isPresent((Double.toString((double)k)));
            }catch (ClassCastException e){
                throw new IllegalArgumentException("Parameter " + k + " couldn't be converted to double ",e);
            }catch (Exception e){
                throw new RuntimeException("An unexpected error occurred", e);
            }
        }
        if(result)
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
    public Synopsis merge(Synopsis... sk) {
        if (sk == null)
            throw new IllegalArgumentException("Synopses specified for merging cannot be null");
        if (sk.length == 0)
            return this;
        Bloomfilter mergedSyn = new Bloomfilter(this);          // create a "copy" of this Bloomfilter
        mergedSyn.bm = new BloomFilter(mergedSyn.numOFElem, mergedSyn.prob);    //create an empty bm with the same parameters, that its BitSet has surely sizeIsSticky == true

        BloomFilter[] bms = new BloomFilter[sk.length+1];       //list of filters for merging

        try {
            int i;
            for (i = 0; i < sk.length; i++) {
                bms[i] = ((Bloomfilter)sk[i]).bm;   //add parameter sketches to the list
            }
            bms[i] =  this.bm;   //add the calling sketch to the list as the last sketch

            mergedSyn.bm = (BloomFilter) mergedSyn.bm.merge(bms);
        } catch (ClassCastException e){
            throw new IllegalArgumentException("Synopses must be of the same kind to be merged");
        }
        return mergedSyn;
    }


}
