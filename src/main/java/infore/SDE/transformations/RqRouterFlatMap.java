package infore.SDE.transformations;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import infore.SDE.messages.Request;
import org.codehaus.jettison.json.JSONObject;

public class RqRouterFlatMap extends RichFlatMapFunction<Request, Request> implements Serializable{

    private static final long serialVersionUID = 1L;

    //SourceID + Uid (1-1000),Keys(1-1000),
    @Override
    public void flatMap(Request rq, Collector<Request> out) throws Exception {

        //ADD-REMOVE-ESTIMATE SKETCH FOR A STREAM
        if( rq.getRequestID()%10 > 8) {
            return;
        }
        if(rq.getNoOfP() == 1 && rq.getRequestID() != 8)
            out.collect(rq);
        else {
            String tmpkey = rq.getKey();
            if (rq.getRequestID()%10 == 8){
                String[] dataSets = tmpkey.split(",");
                String[] params = rq.getParam();
                if (params.length != 2){
                    System.out.println("Wrong number of arguments");
                    return;
                }
                JSONObject jObj = new JSONObject(params[1]);
                int numOfUIDs = jObj.length();

                if (numOfUIDs == dataSets.length && numOfUIDs > 1){
                    //this is what we want for querying multiple synopses
                    String uIDtoQuery;
                    int noOfP;
                    int totalParall = 0;    //we need to know what is the total parallelism of the request i.e. the sum of Synopses' parallelism

                    Iterator<String> iter2 =  jObj.keys();
                    while (iter2.hasNext()){
                        uIDtoQuery = iter2.next();
                        noOfP = jObj.getInt(uIDtoQuery);
                        if (noOfP < 1){
                            System.out.println("Parallelism can't be lower than 1");
                            return;
                        }
                        totalParall += noOfP;
                    }

                    Iterator<String> iter =  jObj.keys();
                    int ctr = 0;
                    while (iter.hasNext()){
                        uIDtoQuery = iter.next();
                        String datasetKey = dataSets[ctr];    // get the corresponding datasetKey of the uID
                        ctr++;
                        noOfP = jObj.getInt(uIDtoQuery);
                        rq.setUID(Integer.parseInt(uIDtoQuery));
                        rq.setNoOfP(totalParall);
                        String[] newParams = {params[0],params[1],tmpkey};  //add the datasetKey as param in order to used it in ReduceFlatMap
                        rq.setParam(newParams);
                        if (noOfP == 1){
                            rq.setDataSetkey(datasetKey);
                            out.collect(rq);
                        }else {
                            for (int j = 0; j < noOfP; j++) {
                                rq.setDataSetkey(datasetKey + "_" + rq.getNoOfP() + "_KEYED_" + j);
                                out.collect(rq);
                            }
                        }
                    }
                } else if (numOfUIDs == dataSets.length && numOfUIDs == 1) {
                    // this is a request ID 3 actually (estimate single Synopsis)
                    String uIDtoQuery = "";
                    int noOfP;
                    Iterator<String> iter =  jObj.keys();
                    while (iter.hasNext()){ //get the only uID that exists
                        uIDtoQuery = iter.next();
                    }
                    noOfP = jObj.getInt(uIDtoQuery);
                    rq.setUID(3);
                    rq.setNoOfP(noOfP);
                    rq.setUID(Integer.parseInt(uIDtoQuery));
                    String [] newParam = {params[0]};   //keep only the key to query
                    rq.setParam(newParam);
                    if (noOfP == 1){
                        rq.setDataSetkey(tmpkey);
                        out.collect(rq);
                    }else {
                        for (int i = 0; i < noOfP; i++) {
                            rq.setDataSetkey(tmpkey + "_" + rq.getNoOfP() + "_KEYED_" + i);
                            out.collect(rq);
                        }
                    }
                }
            }
            else if(rq.getRequestID()%10 == 6) {
                int n = Integer.parseInt(rq.getParam()[0]);
                String[] dataSets = rq.getDataSetkey().split(",");
                String[] tpr = rq.getParam();
                String[] pr = rq.getParam();
                pr[1]=""+rq.getUID();

                for(int i=0;i< n;i++){

                    tmpkey = dataSets[i];
                    rq.setUID(Integer.parseInt(tpr[i+1]));

                    for(int j = 0; j < rq.getNoOfP(); j ++) {

                        rq.setKey(tmpkey + "_" + rq.getNoOfP() + "_KEYED_" + j);
                        out.collect(rq);

                    }

                }
            }else{
                if(rq.getSynopsisID() == 100){
                    for(int i=0;i<Integer.parseInt(rq.getParam()[2]);i++){
                        rq.setDataSetkey(tmpkey + "_" + i);

                        out.collect(rq);

                    }
                    return;
                }
                for(int i = 0; i < rq.getNoOfP(); i ++) {

                    if(rq.getRequestID()%10 == 4){
                        rq.setKey(tmpkey +"_"+rq.getNoOfP()+"_RANDOM_" + i);
                        out.collect(rq);
                    }else{
                        //System.out.println("here ->" + i );
                        rq.setDataSetkey(tmpkey + "_" + rq.getNoOfP() + "_KEYED_" + i);

                        out.collect(rq);
                        //rq.setKey(tmpkey +"_"+rq.getNoOfP()+"_RANDOM_" + i);
                        //out.collect(rq);
                    }
                }
            }
        }
    }
}
