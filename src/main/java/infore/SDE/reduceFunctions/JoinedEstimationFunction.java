package infore.SDE.reduceFunctions;

import infore.SDE.messages.Estimation;
import infore.SDE.synopses.Synopsis;

public class JoinedEstimationFunction extends ReduceFunction{

    /** The first sketch that arrives from a request*/
    public Synopsis firstSketch;

    /** The remaining sketches that arrive from a request. They will be merged with the {@link JoinedEstimationFunction#firstSketch} when all of them are gathered. */
    private Synopsis[] remainingSketches;

    /** The uid of the estimate multiple synopses request. Many requests might originate from this request.
     * We want to be sure that all synopses gathered for merging are from the same Request. */
    private String origin_req_uid;

    public JoinedEstimationFunction(int noOfP, int count, String[] param, int synopsisID, int requestID) {
        super(noOfP,count, param, synopsisID, requestID);
        remainingSketches = new Synopsis[noOfP-1];  //minus 1 because the first sketch that arrives will not belong in the array
    }

    @Override
    public Object reduce() {
        Synopsis mergedSyn = firstSketch.merge(remainingSketches);
        String keyToQuery = this.parameters[0];
        return mergedSyn.estimate(keyToQuery);
    }

    @Override
    public boolean add(Estimation e) {
        String uid = e.getParam()[3];   //uid of the Request that this request and therefore synopsis came from
        if (firstSketch == null) {
            firstSketch = (Synopsis) e.getEstimation();
            origin_req_uid = uid;
        }
        else {
            if (!uid.equals(origin_req_uid)){
                 /*we want to make sure that synopses are gathered for merge-and-estimation after a single estimate multiple request.
                 Each request has a different uid, hence all synopses gathered must have the same origin request uid
                 This is done to tackle cases when the uid of a requested synopsis doesn't exist.
                 A sketch here has arrived due to another estimate multiple Request. This sketch shouldn't be merged with those already gathered.
                 This happened due to wrong request parameters in the already gathered sketches (i.e. the former request).
                 Begin gathering synopses for the new-coming request, reset the ReduceFunction*/

                System.out.println("The request with uID: "+ origin_req_uid + " and dataset key: " + e.getEstimationkey() +
                        " had wrong parameters. \nWas each dataset key assigned to the correct uID ? \n" +
                        "Started estimating multiple synopses for the request with uID: "+uid);
                origin_req_uid = uid;
                firstSketch = (Synopsis) e.getEstimation();
                count = 0;  // reset count
                this.setnOfP(e.getNoOfP());
                this.setParameters(e.getParam());
                this.setSynopsisID(e.getSynopsisID());
                this.requestID = e.getRequestID();
                this.remainingSketches = new Synopsis[nOfP-1];

//                throw new RuntimeException("An error occurred. One or more uid don't correspond to the requested dataset keys. Check again the request parameters.");
            }else {
                if (count-1 >= remainingSketches.length) {
                    throw new RuntimeException("Too many sketches added for merging. Check again the parameters of your request.");
                }
                remainingSketches[count - 1] = (Synopsis) e.getEstimation();  // minus 1 to account for the first sketch that does not belong to the array
            }
        }
        count++;
        return count == nOfP;
    }
}
