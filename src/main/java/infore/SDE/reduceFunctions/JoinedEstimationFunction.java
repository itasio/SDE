package infore.SDE.reduceFunctions;

import infore.SDE.messages.Estimation;
import infore.SDE.synopses.Synopsis;

public class JoinedEstimationFunction extends ReduceFunction{

    /** The first sketch that arrives from a request*/
    public Synopsis firstSketch;

    /** The remaining sketches that arrive from a request. They will be merged with the {@link JoinedEstimationFunction#firstSketch} when all of them are gathered. */
    private final Synopsis[] remainingSketches;
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
        if (firstSketch == null)
            firstSketch = (Synopsis) e.getEstimation();
        else
            remainingSketches[count-1] = (Synopsis) e.getEstimation();  // minus 1 to account for the first sketch that does not belong to the array
        count++;
        return count == nOfP;
    }
}
