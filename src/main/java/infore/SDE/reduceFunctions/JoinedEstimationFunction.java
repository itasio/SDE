package infore.SDE.reduceFunctions;

import infore.SDE.messages.Estimation;
import infore.SDE.synopses.Synopsis;

public class JoinedEstimationFunction extends ReduceFunction{

    private Synopsis sketch;

    public JoinedEstimationFunction(int noOfP, int count, String[] param, int synopsisID, int requestID) {
        super(noOfP,count, param, synopsisID, requestID);
    }

    @Override
    public Object reduce() {
        String keyToQuery = this.parameters[0];
        return this.sketch.estimate(keyToQuery);
    }

    @Override
    public boolean add(Estimation e) {
        count++;
        if (sketch == null){
            sketch = (Synopsis) e.getEstimation();
        }else {
            sketch = sketch.merge((Synopsis) e.getEstimation());
        }
        return count == nOfP;
    }
}
