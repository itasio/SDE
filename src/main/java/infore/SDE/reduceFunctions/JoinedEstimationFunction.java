package infore.SDE.reduceFunctions;

import infore.SDE.messages.Estimation;
import infore.SDE.synopses.Synopsis;

public class JoinedEstimationFunction extends ReduceFunction{

    private Synopsis sketch;

    public JoinedEstimationFunction(int noOfP, int i, String[] param, int synopsisID, int requestID) {
        super(noOfP,i, param, synopsisID, requestID);
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
