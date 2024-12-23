package infore.SDE.reduceFunctions;

import infore.SDE.messages.Estimation;
import infore.SDE.synopses.Synopsis;

public class JoinedEstimationFunction extends ReduceFunction{

    private final Synopsis sketch;

    public JoinedEstimationFunction(int noOfP, int i, String[] param, int synopsisID, int requestID, Object estimation) {
        super(noOfP,i, param, synopsisID, requestID);
        this.sketch = (Synopsis) estimation;
    }

    @Override
    public Object reduce() {
        String keyToQuery = this.parameters[0];
        return this.sketch.estimate(keyToQuery);
    }

    @Override
    public boolean add(Estimation e) {
        this.sketch.merge((Synopsis) e.getEstimation());
        return false;
    }
}
