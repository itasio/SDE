package infore.SDE.reduceFunctions;


import infore.SDE.messages.Estimation;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Vector;

public class SimpleSumFunction extends ReduceFunction {

	private ArrayList<Object> estimations;

	public SimpleSumFunction(int nOfP, int count, String[] parameters, int syn, int rq) {
		super(nOfP, count, parameters, syn, rq);
		estimations = new ArrayList<>();
		// TODO Auto-generated constructor stub
	}

	@Override
	public Object reduce() {
		double sum = 0;
		
		for (Object entry : estimations) {
			sum = sum + Double.parseDouble((String)entry);
		}

		
		return sum;
	}
	@Override
	public boolean add(Estimation e) {

		String[] par = e.getParam();
		if (par[par.length - 1].equals("spatial")){
			@SuppressWarnings("unchecked")
			Vector<Tuple2<Object, Float>> vector = (Vector<Tuple2<Object, Float>>) e.getEstimation();
			for (Tuple2<Object, Float> est_cover : vector){
				String est = (String) est_cover.f0;
				int weightedEst = (int) (Double.parseDouble(est) * est_cover.f1);

				estimations.add(Double.toString(weightedEst));
			}
		}else {
			estimations.add(e.getEstimation());
		}
		count++;
		if(count == nOfP) {
			return true;
		}
		return false;
	}


}
