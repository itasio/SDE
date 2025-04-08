package infore.SDE.reduceFunctions;


import infore.SDE.messages.Estimation;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

public class SimpleMaxFunction extends ReduceFunction{

	private ArrayList<Object> estimations;

	public SimpleMaxFunction(int nOfP, int count, String[] parameters, int Syn, int rq) {
		super(nOfP, count, parameters,Syn, rq);
		estimations = new ArrayList<>();
		// TODO Auto-generated constructor stub
	}

	@Override
	public Object reduce() {
		long max = 0;
		
		for ( Object entry : estimations) {
		  
			if(max <(long)entry)
			   max = (long)entry;
		}

		
		return max;
	}

	@Override
	public boolean add(Estimation e) {
		String[] par = e.getParam();
		if (par[par.length - 1].equals("spatial")){
			@SuppressWarnings("unchecked")
			ArrayList<Tuple2<Object, Float>> arr = (ArrayList<Tuple2<Object, Float>>) e.getEstimation();
			for (Tuple2<Object, Float> est_cover : arr){
				long weightedEst;
				long est;
				if (est_cover.f0 == null){
					est = 0;
				} else if (est_cover.f0 instanceof String) {
					est = Long.parseLong((String) est_cover.f0);
				} else{
					est = ((Number) est_cover.f0).longValue();
				}
				weightedEst = (long) (est * est_cover.f1);
				estimations.add(weightedEst);
			}
		} else {
			estimations.add(e.getEstimation());
		}
		count++;
		if(count == nOfP) {
			return true;
		}
		return false;
	}

}
