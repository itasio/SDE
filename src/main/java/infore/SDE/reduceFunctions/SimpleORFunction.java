package infore.SDE.reduceFunctions;

import infore.SDE.messages.Estimation;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

public class SimpleORFunction extends ReduceFunction {

   private ArrayList<Object> estimations;

	public SimpleORFunction(int nOfP, int count, String[] parameters, int syn, int rq) {
		super(nOfP, count, parameters, syn, rq);
		estimations = new ArrayList<>();
		// TODO Auto-generated constructor stub
	}

	@Override
	public Object reduce() {
		boolean or = false;
		
		for (Object entry : estimations) {
			if (entry instanceof String)
				if (entry.equals("0"))
					entry = false;
				else if(entry.equals("1"))
					entry = true;
			or = or || (boolean)entry;
		}
		return or;
	}


	@Override
	public boolean add(Estimation e) {
		String[] par = e.getParam();
		if (par[par.length - 1].equals("spatial")){
			@SuppressWarnings("unchecked")
			ArrayList<Tuple2<Object, Float>> arr = (ArrayList<Tuple2<Object, Float>>) e.getEstimation();
			for (Tuple2<Object, Float> est_cover : arr){
				estimations.add(est_cover.f0);	//account for the estimation as is, no matter the coverage of the sketch
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
