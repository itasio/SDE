package infore.SDE.ReduceFunctions;

public class SimpleORFunction extends ReduceFunction {



	public SimpleORFunction(int nOfP, int count, String[] parameters, int syn) {
		super(nOfP, count, parameters, syn);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Object reduce() {
		boolean or = false;
		
		for (Object entry : this.getEstimations()) {
			or = or || (boolean)entry;
		}
		return or;
	}

}
