package infore.SDE.synopses;



import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.synopses.Sketches.CM;

public class CountMin extends Synopsis{

	private CM cm;
	int count = 0;

	public CountMin(int uid, String[] parameters) {
     super(uid,parameters[0],parameters[1], parameters[2]);
	 cm = new CM(Double.parseDouble(parameters[3]),Double.parseDouble(parameters[4]),Integer.parseInt(parameters[5]));
	}

	private CountMin(CountMin cmSketch){
		super(cmSketch.SynopsisID, cmSketch.keyIndex, cmSketch.valueIndex, cmSketch.operationMode);
		cm = cmSketch.cm;
		count = cmSketch.count;
	}

	@Override
	public void add(Object k) {
		//String j = (String)k;
		count++;
		//ObjectMapper mapper = new ObjectMapper();
		JsonNode node = (JsonNode)k;
        /*try {
            node = mapper.readTree(j);
        } catch (IOException e) {
            e.printStackTrace();
        } */

		String key = node.get(this.keyIndex).asText();

		if(this.valueIndex.startsWith("null")){

			cm.add(key, 1);
		}else{
			String value = node.get(this.valueIndex).asText();
			//cm.add(Math.abs((key).hashCode()), (long)Double.parseDouble(value));
			cm.add(key, (long)Double.parseDouble(value));
		}

	}

	@SuppressWarnings("deprecation")
	@Override
	public Object estimate(Object k)
	{
		if (k instanceof String){
			return  Long.toString(cm.estimateCount((String) k));
		} else if (k instanceof Number) {
			return Long.toString(cm.estimateCount(((Number) k).longValue()));
		}else {
			try {
				return Long.toString(cm.estimateCount((long) k));
			}catch (ClassCastException e){
				throw new IllegalArgumentException("Parameter " + k + " couldn't be converted to long ",e);
			}catch (Exception e){
				throw new RuntimeException("An unexpected error occurred", e);
			}
		}
	}

	@Override
	public Synopsis merge(Synopsis sk) {
		CountMin mergedSyn = new CountMin(this);
//		mergedSyn.cm.merge(((CountMin) sk).cm);
		mergedSyn.cm = CM.static_merge(this.cm, ((CountMin) sk).cm);
		mergedSyn.count += ((CountMin) sk).count;

		return mergedSyn;
	}

	@Override
	public Estimation estimate(Request rq) {

		if(rq.getRequestID() % 10 == 6){

			String[] par = rq.getParam();
			par[2]= ""+rq.getUID();
			rq.setUID(Integer.parseInt(par[1]));
			rq.setParam(par);
			rq.setNoOfP(rq.getNoOfP()*Integer.parseInt(par[0]));
			return new Estimation(rq, cm, par[1]);

		} else if (rq.getRequestID() % 10 == 8) {
//			return new Estimation(rq,cm,rq.getKey());
			return new Estimation(rq,this,rq.getParam()[2]);	//param[2] is the original datasetKey of the request (needed in ReduceFlatMap)
		}
		String key = rq.getParam()[0];
		String e = Double.toString((double)cm.estimateCount(key));
		return new Estimation(rq, e, Integer.toString(rq.getUID()));


	}
	
	
	
	
}
