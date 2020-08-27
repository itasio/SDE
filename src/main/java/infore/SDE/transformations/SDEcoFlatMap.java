package infore.SDE.transformations;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import infore.SDE.synopses.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.messages.Datapoint;

public class SDEcoFlatMap extends RichCoFlatMapFunction<Datapoint, Request, Estimation> {

	private static final long serialVersionUID = 1L;
	private HashMap<String,ArrayList<Synopsis>> M_Synopses = new HashMap<>();
	private HashMap<String,ArrayList<ContinuousSynopsis>> MC_Synopses = new HashMap<>();
	private int pId;
	@Override
	public void flatMap1(Datapoint node, Collector<Estimation> collector) {

		//String value = node.f1.replace("\"", "");
		ArrayList<Synopsis>  Synopses =  M_Synopses.get(node.getKey());
		if (Synopses != null) {
			for (Synopsis ski : Synopses) {
				ski.add(node.getValues());
			}
			M_Synopses.put(node.getKey(),Synopses);
		}
		ArrayList<ContinuousSynopsis>  C_Synopses =  MC_Synopses.get(node.getKey());
		if (C_Synopses != null) {
			//if(C_Synopses.size()>1)
			//	System.out.println("kati_kati_kati _>" +" pId -> "+pId+"  " +C_Synopses.size());
			for (ContinuousSynopsis c_ski : C_Synopses) {
			Estimation e =c_ski.addEstimate(node.getValues());
			if(e.getEstimation()!=null){
				System.out.println(e.toString());
				collector.collect(e);
			}
			}
			MC_Synopses.put(node.getKey(),C_Synopses);
		}
	}

	@Override
	public void flatMap2(Request rq, Collector<Estimation> collector) throws Exception {
		ArrayList<Synopsis>  Synopses =  M_Synopses.get(rq.getKey());
		ArrayList<ContinuousSynopsis>  C_Synopses =  MC_Synopses.get(rq.getKey());
		System.out.println(pId + rq.toString());
		if (rq.getRequestID() == 1) {
			if(Synopses==null){
				Synopses = new ArrayList<>();
			}


			// countMin
			if (rq.getSynopsisID() == 1) {
				CountMin sketch;
				if (rq.getParam().length > 4)
					sketch = new CountMin(rq.getUID(), rq.getParam());
				else {
					String[] _tmp = { "1", "2", "0.0002", "0.99", "4" };
					sketch = new CountMin(rq.getUID(), _tmp);
				}

				Synopses.add(sketch);

				// BloomFliter
			} else if (rq.getSynopsisID() == 2) {
				Bloomfilter sketch;
				if (rq.getParam().length > 3)
					sketch = new Bloomfilter(rq.getUID(), rq.getParam());
				else {
					String[] _tmp = { "1", "1", "100000", "0.0002" };
					sketch = new Bloomfilter(rq.getUID(), _tmp);
				}
				Synopses.add(sketch);

				// AMS sketch
			} else if (rq.getSynopsisID() == 3) {
				AMSsynopsis sketch;
				if (rq.getParam().length > 3)
					sketch = new AMSsynopsis(rq.getUID(), rq.getParam());
				else {
					String[] _tmp = { "1", "2", "1000", "10" };
					sketch = new AMSsynopsis(rq.getUID(), _tmp);
				}

				Synopses.add(sketch);

				// TimeSeries sketch
			} else if (rq.getSynopsisID() == 4) {
				MultySynopsisDFT sketch;

				if (rq.getParam().length > 3)
				{
				sketch = new MultySynopsisDFT(rq.getUID(), rq.getParam());
				}
				else {
					String[] _tmp = {"1", "2", "5", "30", "8"};
					sketch = new MultySynopsisDFT(rq.getUID(), _tmp);
				}

				Synopses.add(sketch);
				// 5-> LSH - unfinished
			} else if (rq.getSynopsisID() == 5) {

				Bloomfilter sketch = new Bloomfilter(rq.getUID(), rq.getParam());
				Synopses.add(sketch);

				// Coresets
			} else if (rq.getSynopsisID() == 6) {
				FinJoinCoresets sketch;

				if (rq.getParam().length > 10)
					sketch = new FinJoinCoresets(rq.getUID(), rq.getParam());
				else {
					String[] _tmp = { "1","2", "5", "10" };
					sketch = new FinJoinCoresets(rq.getUID(), _tmp);
				}
				Synopses.add(sketch);
				// HyperLogLog
			} else if (rq.getSynopsisID() == 7) {
				HyperLogLogSynopsis sketch;
				if (rq.getParam().length > 2)
					sketch = new HyperLogLogSynopsis(rq.getUID(), rq.getParam());
				else {
					String[] _tmp = { "1", "1", "0.001" };
					sketch = new HyperLogLogSynopsis(rq.getUID(), _tmp);
				}
				Synopses.add(sketch);

				// StickySampling
			} else if (rq.getSynopsisID() == 8) {
				StickySamplingSynopsis sketch;
				if (rq.getParam().length > 4)
					sketch = new StickySamplingSynopsis(rq.getUID(), rq.getParam());
				else {
					String[] _tmp = { "1", "2", "0.01", "0.01", "0.0001"};
					sketch = new StickySamplingSynopsis(rq.getUID(), _tmp);
				}
				Synopses.add(sketch);

				// LossyCounting
			} else if (rq.getSynopsisID() == 9) {
				LossyCountingSynopsis sketch;
				if (rq.getParam().length > 2)
					sketch = new LossyCountingSynopsis(rq.getUID(), rq.getParam());
				else {
					String[] _tmp = { "1", "2", "0.0001" };
					sketch = new LossyCountingSynopsis(rq.getUID(), _tmp);
				}
				Synopses.add(sketch);

				// ChainSampler
			} else if (rq.getSynopsisID() == 10) {
				ChainSamplerSynopsis sketch;
				if (rq.getParam().length > 3)
					sketch = new ChainSamplerSynopsis(rq.getUID(), rq.getParam());
				else {
					String[] _tmp = { "2", "2", "1000", "100000" };
					sketch = new ChainSamplerSynopsis(rq.getUID(), _tmp);
				}
				Synopses.add(sketch);

				// GKsynopsis
			} else if (rq.getSynopsisID() == 11) {
				GKsynopsis sketch;
				if (rq.getParam().length > 3)
					sketch = new GKsynopsis(rq.getUID(), rq.getParam());
				else {
					String[] _tmp = { "2", "2", "0.01"};
					sketch = new GKsynopsis(rq.getUID(), _tmp);
				}
				Synopses.add(sketch);

				// 6-> dynamic load sketch
			} else if (rq.getSynopsisID() == 15) {

				Object instance;

				if (rq.getParam().length == 4) {

					File myJar = new File(rq.getParam()[2]);
					URLClassLoader child = new URLClassLoader(new URL[] { myJar.toURI().toURL() },
							this.getClass().getClassLoader());
					Class<?> classToLoad = Class.forName(rq.getParam()[3], true, child);
					instance = classToLoad.getConstructor().newInstance();
					Synopses.add((Synopsis) instance);

				} else {

					File myJar = new File("C:\\Users\\ado.kontax\\Desktop\\flinkSketches.jar");
					URLClassLoader child = new URLClassLoader(new URL[] { myJar.toURI().toURL() },
							this.getClass().getClassLoader());
					Class<?> classToLoad = Class.forName("com.yahoo.sketches.sampling.NewSketch", true, child);
					instance = classToLoad.getConstructor().newInstance();
					Synopses.add((Synopsis) instance);

				}
				// FINJOIN
			} else if (rq.getSynopsisID() == 13) {

				FinJoinSynopsis sketch;

				if (rq.getParam().length > 3)
					sketch = new FinJoinSynopsis(rq.getUID(), rq.getParam());
				else {
					String[] _tmp = { "0", "0", "10", "100", "8", "3" };
					sketch = new FinJoinSynopsis(rq.getUID(), _tmp);
				}
				Synopses.add(sketch);
				// COUNT
			} else if (rq.getSynopsisID() == 14) {

				Counters sketch;

				if (rq.getParam().length > 3)
					sketch = new Counters(rq.getUID(), rq.getParam());
				else {
					String[] _tmp = { "0", "0", "10", "100", "8", "3" };
					sketch = new Counters(rq.getUID(), _tmp);
				}
				Synopses.add(sketch);
			}
			M_Synopses.put(rq.getKey(),Synopses);
		} //Continuous Synopsis
	    else if(rq.getRequestID() == 5){
			if(rq.getSynopsisID() == 12)
			{
				if(C_Synopses==null){
					C_Synopses = new ArrayList<>();
				}

			ContinuousMaritimeSketches sketch;
			rq.setNoOfP(1) ;
				if (rq.getParam().length > 5)
					sketch = new ContinuousMaritimeSketches(rq.getUID(), rq, rq.getParam());
				else {
					String[] _tmp = {"1", "1", "18000","10000","50","50"};
					sketch = new ContinuousMaritimeSketches(rq.getUID(), rq, _tmp);
				}
				C_Synopses.add(sketch);

			}
			MC_Synopses.put(rq.getKey(),C_Synopses);
	    }
		// Estimate - delete
		else {
			for (Synopsis syn : Synopses) {

						if (rq.getUID() == syn.getSynopsisID()) {
							if (rq.getRequestID() % 10 == 2) {
								//System.out.println("removed");
								Synopses.remove(syn);
								M_Synopses.put(rq.getKey(),Synopses);

							} else if (rq.getRequestID() % 10 == 3){

								Estimation e = syn.estimate(rq);
								if(e.getEstimation() == null) {
									System.out.println(e.toString());
								}else{
									collector.collect(e);
									//System.out.println(pId+ "_"  + rq.getKey() + "_" + e.toString());
								}
							}
				}
			}
		}
	}

	public void open(Configuration config)  {
	 	pId = getRuntimeContext().getIndexOfThisSubtask();
	}

}
