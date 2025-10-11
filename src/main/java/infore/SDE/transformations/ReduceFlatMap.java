package infore.SDE.transformations;

import infore.SDE.reduceFunctions.*;
import infore.SDE.messages.Estimation;
import infore.SDE.reduceFunctions.WLSH_Reduce;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;

import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReduceFlatMap extends RichFlatMapFunction<Estimation, Estimation> {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private HashMap<String, ReduceFunction> rf = new HashMap<>();

    private transient boolean haveWrittenNumRecOut = false;
    private transient Counter numRecordsOut;
//    private transient InstantRateMeter emitRateMeter;
    private transient Meter emitRateMeter;
    private transient ScheduledExecutorService scheduler;
    private transient ConcurrentLinkedQueue<String> buffer; // lock-free
    private transient ConcurrentLinkedQueue<String> bufferEmitRate; // lock-free
    private int pId;
    private transient BufferedWriter outStreamNumOfRecordsOut;
    private transient BufferedWriter outStreamNumOfRecordsOutPerSec;
    private static final Logger LOG = LoggerFactory.getLogger(ReduceFlatMap.class);

    @Override
    public void flatMap(Estimation value, Collector<Estimation> out){

        ReduceFunction t_rf = rf.get("" + value.getEstimationkey());
        int id = value.getSynopsisID();
        String key  = value.getEstimationkey();

            if (t_rf == null){

                t_rf = initReduceFunction(value, id);
                rf.put("" + key, t_rf);

            }else{

                if (t_rf.add(value)) {
                    Object output = null;
                    try {
                        output = t_rf.reduce();
                    } catch (IllegalArgumentException e) {
                        System.out.println(e.getMessage());
                        rf.remove("" + key);
                    }
                    if (output != null) {
                        value.setEstimation(output);
                        rf.remove("" + key);
                        if(id == 28)
                            value.setEstimationkey(value.getUID()+"");
                        emitRateMeter.markEvent();

//                        int curRate = (int) emitRateMeter.getRate();
//                        String entry = String.format("%d,%d,%d", System.currentTimeMillis (),curRate, pId);
//                        System.out.println(entry);      //print rate for every record out (only gather metrics over 1000 estimate requests)

                        numRecordsOut.inc();    //estimate has been calculated
                        out.collect(value);
                    }

                }
            }
        }

    private ReduceFunction initReduceFunction(Estimation value, int id) {
        ReduceFunction t_rf = null;
        //RadiusCount
        if (id == 100) {
            t_rf = new RadiusReduce(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID(), value.getRequestID());
            t_rf.add(value);
        }

        //MAX
        if (id == 11) {
            t_rf = new SimpleMaxFunction(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID(), value.getRequestID());
            t_rf.add(value);
        }
        //AVG
        else if (id == 15) {
            t_rf = new SimpleAvgFunction(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID(), value.getRequestID());
            t_rf.add(value);
        }
        //SUM
        else if (id == 1 || id == 3 || id == 8 || id == 9 || id == 7) {

            if (id == 1 && value.getRequestID() % 10 == 6) {
                t_rf = new JoinEstimationFunction(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID(), value.getRequestID());
            } else if (id == 1 && value.getRequestID() % 10 == 8) {
                t_rf = new JoinedEstimationFunction(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID(), value.getRequestID());
            } else {
                t_rf = new SimpleSumFunction(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID(), value.getRequestID());
            }
            t_rf.add(value);
        }
        //OR
        else if (id == 2) {
            if (value.getRequestID() % 10 == 8)
                t_rf = new JoinedEstimationFunction(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID(), value.getRequestID());
            else
                t_rf = new SimpleORFunction(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID(), value.getRequestID());
            t_rf.add(value);
        }
        //DFT CORRELATION
        else if (id == 4){
            t_rf = new CorrelationDFTReduce(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID(), value.getRequestID());
            t_rf.add(value);
        }
        //KMEANS CORESETS
        else if( id == 6) {
            //System.out.println("START");
            t_rf = new KmeansReduce(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID(), value.getRequestID());
            t_rf.add(value);
        }
        //KMEANS CORESETS
        else if( id == 13) {
            //System.out.println("START");
            t_rf = new TopKReduce(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID(), value.getRequestID());
            t_rf.add(value);
        }
        //WINDOW LSH SYNOPSIS
        else if (id == 28) {
            t_rf = new WLSH_Reduce(value.getNoOfP(), 0,value.getEstimationkey(), Double.parseDouble(value.getParam()[0]),Integer.parseInt(value.getParam()[1]));
            t_rf.add(value);
        }
        else if (id == 29) {
            t_rf = new WDFT_Reduce(value.getNoOfP(), Double.parseDouble(value.getParam()[0]),Integer.parseInt(value.getParam()[0]),Integer.parseInt(value.getParam()[0]),stringToStringArray(value.getParam()[0]));
            //int workers, double th, int k, int t, String[] stock
            t_rf.add(value);
        }
        return t_rf;
    }


    private  String[] stringToStringArray(String param)
    {
        return param.split(";");
    }

    public void open(Configuration config)  {
        pId = getRuntimeContext().getIndexOfThisSubtask();

        emitRateMeter = getRuntimeContext()
                .getMetricGroup()
                .meter("EmissionRate", new MeterView(1));

        String pathName = "/tmp/flink-metrics-logs";

        if (new File(pathName).mkdirs()) {
            LOG.warn("Directory: {} couldn't be created properly", pathName);
        }

        String fileNameNumOfRecordsOut = pathName + "/numRecordsOut.csv";
        String fileNameNumOfRecordsOutPerSec = pathName + "/emission-rate.csv";

        Path pathNumOfRecordsOutPerSec = Paths.get(fileNameNumOfRecordsOutPerSec);
        Path pathNumOfRecordsOut = Paths.get(fileNameNumOfRecordsOut);

        try {
            Files.write(
                    pathNumOfRecordsOutPerSec, new byte[0],  // Empty content
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.write(
                    pathNumOfRecordsOut, new byte[0],  // Empty content
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

            outStreamNumOfRecordsOut = Files.newBufferedWriter(pathNumOfRecordsOut, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            outStreamNumOfRecordsOutPerSec = Files.newBufferedWriter(pathNumOfRecordsOutPerSec, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        MetricGroup metrics = getRuntimeContext().getMetricGroup();
        numRecordsOut = metrics.counter("numberOfRecordsOut");

        buffer = new ConcurrentLinkedQueue<>();
        bufferEmitRate = new ConcurrentLinkedQueue<>();

        scheduler = Executors.newSingleThreadScheduledExecutor();

        // Collect metrics into memory every 500ms
        scheduler.scheduleAtFixedRate(() -> {
            long timestamp = System.currentTimeMillis();
            String entry = String.format("%d,%d,%d", timestamp, numRecordsOut.getCount(), pId);
            buffer.add(entry);

            String entryEmitRate = String.format("%d,%d,%d", timestamp,(int)emitRateMeter.getRate(), pId);
            bufferEmitRate.add(entryEmitRate);
        }, 0, 1, TimeUnit.SECONDS); // <-- sampling intervals

        // Flush to file every 5 second
        scheduler.scheduleAtFixedRate(() -> {
            List<String> toWrite = new ArrayList<>();
            String item;
            while ((item = buffer.poll()) != null){
                toWrite.add(item);
            }

            List<String> toWriteEmitRate = new ArrayList<>();
            String itemEmitRate;
            while ((itemEmitRate = bufferEmitRate.poll()) != null){
                toWriteEmitRate.add(itemEmitRate);
            }

            try {
                for (String str : toWrite){
                    outStreamNumOfRecordsOut.write(str+"\n");
                }
                if (!haveWrittenNumRecOut) {
                    LOG.info("numRecordsOut is about to be flushed for subtask: {}", pId);
                    haveWrittenNumRecOut = true;
                }
                outStreamNumOfRecordsOut.flush();
                for (String str : toWriteEmitRate) {
                    outStreamNumOfRecordsOutPerSec.write(str+"\n");
                }
                outStreamNumOfRecordsOutPerSec.flush();
            } catch (IOException e) {
                e.printStackTrace(); // or log using SLF4J
            } catch (Throwable t) {
                LOG.error("Unexpected error in metric writer", t);
            }
        }, 1, 5, TimeUnit.SECONDS); // <-- flush interval
    }



    @Override
    public void close() throws Exception {
        if (scheduler != null) scheduler.shutdownNow();
        if (outStreamNumOfRecordsOut != null){
            outStreamNumOfRecordsOut.flush();
            outStreamNumOfRecordsOut.close();
        }
        if (outStreamNumOfRecordsOutPerSec != null){
            outStreamNumOfRecordsOutPerSec.flush();
            outStreamNumOfRecordsOutPerSec.close();
        }
    }

}

