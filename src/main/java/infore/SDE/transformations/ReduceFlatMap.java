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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ReduceFlatMap extends RichFlatMapFunction<Estimation, Estimation> {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private HashMap<String, ReduceFunction> rf = new HashMap<>();

    private transient Counter numRecordsOut;
    private transient Meter emitRateMeter;
    private transient ScheduledExecutorService scheduler;
    private transient List<String> buffer;
    private transient Object bufferLock;
    private int pId;


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
                .meter("EmissionRate", new MeterView(5)); // 5-second window

        String pathName = "/tmp/flink-metrics-logs";
        String fileNameNumOfRecordsOut = "/tmp/flink-metrics-logs/par-8-CM-numRecordsOut.csv";
        String fileNameNumOfRecordsOutPerSec = "/tmp/flink-metrics-logs/emission-rate.csv";

        try {
            Files.write(
                    Paths.get(fileNameNumOfRecordsOutPerSec), new byte[0],  // Empty content
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.write(
                    Paths.get(fileNameNumOfRecordsOut), new byte[0],  // Empty content
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
        }

        MetricGroup metrics = getRuntimeContext().getMetricGroup();
        numRecordsOut = metrics.counter("numRecordsOut");

        buffer = new ArrayList<>();
        bufferLock = new Object();
        List<String> toEmissionWrite = new ArrayList<>();
        new File(pathName).mkdirs();

        scheduler = Executors.newScheduledThreadPool(3);

        // Collect metrics into memory every 10ms
        scheduler.scheduleAtFixedRate(() -> {
            long timestamp = System.currentTimeMillis();
            String entry = String.format("%d,%d,%d", timestamp, numRecordsOut.getCount(), pId);
            synchronized (bufferLock) {
                buffer.add(entry);
            }
        }, 0, 10, TimeUnit.MILLISECONDS); // <-- sampling intervals

        // Flush to file every 1 second
        scheduler.scheduleAtFixedRate(() -> {
            List<String> toWrite;
            synchronized (bufferLock) {
                if (buffer.isEmpty()) return;
                toWrite = new ArrayList<>(buffer);
                buffer.clear();
            }

            Path path = Paths.get(fileNameNumOfRecordsOut);
            try {
                Files.write(path, new ArrayList<>(toWrite),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                e.printStackTrace(); // or log using SLF4J
            }
        }, 1, 1, TimeUnit.SECONDS); // <-- flush interval

        // Flush to file every 5 second
        scheduler.scheduleAtFixedRate(() -> {
            toEmissionWrite.clear();
            String entry = String.format("%d,%d,%d", System.currentTimeMillis (),(int)emitRateMeter.getRate(), pId);
            toEmissionWrite.add(entry);

            Path path = Paths.get(fileNameNumOfRecordsOutPerSec);
            try {
                Files.write(path, toEmissionWrite,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                e.printStackTrace(); // or log using SLF4J
            }
        }, 1, 5, TimeUnit.SECONDS); // <-- flush interval
    }



    @Override
    public void close() throws Exception {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

}

