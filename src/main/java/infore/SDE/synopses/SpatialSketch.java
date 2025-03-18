package infore.SDE.synopses;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.math.BigIntegerMath;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import org.apache.flink.api.java.tuple.Tuple2;

import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Stream;

public class SpatialSketch extends Synopsis {

    /** The highest resolution of the grid (n x n)   */
    private final int n;

    /** Current height of the hierarchy equivalent to the length of layers*/
    private final int levels;

    /** The kind of synopses that will be held in spatialsketch (e.g. CountMin, Bloomfilter etc.)*/
    private final int heldSynopsisID;

    /** The parameters of the Synopses held in each grid*/
    String[] heldSynParam;

    /** Maps each key to a specific grid. */
    private final HashMap<String, Synopsis[][]> grids = new HashMap<>();

    /** The last x_cell updated. To save time between updates*/
    private int prev_x = -1;
    /** The last y_cell updated. To save time between updates*/
    private int prev_y = -1;
    /** The last interval updated. To save time between updates*/
    private ArrayList<Tuple2<Integer, Integer>> prev_x_interval, prev_y_interval;

    /** Default resolution, but can increase dynamically (used when deleting grids(Dynamic SpatialSketch)*/
    private final int resolution = 1;

    /** The set of largest non-overlapping intervals, for n that are power of 2, this is simply [1, n], for n=11, this is [1,8], [9,10], [11,11]*/
    private final ArrayList<Dyadic1D> topLevelIntervals;


    public SpatialSketch(int uid, String[] parameters){
        super(uid,parameters[0],parameters[1], parameters[2]);
        n = Integer.parseInt(parameters[3]);
        if ( n <= 0 || n % 2 != 0){
            throw new IllegalArgumentException("Currently supported only grids that are power of 2.");
        }
        levels = BigIntegerMath.log2(BigInteger.valueOf(n), RoundingMode.FLOOR) + 1;

        topLevelIntervals = new ArrayList<>();
        topLevelIntervals.add(new Dyadic1D(1, n));

        heldSynopsisID = Integer.parseInt(parameters[4]);

        heldSynParam = new String[]{parameters[0], parameters[1], parameters[2]};   //the same as this spatialsketch's params
        String[] restOfHeldSynParam = Arrays.copyOfRange(parameters, 5, parameters.length);
        heldSynParam = Stream.concat(Arrays.stream(heldSynParam), Arrays.stream(restOfHeldSynParam)).toArray(String[]::new);

        verifyHeldSynParameters();
        initGrids();
    }

    private void verifyHeldSynParameters() {
        if (heldSynopsisID == 1 && heldSynParam.length == 6)   // COUNTMIN
            return;
        if (heldSynopsisID == 2 && heldSynParam.length == 5)   // BLOOMFILTER
            return;
        if (heldSynopsisID == 3 && heldSynParam.length == 5)   // AMS
            return;
        if (heldSynopsisID == 7 && heldSynParam.length == 4)   // HYPERLOGLOG
            return;
        if (heldSynopsisID == 8 && heldSynParam.length == 6)   // STICKYSAMPLING
            return;
        if (heldSynopsisID == 9 && heldSynParam.length == 4)   // LOSSYCOUNTING
            return;
        throw new IllegalArgumentException("Synopsis id not supported for SpatialSketch or Wrong number of parameters for the managed synopses.");
    }

    private void initGrids() {
        for (int i = 0; i < levels; i++) {
            for (int j = 0; j < levels; j++) {
                int xDim = (int)Math.pow(2, i);
                int yDim = (int)Math.pow(2, j);
                String key = getKeyFromDims(xDim, yDim);
                grids.put(key, new Synopsis[xDim][yDim]);
            }
        }
    }

    private String getKeyFromDims(int xDim, int yDim) {
        return xDim + "x" + yDim;
    }

    @Override
    public void add(Object k) {
        try {
            JsonNode node = (JsonNode)k;
            JsonNode key = node.get(this.keyIndex);
            JsonNode dataNode = node.get(this.valueIndex);
            JsonNode xPos = dataNode.get("x_pos");
            JsonNode yPos = dataNode.get("y_pos");
            JsonNode value = dataNode.get("value");
            String valueStr = value.asText();
            String keyStr = key.asText();
//        if (key == null || dataNode == null || xPos == null || yPos == null || value == null){
//            System.out.println("Data couldn't be added to synopsis. A data parameter name was incorrect");
//            return;
//        }

            int x = Integer.parseInt(xPos.asText());
            int y = Integer.parseInt(yPos.asText());

            if (x < 0 || y < 0 || x > n-1 || y > n-1){
                System.out.println("Data couldn't be added to synopsis. Each coordinate must be in range: [0 - "+(n-1)+"]");
                return;
            }

            ArrayList<Tuple2<Integer, Integer>> xIntervals, yIntervals;
            // Find the set of dyadic intervals for the given x,y by recursing on their respective top level interval
            if (x == prev_x) {
                xIntervals = prev_x_interval;
            } else {
                xIntervals = FindChildInterval(x + 1, 1, n);
                prev_x_interval = xIntervals;
                prev_x = x;
            }
            if (y == prev_y) {
                yIntervals = prev_y_interval;
            } else {
                yIntervals = FindChildInterval(y + 1, 1, n);
                prev_y_interval = yIntervals;
                prev_y = y;
            }

            for (Tuple2<Integer, Integer> x_inter: xIntervals){
                for(Tuple2<Integer, Integer> y_inter: yIntervals){
                    if ((x_inter.f1 - x_inter.f0 + 1 > n) && (y_inter.f1 - y_inter.f0 + 1 > n)) {
                        continue;
                    }
                    // Update the individual intervals
                    UpdateInterval(x_inter.f0 - 1, y_inter.f0 - 1, x_inter.f1 - 1, y_inter.f1 - 1, keyStr, valueStr);
                }
            }
        } catch (NumberFormatException e) {
            System.out.println("Data couldn't be added to synopsis. One of the values passed to synopsis couldn't be converted to integer.");
        } catch (NullPointerException e){
            System.out.println("Data couldn't be added to synopsis. A data parameter name was incorrect.");
        }


    }

    private void UpdateInterval(int x1, int y1, int x2, int y2, String keyStr, String value) {
        // The key of the grid
        String key = getKeyFromDims(n / (x2 - x1 + 1), n / (y2 - y1 + 1));
        Synopsis[][] gridToUpdate = grids.get(key);
        if (gridToUpdate == null)   //grid has been dropped
            return;
        int x_cell = x1/(x2-x1+1);
        int y_cell = y1/(y2-y1+1);

        if(gridToUpdate[x_cell][y_cell] == null){
            //  initialize sketch in this cell
            Synopsis sk = initHeldSketch();
            gridToUpdate[x_cell][y_cell] = sk;
        }
        updateSketch(gridToUpdate[x_cell][y_cell], keyStr, value);  //send the sketch with new data
//        System.out.println("Updated sketch in grid with dims: "+key +" in position: ["+ x_cell + "," + y_cell + "]");
    }

    private ArrayList<Tuple2<Integer, Integer>> FindChildInterval(int target, int start, int end) {
        ArrayList<Tuple2<Integer, Integer>> intervals = new ArrayList<>();
        for (int i = 0; i < levels; i++) {
            intervals.add(new Tuple2<>(-1, -1));
        }
        int diff = end - start + 1;
        for (int i = 0; i < levels; i++) {
            if (diff < resolution) {
                // Check if interval is large enough to be considered for given resolution
                break;
            }
            intervals.get(i).f0 = start;
            intervals.get(i).f1 = end;
            if (target == start && target == end) {
                // base case (target, target)
                break;
            } else if (start == end && target != start) {
                System.out.println("Error in base case " + target + ", (" + start + ", " + end + ")");
                break;
            } else {
                // Split interval
                if (isWithin(target, start, start + diff/2 - 1)) {
                    end = start + diff/2 - 1;
                } else if (isWithin(target, start + diff/2, end)) {
                    start = start + diff/2;
                } else {
                    System.out.println("Error in recursion " + target + ", (" + start + ", " + end + ")");
                    break;
                }
                diff = diff/2;
            }
        }
        return intervals;
    }

    private boolean isWithin(int target, int start, int end) {
        return target >= start && target <= end;
    }

    /**
     * Update the specified Synopsis with key and value
     * @param synopsis the synopsis to update
     * @param key the key to add in synopsis
     * @param value the value for the specified key
     */
    private void updateSketch(Synopsis synopsis, String key, String value) {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode jsonNode = objectMapper.createObjectNode();

        jsonNode.put(heldSynParam[0], key);
        jsonNode.put(heldSynParam[1], value);
        synopsis.add(jsonNode);
    }

    private Synopsis initHeldSketch() {
        switch (heldSynopsisID){
            case 1:
                return new CountMin(heldSynopsisID, heldSynParam);
            case 2:
                return new Bloomfilter(heldSynopsisID, heldSynParam);
            case 3:
                return new AMSsynopsis(heldSynopsisID, heldSynParam);
            case 7:
                return new HyperLogLogSynopsis(heldSynopsisID, heldSynParam);
            case 8:
                return new StickySamplingSynopsis(heldSynopsisID, heldSynParam);
            case 9:
                return new LossyCountingSynopsis(heldSynopsisID, heldSynParam);
            default:
                throw new IllegalArgumentException("Synopsis id not supported for SpatialSketch yet");
        }
    }

    @Override
    public Object estimate(Object k) {
        return null;
    }

    @Override
    public Estimation estimate(Request rq) {
        try {
            String[] paramsArray = rq.getParam();
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(paramsArray[0]);

            String extractedQueryKey = rootNode.get("queryKey").asText();
            JsonNode rs = rootNode.get("ranges");

            Iterator<JsonNode> iter = rs.elements();
            ArrayList<int[]> rangesToQuery = new ArrayList<>();
            ArrayList<Tuple2<Object, Float>> est_cov = new ArrayList<>();

            while (iter.hasNext()){
                rangesToQuery.add(objectMapper.convertValue(iter.next(), int[].class));
            }

            // To lead the estimation to the respective Reduce Function of the held synopses.
            rq.setSynopsisID(this.heldSynopsisID);

            String[] oldParam = rq.getParam();
            String[] newParam = Arrays.copyOf(oldParam, oldParam.length+1);
            newParam[newParam.length - 1] = "spatial";  // To distinguish in the Reduce Function that this estimation comes from spatial sketch.
            rq.setParam(newParam);

            if (rangesToQuery.isEmpty()) {
               //No ranges have been given as parameter
                est_cov.add(new Tuple2<>("0",0F));
                return new Estimation(rq, est_cov, Integer.toString(rq.getUID()));    // Estimation is simply zero
            }

            ArrayList<Tuple2<Synopsis, Float>> sketchesForEst = findSketchesInRange(rangesToQuery);

            if (sketchesForEst.isEmpty()){
                est_cov.add(new Tuple2<>("0",0F));
                return new Estimation(rq, est_cov, Integer.toString(rq.getUID()));    // Estimation is simply zero
            }

            for (Tuple2<Synopsis, Float> sk_cov : sketchesForEst){
                Synopsis sk = sk_cov.f0;
                Object est = sk.estimate(extractedQueryKey);
                est_cov.add(new Tuple2<>(est, sk_cov.f1));
            }

            return new Estimation(rq, est_cov, Integer.toString(rq.getUID()));
        } catch (Exception e){
            System.out.println("Synopsis couldn't be queried. An error occurred while parsing request parameters.");
            System.out.println("Request param must be JSON like this: \n" +
                    "{\n"+
                    "  \"ranges\": [[x1, y1, x2, y2], [x3, y3, x4, y4]],\n" +
                    "  \"queryKey\": key,\n" +
                    "  \"queryKeyEnd\": keyEnd,\n" +
                    "  \"timestamp\": timestamp \n" +
                    "}");
            return new Estimation(rq, null, Integer.toString(rq.getUID()));

        }
    }

    /**
     * Finds the sketches that correspond to the specified ranges
     * @param rangesToQuery The ArrayList of ranges in which to search for synopses
     * @return An ArrayList of the synopses that correspond to the given ranges, accompanied with their respective coverage.
     *          If no sketch correspond to specified ranges, or no sketch in these ranges has been initialized, the arrayList will be empty.
     */
    private ArrayList<Tuple2<Synopsis, Float>> findSketchesInRange(ArrayList<int[]> rangesToQuery) {
        ArrayList<Tuple2<Synopsis, Float>> sketches = new ArrayList<>();
        for (int[] r: rangesToQuery){
            if (r.length != 4)
                continue;   //only ranges in the form x1, y1, x2, y2 are valid
            ArrayList<Dyadic2D> dyadicIntervals = getDyadicIntervals(r[0], r[1], r[2], r[3]);
            for (Dyadic2D di : dyadicIntervals){
                di.x1--;
                di.x2--;
                di.y1--;
                di.y2--;

                String key = getKeyFromDims(n / (di.x2 - di.x1 + 1), n / (di.y2 - di.y1 + 1));
                Synopsis[][] gridToQuery = grids.get(key);
                if (gridToQuery == null)   //grid has been dropped, go to next interval
                    continue;
                int x_cell = di.x1/(di.x2-di.x1+1);
                int y_cell = di.y1/(di.y2-di.y1+1);
                if(gridToQuery[x_cell][y_cell] != null){
                    //the required sketch of this grid has been initialized
                    sketches.add(new Tuple2<>(gridToQuery[x_cell][y_cell], di.coverage));
                }
            }
        }
        return sketches;
    }

    private ArrayList<Dyadic2D> getDyadicIntervals(int x1, int y1, int x2, int y2) {
        ArrayList<Dyadic2D> dIntervals = new ArrayList<>();

        if (x1 > x2 || y1 > y2) {
            System.out.println("Query parameters set incorrectly. They must be x2 >= x1 and y2 >= y1");
            return dIntervals;
        }
        // x dimension
        ArrayList<Dyadic1D> x_intervals = getOneDimIntervals(x1, x2);

        // y dimension
        ArrayList<Dyadic1D> y_intervals = getOneDimIntervals(y1, y2);

        // Combine x and y intervals into 2D intervals
        for(Dyadic1D x_int : x_intervals){
            for (Dyadic1D y_int : y_intervals){
                Dyadic2D dyadicInt = new Dyadic2D(x_int.start, y_int.start, x_int.end, y_int.end, x_int.coverage * y_int.coverage);
                dIntervals.add(dyadicInt);
            }
        }

        return dIntervals;
    }

    private ArrayList<Dyadic1D> getOneDimIntervals(int start, int end) {
        ArrayList<Dyadic1D> intervals = new ArrayList<>();
        for (Dyadic1D interval : topLevelIntervals){
            OverlapType overlap = intervalOverlap(start+1, end+1, interval.start, interval.end);
            if (overlap == OverlapType.COVERS){
                // Exact overlap, thus this interval is required, but continue the loop for potential other intervals
                intervals.add(interval);
            } else if (overlap == OverlapType.FULLY_CONTAINED) {
                // interval completely contained, therefore the other top level intervals do not have to be considered
                Dyadic1D target = new Dyadic1D(start + 1, end + 1);
                intervals = ObtainIntervals(target, interval);
                break;
            } else if (overlap == OverlapType.LOWER_CONTAINED) {
                // Lower overlap, implying upper part is out of range, therefore we can break afterwards
                Dyadic1D target = new Dyadic1D(start + 1, end + 1);
                ArrayList<Dyadic1D> subIntervals = ObtainIntervals(target, interval);
                intervals.addAll(subIntervals);
            } else if (overlap == OverlapType.UPPER_CONTAINED) {
                // Upper overlap
                Dyadic1D target = new Dyadic1D(start + 1, end + 1);
                ArrayList<Dyadic1D> subIntervals = ObtainIntervals(target, interval);
                intervals.addAll(subIntervals);
            }
        }
        return intervals;
    }

    /**
     * Given a target 1D range that overlaps with a higher level dyadic interval in some manner,
     * obtain the dyadic intervals that together compose the target part that overlaps with the base
     */
    private ArrayList<Dyadic1D> ObtainIntervals(Dyadic1D target, Dyadic1D base) {
        // Check for resolution compliance
        if (base.end - base.start + 1 < resolution) {
            return new ArrayList<>();
        } else if (target.start == base.start && target.end == base.end) {
            // If exactly overlap, return
            // split interval, if overlap lower, recurse lower, if overlap upper, recurse upper
            ArrayList<Dyadic1D> res = new ArrayList<>();
            res.add(target);
            return res;
        } else {
            // Recursion
            int baseRange = base.end - base.start + 1;
            int power = BigIntegerMath.log2(BigInteger.valueOf(baseRange), RoundingMode.FLOOR);
            // Split interval
            Dyadic1D lower_base = new Dyadic1D(base.start, base.end - (int) Math.pow(2, power - 1));
            Dyadic1D upper_base = new Dyadic1D(base.start + (int) Math.pow(2, power - 1), base.end);

            ArrayList<Dyadic1D> lower_res = getPartialIntervals(target, lower_base, base);
            ArrayList<Dyadic1D> upper_res = getPartialIntervals(target, upper_base, base);

            lower_res.addAll(upper_res);
            return lower_res;
        }
    }

    private ArrayList<Dyadic1D> getPartialIntervals(Dyadic1D target, Dyadic1D halfIntVal, Dyadic1D base){
        ArrayList<Dyadic1D> half_res = new ArrayList<>();
        if (intervalOverlap(target.start, target.end, halfIntVal.start, halfIntVal.end) != OverlapType.NONE){
            int dStart = Math.max(target.start, halfIntVal.start);
            int dEnd = Math.min(target.end, halfIntVal.end);
            Dyadic1D targetParam = new Dyadic1D(dStart, dEnd);
            half_res = ObtainIntervals(targetParam, halfIntVal);
            if (half_res.isEmpty()) {
                Dyadic1D partial = base;
                partial.coverage = (float) (target.end - target.start + 1) / (float) (base.end - base.start + 1);
                half_res.add(partial);
            }
        }
        return half_res;
    }

    /**
     * Discovers how the interval [start1, end1] overlaps in [start2, end2]
     *
     * @return  COVERS if [start1, end1] covers [start2, end2]
     *          <br>
     *          FULLY_CONTAINED if [start1, end1] is fully contained in  [start2, end2]
     *          <br>
     *          LOWER_CONTAINED if [start1, end1] is contained in the lower part of [start2, end2]
     *          <br>
     *          UPPER_CONTAINED if [start1, end1] is contained in the upper part of [start2, end2]
     *          <br>
     *          NONE [start1, end1] does not overlap with [start2, end2]
     */
    private OverlapType intervalOverlap(int start1, int end1, int start2, int end2) {
        if ((start1 <= start2 && end1 >= end2)) {
            // e.g. [1---------8]
            //         [2---4]
            return OverlapType.COVERS;
        } else if (start1 >= start2 && end1 <= end2) {
            // e.g.    [2--4]
            //      [1---------8]
            return OverlapType.FULLY_CONTAINED;
        } else if (start1 <= start2 && end1 >= start2 && end1 <= end2) {
            // e.g. [1-----4]
            //          [2-------6]
            return OverlapType.LOWER_CONTAINED;
        } else if (start1 >= start2 && start1 <= end2 && end1 >= end2) {
            // e.g.     [2-------6]
            //      [1-----4]
            return OverlapType.UPPER_CONTAINED;
        } else {
            return OverlapType.NONE;
        }
    }
private enum OverlapType {
    COVERS, FULLY_CONTAINED, LOWER_CONTAINED, UPPER_CONTAINED, NONE
}
    @Override
    public Synopsis merge(Synopsis... sk) {
        return null;
    }

    static class Dyadic2D {
        int x1, y1, x2, y2;

        public Dyadic2D(int x1, int y1, int x2, int y2, float coverage) {
            this.x1 = x1;
            this.y1 = y1;
            this.x2 = x2;
            this.y2 = y2;
            this.coverage = coverage;
        }

        float coverage;
    }

    static class Dyadic1D {
        int start, end;
        float coverage;

        public Dyadic1D(int start, int end, float coverage) {
            this.start = start;
            this.end = end;
            this.coverage = coverage;
        }

        public Dyadic1D(int start, int end) {
            this.start = start;
            this.end = end;
            this.coverage = 1;
        }
    }
}
