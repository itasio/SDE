package infore.SDE.synopses;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.math.BigIntegerMath;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import org.apache.flink.api.java.tuple.Tuple2;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jol.info.GraphLayout;

import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Stream;

import static java.lang.Math.floor;

public class SpatialSketch extends Synopsis {

    /** The highest resolution of the grid (n x n)   */
    private final int n;

    /** Current height of the hierarchy equivalent to the length of layers*/
    private int levels;

    /** The kind of synopses that will be held in spatialsketch (e.g. CountMin, Bloomfilter etc.)*/
    private final int heldSynopsisID;

    /** The parameters of the Synopses held in each grid*/
    String[] heldSynParam;

    /** Maps each key to a specific grid. */
    private final HashMap<String, Synopsis[][]> grids = new HashMap<>();

    /** Stores a snapshot of keys of the grids.*/
    private final HashSet<String> listOfGridKeys ;

    /** Keys of grids that have been dropped while reducing memory.</br>
     * Populated every time a grid gets dropped.
    Cached here to remove keys listOfGridKeys after update of sketches is finished. After that, gridsDropped gets cleared. */
    private final ArrayList<String> gridsDropped = new ArrayList<>();

    /** Default resolution, but can increase dynamically (used when deleting grids(Dynamic SpatialSketch)*/
    private int resolution = 1;

    /** The set of largest non-overlapping intervals, for n that are power of 2, this is simply [1, n], for n=11, this is [1,8], [9,10], [11,11]*/
    private final ArrayList<Dyadic1D> topLevelIntervals;

    /** The memory limit for dynamic spatialsketch (Dynsketch)*/
    private final long memoryLimit;

    /** The memory used from the {@link #grids} */
    private long currentMemoryUsed;

    /** The combined exponent of the diagonal layer resolution to be dropped.
     *First grids to drop are g(2^1, 2^0) and g(2^0, 2^1), where exponent sum is odd*/
    private int diagExponent = 1;

    /** The keys of the grids to be dropped in DynSketch. When empty, we move to next layer or phase.
     * There are two phases in reduction of DynSketch. <br>
     * Phase 1: Drop grids of diagonal layers sequentially in alternating layers <br>
     * Phase 2: Drop the highest resolution grids*/
    private ArrayList<String> gridsToBeDropped = new ArrayList<>();

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

        memoryLimit = Long.parseLong(parameters[5]);

        heldSynParam = new String[]{parameters[0], parameters[1], parameters[2]};   //the same as this spatialsketch's params
        String[] restOfHeldSynParam = Arrays.copyOfRange(parameters, 6, parameters.length);
        heldSynParam = Stream.concat(Arrays.stream(heldSynParam), Arrays.stream(restOfHeldSynParam)).toArray(String[]::new);

        verifyHeldSynParameters();
        initGrids();
        listOfGridKeys =  new HashSet<>(grids.keySet());
        currentMemoryUsed = getTrueSize(grids) + getTrueSize(listOfGridKeys);
        isMemoryLimitReached();
        updateListOfGridKeys();

    }

/*    private long calculateMemoryUsed() {
        long currentMemoryUsage = MemoryAgent.getObjectSize(grids);
        // we need to take into account all the objects that exist in grids
        for (Map.Entry<String, Synopsis[][]> entry : grids.entrySet()) {
            String key = entry.getKey();
            Synopsis[][] grid = entry.getValue();
            currentMemoryUsage += MemoryAgent.getObjectSize(key);
            currentMemoryUsage += MemoryAgent.getObjectSize(grid);
            for(Synopsis[] synRow : grid){
                for (Synopsis syn : synRow){
                    if (syn != null)
                        currentMemoryUsage += MemoryAgent.getObjectSize(syn);
                }
            }
        }
        return currentMemoryUsage;
    }
*/
    /** Finds the memory usage of the specified object*/
    private long getTrueSize(Object obj){
        if (obj == null)
            return 0;
        return GraphLayout.parseInstance(obj).totalSize();
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
        if (heldSynopsisID == 11 && heldSynParam.length == 5)   // GKQUANTILES
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

    private Tuple2<Integer, Integer> getDimsFromKey(String key){
        String[] x = key.split("x");
        int xDim = Integer.parseInt(x[0]);
        int yDim = Integer.parseInt(x[1]);
        return new Tuple2<>(xDim, yDim);
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

            for (String keyOfGrid : listOfGridKeys) {
                if (isMemoryLimitReached()){
                    return;
                }

                // get the dims of the grid
                Tuple2<Integer, Integer> dims = getDimsFromKey(keyOfGrid);
                int xDim = dims.f0;
                int yDim = dims.f1;

                Synopsis[][] grid = grids.get(keyOfGrid);
                if (grid == null) {
                    // grid does not exist. It has been dropped while reducing memory
                    // continue in next key
                    continue;
                }

                int xMax = n ;
                int yMax = n ;

                int xCell = (int) floor(x * ((double) xDim / xMax));
                int yCell = (int) floor(y * ((double) yDim / yMax));

                if(grid[xCell][yCell] == null){
                    //  initialize sketch in this cell
                    Synopsis sk = initHeldSketch();
                    grid[xCell][yCell] = sk;
                    currentMemoryUsed += getMemoryDiff(sk, null);
                }
                updateSketch(grid[xCell][yCell], keyStr, valueStr);  //send the sketch with new data
            }

            // all sketches have been updated i.e. one sketch per grid remove the deleted sketches
            // , if any, from the listOfGridKeys, in order to be consistent with the grids
            updateListOfGridKeys();

        } catch (NumberFormatException e) {
            System.out.println("Data couldn't be added to synopsis. One of the values passed to synopsis couldn't be converted to integer.");
        } catch (NullPointerException e){
            System.out.println("Data couldn't be added to synopsis. A data parameter name was incorrect.");
        }


    }

    /**
     * Make {@link #listOfGridKeys} consistent with {@link #grids}. </br>
     * During reducing memory an entry might have been dropped from {@link #grids} and cached in {@link #gridsDropped}.
     * Removes those grids for {@link #listOfGridKeys} also and clear {@link #gridsDropped}
     */
    private void updateListOfGridKeys(){
        long beforeMem = getTrueSize(listOfGridKeys);
        if (!gridsDropped.isEmpty()){
            for (String gridDeleted : gridsDropped){
                listOfGridKeys.remove(gridDeleted);
            }
            gridsDropped.clear();
        }
        long afterMem =  getTrueSize(listOfGridKeys);
        currentMemoryUsed -= (beforeMem - afterMem);
    }

    private boolean isMemoryLimitReached() {
        if (memoryLimit <= 0)   //memory limit not set
            return false;
        while (currentMemoryUsed >= memoryLimit){
            if (levels == 1){   //only grid 1x1 left. Can't delete anymore.
                System.out.println("Memory limit for current sketch has been reached. Can't add more elements.");
                return true;
            }
            dropNextGrid();
        }
        return false;
    }

    private void dropNextGrid() {
        if (gridsToBeDropped.isEmpty()) {
            if ( diagExponent < (levels-1)*2 ) {
                // phase 1
                gridsToBeDropped = getDiagonalGridKeys(diagExponent, levels);
                diagExponent += 2;  // grids deletion happens in alternating layers
            }else {
                // phase 2 (all diagonal grids have been dropped)
                gridsToBeDropped = getHighestResolutionGridKeys();
            }
        }
        boolean dropped = false;
        while (!dropped){
            dropped = dropGrid( gridsToBeDropped.remove(0) );
        }
    }

    private boolean dropGrid(String key) {
        Synopsis [][] grid = grids.remove(key);
        if (grid == null){
            return false;       //this grid does not exist
        }
        currentMemoryUsed -= getMemoryDiff(grid, null);
        currentMemoryUsed -= getMemoryDiff(key, null);

        // A grid is dropped. Add it to the list, in order to update later the listOfGridKeys
        gridsDropped.add(key);

        if (key.equals(getKeyFromDims(n / resolution, n / resolution))){    //removed highest resolution grid
            resolution *= 2;
            levels -= 1;
            System.out.println("Resolution reduced to " + n/resolution);
        }
        return true;
    }

    /**
     * Gets all grid keys of the grids that lay on the L-shape, i.e., have at least one axis with the highest resolution
     */
    private ArrayList<String> getHighestResolutionGridKeys() {
        ArrayList<String> xKeys = new ArrayList<>();    //keys for grids with max X resolution
        ArrayList<String> yKeys = new ArrayList<>();    //keys for grids with max Y resolution
        ArrayList<String> totalKeys = new ArrayList<>();

        for (int y = 0; y < levels; y += 1) { // steps of 2 to skip odd sized exponent sums which should already been dropped
            xKeys.add(getKeyFromDims((int) Math.pow(2, levels - 1), (int) Math.pow(2, y)));
        }

        for (int x = 0; x < levels; x += 1) {
            yKeys.add(getKeyFromDims((int) Math.pow(2, x), (int) Math.pow(2, levels - 1)));
        }

        //sort them so that keys for grids with the fewest cells (Synopses) are first
        boolean swap = true;
        while (!xKeys.isEmpty() && !yKeys.isEmpty()) {
            if (swap) {
                totalKeys.add(xKeys.remove(0));
            } else {
                totalKeys.add(yKeys.remove(0));
            }
            swap = !swap;
        }
        return totalKeys;
    }

    /**
     * Generate all grid keys of the grids that lay on the diagonal of a given exponent sum
     * i.e., for some grid g(2^i, 2^j), the exponent sum is i + j
     */
    private ArrayList<String> getDiagonalGridKeys(int exponent_sum, int max_exponent) {
        ArrayList<String> keys = new ArrayList<>();
        int i, j = 0;
        for (i = 0; i <= exponent_sum && i+j <= exponent_sum && i < max_exponent; i++) {
            for (; j <= exponent_sum && i+j <= exponent_sum && j < max_exponent; j++) {
                if (i + j == exponent_sum) {
                    keys.add(getKeyFromDims((int) Math.pow(2, i), (int) Math.pow(2, j)));
                }
            }
            j = 0;
        }
        return keys;
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
            currentMemoryUsed += getMemoryDiff(sk, null);
        }
        updateSketch(gridToUpdate[x_cell][y_cell], keyStr, value);  //send the sketch with new data



        //memory of synopsis stays the same when inserting data

//        System.out.println("Updated sketch in grid with dims: "+key +" in position: ["+ x_cell + "," + y_cell + "]");
    }

    /**
     * Finds the difference in memory usage between two objects
     * @return The absolute difference in memory usage between the specified objects.<br>
     * Zero if both objects are null.<br>
     * The memory used by the non-null object, if one of them is null.
     */
    private long getMemoryDiff(Object objA, Object objB) {

        if (objA == null && objB == null)
            return 0;
        else if (objA != null && objB == null)
            return getTrueSize(objA);
//            return MemoryAgent.getObjectSize(objA);
        else if (objA == null && objB != null)
            return getTrueSize(objB);
//            return MemoryAgent.getObjectSize(objB);
        else
            return Math.abs(getTrueSize(objA) - getTrueSize(objB));
//            return Math.abs(MemoryAgent.getObjectSize(objA) - MemoryAgent.getObjectSize(objB));
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
            case 11:
                return new GKsynopsis(heldSynopsisID, heldSynParam);
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

            String rangesStr = getStringFromRanges(rangesToQuery);
            // To lead the estimation to the respective Reduce Function of the held synopses.
            rq.setSynopsisID(this.heldSynopsisID);

            String[] oldParam = rq.getParam();
            String[] newParam = Arrays.copyOf(oldParam, oldParam.length+1);
            newParam[newParam.length - 1] = "spatial";  // To distinguish in the Reduce Function that this estimation comes from spatial sketch.
            rq.setParam(newParam);

            if (rangesToQuery.isEmpty()) {
               //No ranges have been given as parameter
                est_cov.add(new Tuple2<>("0",0F));
                return new Estimation(rq, est_cov, Integer.toString(rq.getUID())+ "," + extractedQueryKey + "," + rangesStr);    // Estimation is simply zero
                //add queryKey and rangesToQuery in estimation key, in order to be correctly reduced in ReduceFlatMap
                //(many simultaneous requests for the same sketch could cause wrong estimate if only rq.getUID() was used)
            }

            ArrayList<Tuple2<Synopsis, Float>> sketchesForEst = findSketchesInRanges(rangesToQuery);

            if (sketchesForEst.isEmpty()){
                est_cov.add(new Tuple2<>("0",0F));
                return new Estimation(rq, est_cov, Integer.toString(rq.getUID())+ "," + extractedQueryKey + "," + rangesStr);    // Estimation is simply zero
            }

            for (Tuple2<Synopsis, Float> sk_cov : sketchesForEst){
                Synopsis sk = sk_cov.f0;
                Object est = sk.estimate(extractedQueryKey);
                est_cov.add(new Tuple2<>(est, sk_cov.f1));
            }

            return new Estimation(rq, est_cov, Integer.toString(rq.getUID())+ "," + extractedQueryKey + "," + rangesStr);
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

    private String getStringFromRanges(ArrayList<int[]> rangesToQuery) {
        return rangesToQuery.stream()
                .map(Arrays::toString)
                .reduce((a, b) -> a + ", " + b)
                .orElse("[]");
    }

    /**
     * Finds the sketches that correspond to the specified ranges
     * @param rangesToQuery The ArrayList of ranges in which to search for synopses
     * @return An ArrayList of the synopses that correspond to the given ranges, accompanied by their respective coverage.
     *          If no sketch correspond to specified ranges, or no sketch in these ranges has been initialized, the arrayList will be empty.
     */
    private ArrayList<Tuple2<Synopsis, Float>> findSketchesInRanges(ArrayList<int[]> rangesToQuery) {
        ArrayList<Tuple2<Synopsis, Float>> sketches = new ArrayList<>();
        for (int[] r: rangesToQuery){
            if (r.length != 4)
                continue;   //only ranges in the form x1, y1, x2, y2 are valid
            ArrayList<Dyadic2D> dyadicIntervals = getDyadicIntervals(r[0], r[1], r[2], r[3]);
            for (Dyadic2D di : dyadicIntervals) {
                di.x1--;
                di.x2--;
                di.y1--;
                di.y2--;

                ArrayList<Tuple2<Synopsis, Float>> sketchesFromDi =  getSketchesFromDyadicInterval(di);
                if (!sketchesFromDi.isEmpty())
                    sketches.addAll(sketchesFromDi);
            }
        }
        return sketches;
    }

    /**
     * Given a dyadic interval, finds the sketch(es) that correspond to it
     * @param di The dyadic interval from which the sketches are wanted
     * @return ArrayList containing tuples of synopsis accompanied by their coverage or
     * an empty arraylist if no sketches correspond to it
     */
    private ArrayList<Tuple2<Synopsis, Float>> getSketchesFromDyadicInterval(Dyadic2D di) {
        ArrayList<Tuple2<Synopsis, Float>> sketches = new ArrayList<>();
        String key = getKeyFromDims(n / (di.x2 - di.x1 + 1), n / (di.y2 - di.y1 + 1));
        Synopsis[][] grid = grids.get(key);
        if (grid == null) {   //grid has been dropped, break down dyadic interval of this grid
            ArrayList<Tuple2<Synopsis, Float>> brokenDownSketches = findSketchesForDeletedGrid(di);
            if (!brokenDownSketches.isEmpty()){
                sketches.addAll(brokenDownSketches);
            }
        }
        else{
            int x_cell = di.x1/(di.x2-di.x1+1);
            int y_cell = di.y1/(di.y2-di.y1+1);
            if(grid[x_cell][y_cell] != null){
                //the required sketch of this grid has been initialized
                sketches.add(new Tuple2<>(grid[x_cell][y_cell], di.coverage));
            }
        }
        return sketches;
    }

    /**
     * Breaks down the specified dyadic interval, to find the sketch(es) that compose it
     * @param di The dyadic interval to break down
     * @return ArrayList containing tuples of synopsis accompanied by their coverage or
     *      * an empty arraylist if no sketches correspond to it
     */
    private ArrayList<Tuple2<Synopsis, Float>> findSketchesForDeletedGrid(Dyadic2D di) {
        ArrayList<Tuple2<Synopsis, Float>> sketches = new ArrayList<>();
        Dyadic2D di1 = new Dyadic2D(di);
        Dyadic2D di2 = new Dyadic2D(di);

        int xDim = di.x2 - di.x1 + 1;
        int yDim = di.y2 - di.y1 + 1;
        // Nothing to break, return
        if (xDim == resolution && yDim == resolution) {
            return sketches;
            // Otherwise break largest dimension
        } else if (xDim >= yDim) {
            di1.x2 = di1.x1 + (xDim / 2) - 1;
            di2.x1 = di2.x1 + (xDim / 2);
        } else {
            di1.y2 = di1.y1 + (yDim / 2) - 1;
            di2.y1 = di2.y1 + (yDim / 2);
        }
        ArrayList<Tuple2<Synopsis, Float>> sketchesFromDi1 = getSketchesFromDyadicInterval(di1);
        if (!sketchesFromDi1.isEmpty()){
            sketches.addAll(sketchesFromDi1);
        }
        ArrayList<Tuple2<Synopsis, Float>> sketchesFromDi2 = getSketchesFromDyadicInterval(di2);
        if (!sketchesFromDi2.isEmpty()){
            sketches.addAll(sketchesFromDi2);
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
                Dyadic1D partial = new Dyadic1D(base);
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
        public Dyadic2D(@NotNull Dyadic2D di){
            this.x1 = di.x1;
            this.y1 = di.y1;
            this.x2 = di.x2;
            this.y2 = di.y2;
            this.coverage = di.coverage;
        }

        float coverage;
    }

    static class Dyadic1D {
        int start, end;
        float coverage;

        public Dyadic1D(@NotNull Dyadic1D di){
            this.start = di.start;
            this.end = di.end;
            this.coverage = di.coverage;
        }

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
