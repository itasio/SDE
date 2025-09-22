package infore.SDE.Experiments;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.IOException;
import java.util.*;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;

public class example {

    public static void main(String[] args) throws IOException {


//        String userSchema = "{"
//                + "\"type\":\"record\","
//                + "\"name\":\"Employee\","
//                + "\"fields\":["
//                + "{\"name\":\"id\", \"type\":\"int\"},"
//                + "{\"name\":\"name\", \"type\":\"string\"},"
//                + "{\"name\":\"salary\", \"type\":\"double\"}"
//                + "]"
//                + "}";


//        String dataSchema = "{"
//                +"\"type\":\"record\","
//                +"\"name\":\"Datapoint\","
//                +"\"fields\":["
//                +    "{\"name\":\"DataSetkey\", \"type\":\"string\"},"
//                +    "{\"name\":\"streamID\", \"type\":\"string\"},"
//                +    "{\"name\":\"values\","
//                +    "type\": {\"type\":\"record\","
//                +            "name\":\"Values\","
//                +            "fields\":"
//                +                    "["
//                +                    "{\"name\":\"ip\", \"type\":\"int\"},"
//                +                    "{\"name\":\"dataParam\", \"type\":"
//                +                        "{"
//                +                            "type\":\"array\","
//                +                                "items\":\"int"
//                +                        "}"
//                +                    "}"
//                +                    "]"
//                +            "}"
//                +    "}"
//               + "]"
//               + "}";


        String cwd = System.getProperty("user.dir");
        System.out.println("Current working directory: " + cwd);
        Schema dataSchema = new Schema.Parser().parse(new File("src/main/avro/dataSchema.avsc"));

        File file = new File("src/main/avro/datapoints.avro");
        
        createAvroFile(dataSchema, file);

        int numOfRows = 10;
        previewAvroFile(dataSchema, file, numOfRows);

/*
        int x = 3;
        int y = 1;

        int n = 4;
        int xMax = n;
        int yMax = n;

        int levels = BigIntegerMath.log2(BigInteger.valueOf(n), RoundingMode.FLOOR) + 1;
        System.out.println("Levels: "+ levels);
        ArrayList<Tuple2<Integer, Integer>> arr = new ArrayList<>();

        for (int i = 0; i < levels; i++) {
            for (int j = 0; j < levels; j++) {
                int xDim = (int)Math.pow(2, i);
                int yDim = (int)Math.pow(2, j);
                arr.add(new Tuple2<>(xDim,yDim));
            }
        }

        for (Tuple2<Integer, Integer> tup : arr){
            int xDim = tup.f0;
            int yDim = tup.f1;



            int xCell = (int) floor(x * ((double) xDim / xMax));
            int yCell = (int) floor(y * ((double) yDim / yMax));

            System.out.println("X_DIMs x Y_DIMS: " + xDim + "x" + yDim + " Cell: " + xCell + ", " +yCell);

            for (int j = yDim-1; j >= 0; j--) {
                for (int i = 0; i < xDim; i++) {
                    if (i == xCell && j == yCell){
                        System.out.print("■");
                    }else {
                        System.out.print("□");
                    }
//                    System.out.print(" ");
                }
                System.out.println();
            }

        }
*/

    }

    private static void previewAvroFile(Schema schema, File file, int numOfRows) throws IOException {
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader);

        GenericRecord record = null;
        int ctr = 0;
        while (dataFileReader.hasNext() && ctr < numOfRows) {
            record = dataFileReader.next(record);
            System.out.println(record);
            ctr++;
        }
        dataFileReader.close();
    }

    private static void createAvroFile(Schema schema, File file) throws IOException {

        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);

        dataFileWriter.setCodec(org.apache.avro.file.CodecFactory.deflateCodec(6));
        dataFileWriter.create(schema, file);

        int i = 0;
        int min = 10;
        int max = 2000;
        int totalSketches = 1;
        int dataCount = 167000;
        int dataSent = 0;
        Random r = new Random();
        while (i < dataCount) {
            if (i % 1_000 == 0){
                System.out.println("Sent data: "+ i);
            }
            int randomValue = 10 + r.nextInt(max - min);
            int randomkey = (i + 1) * 2;

            int rangeBoundary_X = 2;
            int rangeBoundary_Y = 3;

            for (int x_cell = 0; x_cell <= rangeBoundary_X; x_cell++) {
                for (int y_cell = 0; y_cell <= rangeBoundary_Y; y_cell++) {

                    int randomSketch = r.nextInt(totalSketches) + 1;

                    GenericRecord data = new GenericData.Record(schema);

                    data.put("DataSetkey", "spatial" + randomSketch);
                    data.put("streamID", "ip");
                    data.put("key", randomkey);
                    data.put("xCell", x_cell);
                    data.put("yCell", y_cell);
                    data.put("value", randomValue);
                    dataFileWriter.append(data);
                    dataSent++;
                }

            }
            i++;
        }
        System.out.println("Total data sent: " + dataSent);
        dataFileWriter.close();
    }

}
