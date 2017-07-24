/**
 * Class for triplet list into compressed byte matrix conversion
 *
 * @author Simone Schirinzi
 */
import it.unimi.dsi.fastutil.Hash;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.*;

public class BigDataSetHandler {
    private static final String inputTripleteSimilarityRow = "C://Users/Simone/Dropbox/Università/Affinity Propagation/dataset/TravelRoutingMatrix.txt";
    private static final String inputTripleteSimilarityCol = "C://Users/Simone/Dropbox/Università/Affinity Propagation/dataset/TravelRoutingMatrixT.txt";
    private static final String inputSinglePreference = "C://Users/Simone/Dropbox/Università/Affinity Propagation/dataset/TravelRoutingPreferences.txt";
    private static final String outputFile = "C://Users/Simone/Dropbox/Università/Affinity Propagation/dataset/travelling.zip";
    private static final String outputFileT = "C://Users/Simone/Dropbox/Università/Affinity Propagation/dataset/travellingT.zip";

    private static final int size = 456;
    private static HashMap<Integer,Double> map;

    public static void main(String[] args){
        try(BufferedReader rowReader = new BufferedReader( new InputStreamReader( new FileInputStream(inputTripleteSimilarityRow), "UTF-8"));
            BufferedReader colReader = new BufferedReader( new InputStreamReader( new FileInputStream(inputTripleteSimilarityCol), "UTF-8"));
            BufferedReader rowPrefReader = new BufferedReader( new InputStreamReader( new FileInputStream(inputSinglePreference), "UTF-8"));
            BufferedReader colPrefReader = new BufferedReader( new InputStreamReader( new FileInputStream(inputSinglePreference), "UTF-8"));
            ZipOutputStream rowOut = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile)));
            ZipOutputStream colOut = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(outputFileT)))) {

            int prevP = 0, currP;
            for(int i=0; i<size; i++){
                rowOut.putNextEntry(new ZipEntry(i+".line"));
                colOut.putNextEntry(new ZipEntry(i+".line"));

                map = new HashMap<>();
                readLine(i,rowReader,rowPrefReader,map);
                rowOut.write(HashMapToByteArray(map));

                map = new HashMap<>();
                readCol(i,colReader,colPrefReader,map);
                colOut.write(HashMapToByteArray(map));

                currP = Math.floorDiv(i*100,size);
                if(currP != prevP){
                    prevP = currP;
                    System.out.println(prevP + "%");
                }

                rowOut.closeEntry();
                colOut.closeEntry();
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void readLine(int i, BufferedReader reader, BufferedReader prefReader, HashMap<Integer,Double> map) throws IOException {
        String[] split = reader.readLine().split(" ");
        String[] firstSplit = split[0].split(",");
        assert (Integer.parseInt(firstSplit[0]) == i);

        int index;
        double value;
        String[] otherSplit;

        index = (Integer.parseInt(firstSplit[2]));
        value = Double.parseDouble(firstSplit[3]);
        map.put(index-1,value);

        for(int j=1; j < split.length; j++){
            otherSplit = split[j].split(",");

            index = Integer.parseInt(otherSplit[1]);
            value = Double.parseDouble(otherSplit[2]);
            map.put(index-1,value);
        }

        map.put(i, Double.parseDouble(prefReader.readLine()));
    }

    private static void readCol(int i, BufferedReader reader, BufferedReader prefReader, HashMap<Integer,Double> map) throws IOException {
        String[] split = reader.readLine().split(" ");
        String[] firstSplit = split[0].split(",");
        assert (Integer.parseInt(firstSplit[0]) == i);

        int index;
        double value;
        String[] otherSplit;

        index = (Integer.parseInt(firstSplit[1]));
        value = Double.parseDouble(firstSplit[3]);
        map.put(index-1, value);

        for(int j=1; j < split.length; j++){
            otherSplit = split[j].split(",");

            index = Integer.parseInt(otherSplit[0]);
            value = Double.parseDouble(otherSplit[2]);
            map.put(index-1, value);
        }

        map.put(i, Double.parseDouble(prefReader.readLine()));
    }

    private static byte[] HashMapToByteArray(HashMap<Integer,Double> map){
        ByteBuffer buffer = ByteBuffer.allocate((Integer.BYTES + Double.BYTES) * map.size());
        for(Map.Entry<Integer,Double> e : map.entrySet()){
            buffer.putInt(e.getKey());
            buffer.putDouble(e.getValue());
        }
        return buffer.array();
    }
}
