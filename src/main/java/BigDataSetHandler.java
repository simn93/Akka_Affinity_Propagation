/**
 * Class for triplet list into compressed byte matrix conversion
 *
 * @author Simone Schirinzi
 */
import java.io.*;
import java.nio.ByteBuffer;
import java.util.zip.*;

public class BigDataSetHandler {
    private static final String inputTripleteSimilarityRow = "C://Users/Simo/Dropbox/Università/Affinity Propagation/dataset/TravelRoutingMatrix.txt";
    private static final String inputTripleteSimilarityCol = "C://Users/Simo/Dropbox/Università/Affinity Propagation/dataset/TravelRoutingMatrixT.txt";
    private static final String inputSinglePreference = "C://Users/Simo/Dropbox/Università/Affinity Propagation/dataset/TravelRoutingPreferences.txt";
    private static final String outputFile = "C://Users/Simo/Dropbox/Università/Affinity Propagation/dataset/travelling.zip";
    private static final String outputFileT = "C://Users/Simo/Dropbox/Università/Affinity Propagation/dataset/travellingT.zip";

    private static final int size = 456;

    public static void main(String[] args){
        try(BufferedReader rowReader = new BufferedReader( new InputStreamReader( new FileInputStream(inputTripleteSimilarityRow), "UTF-8"));
            BufferedReader colReader = new BufferedReader( new InputStreamReader( new FileInputStream(inputTripleteSimilarityCol), "UTF-8"));
            BufferedReader rowPrefReader = new BufferedReader( new InputStreamReader( new FileInputStream(inputSinglePreference), "UTF-8"));
            BufferedReader colPrefReader = new BufferedReader( new InputStreamReader( new FileInputStream(inputSinglePreference), "UTF-8"));
            ZipOutputStream rowOut = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile)));
            ZipOutputStream colOut = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(outputFileT)))) {

            ByteBuffer lineV = ByteBuffer.allocate(size * Double.BYTES);;

            int prevP = 0, currP=-1;
            for(int i=0; i<size; i++){
                rowOut.putNextEntry(new ZipEntry(i+".line"));
                colOut.putNextEntry(new ZipEntry(i+".line"));

                for(int j = 0; j < size; j++)lineV.putDouble(j*Double.BYTES,0.0);
                lineV = readLine(i,rowReader,rowPrefReader,lineV);
                rowOut.write(lineV.array());

                for(int j = 0; j < size; j++)lineV.putDouble(j*Double.BYTES,0.0);
                lineV = readCol(i,colReader,colPrefReader,lineV);
                colOut.write(lineV.array());

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

    private static ByteBuffer readLine(int i, BufferedReader reader, BufferedReader prefReader, ByteBuffer v) throws IOException {
        String[] split = reader.readLine().split(" ");
        String[] firstSplit = split[0].split(",");
        assert (Integer.parseInt(firstSplit[0]) == i);

        int index;
        double value;
        String[] otherSplit;

        index = (Integer.parseInt(firstSplit[2]));
        value = Double.parseDouble(firstSplit[3]);
        v.putDouble(Double.BYTES * (index-1), value);

        for(int j=1; j < split.length; j++){
            otherSplit = split[j].split(",");

            index = Integer.parseInt(otherSplit[1]);
            value = Double.parseDouble(otherSplit[2]);
            v.putDouble(Double.BYTES * (index-1),value);
        }

        v.putDouble(Double.BYTES * i, Double.parseDouble(prefReader.readLine()));
        return v;
    }

    private static ByteBuffer readCol(int i, BufferedReader reader, BufferedReader prefReader, ByteBuffer v) throws IOException {
        String[] split = reader.readLine().split(" ");
        String[] firstSplit = split[0].split(",");
        assert (Integer.parseInt(firstSplit[0]) == i);

        int index;
        double value;
        String[] otherSplit;

        index = (Integer.parseInt(firstSplit[1]));
        value = Double.parseDouble(firstSplit[3]);
        v.putDouble(Double.BYTES * (index-1), value);

        for(int j=1; j < split.length; j++){
            otherSplit = split[j].split(",");

            index = Integer.parseInt(otherSplit[0]);
            value = Double.parseDouble(otherSplit[2]);
            v.putDouble(Double.BYTES * (index-1), value);
        }

        v.putDouble(Double.BYTES * i, Double.parseDouble(prefReader.readLine()));
        return v;
    }
}
