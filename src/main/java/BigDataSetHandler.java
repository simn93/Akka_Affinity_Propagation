/**
 * Class for triplet list into compressed byte matrix conversion
 *
 * @author Simone Schirinzi
 */
import java.io.*;
import java.nio.ByteBuffer;
import java.util.zip.*;

public class BigDataSetHandler {
    private static final String inputTripleteSimilarityRow = "C://Users/Simone/Downloads/TravelRoutingSimilarities.txt";
    private static final String inputTripleteSimilarityCol = "C://Users/Simone/Downloads/TravelRoutingSimilaritiesT.txt";
    private static final String inputSinglePreference = "C://Users/Simone/Downloads/TravelRoutingPreferences.txt";
    private static final String outputFile = "C://Users/Simone/Downloads/travelling.zip";
    private static final String outputFileT = "C://Users/Simone/Downloads/travellingT.zip";

    private static final int size = 456;

    private static int unreadIndex = -1;
    private static double unreadValue = -1;

    public static void main(String[] args){
        try(BufferedReader rowReader = new BufferedReader( new InputStreamReader( new FileInputStream(inputTripleteSimilarityRow), "UTF-8"));
            BufferedReader colReader = new BufferedReader( new InputStreamReader( new FileInputStream(inputTripleteSimilarityCol), "UTF-8"));
            BufferedReader rowPrefReader = new BufferedReader( new InputStreamReader( new FileInputStream(inputSinglePreference), "UTF-8"));
            BufferedReader colPrefReader = new BufferedReader( new InputStreamReader( new FileInputStream(inputSinglePreference), "UTF-8"));
            ZipOutputStream rowOut = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile)));
            ZipOutputStream colOut = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(outputFileT)))) {

            ByteBuffer lineV;

            rowOut.putNextEntry(new ZipEntry("matrix.dat"));
            colOut.putNextEntry(new ZipEntry("matrix.dat"));

            int prevP = 0, currP=-1;
            for(int i=0; i<size; i++){
                lineV = ByteBuffer.allocate(size*Double.BYTES);
                for(int j = 0; j < size; j++)lineV.putDouble(0.0);
                lineV = readLine(rowReader,0,i,rowPrefReader,lineV);

                rowOut.write(lineV.array());

                lineV = ByteBuffer.allocate(size*Double.BYTES);
                for(int j = 0; j < size; j++)lineV.putDouble(0.0);
                lineV = readLine(colReader,1,i,colPrefReader,lineV);

                colOut.write(lineV.array());

                currP = Math.floorDiv(i*100,size);
                if(currP != prevP){
                    prevP = currP;
                    System.out.println(prevP + "%");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    // index: 0 -> row , 1 -> col
    private static ByteBuffer readLine(BufferedReader reader, int index, int rowNum, BufferedReader prefReader, ByteBuffer v) throws IOException {
        boolean end = false;

        if(unreadIndex != -1)v.putDouble(unreadIndex*Double.BYTES, unreadValue);

        String line;
        String[] split;
        while ((line = reader.readLine()) != null && !end){
            split = line.split(",");
            if(Integer.parseInt(split[index]) != rowNum){
                end = true;
                unreadIndex = Integer.parseInt(split[(index+1)%2]);
                unreadValue = Double.parseDouble(split[2]);
            } else {
                v.putDouble((Integer.parseInt(split[(index+1)%2]) - 1) * Double.BYTES, Double.parseDouble(split[2]));
            }
        }

        if((line = prefReader.readLine()) != null){
            v.putDouble(rowNum * Double.BYTES, Double.parseDouble(line));
        }

        return v;
    }
}
