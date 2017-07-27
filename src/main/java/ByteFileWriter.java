import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ByteFileWriter {
    public static void main(String[] args){
        String inputMatrixRow = "C://Users/Simone/Dropbox/Università/Affinity Propagation/dataset/exons.txt";
        String inputMatrixCol = "C://Users/Simone/Dropbox/Università/Affinity Propagation/dataset/exonsT.txt";
        String outputFile = "C://Users/Simone/Dropbox/Università/Affinity Propagation/dataset/exonsLineHashByte.zip";
        String outputFileT = "C://Users/Simone/Dropbox/Università/Affinity Propagation/dataset/exonsLineHashByteT.zip";

        int i,j;
        try(BufferedReader rowReader = new BufferedReader( new InputStreamReader( new FileInputStream(inputMatrixRow), "UTF-8"));
            BufferedReader colReader = new BufferedReader( new InputStreamReader( new FileInputStream(inputMatrixCol), "UTF-8"));
            ZipOutputStream rowOut = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile)));
            ZipOutputStream colOut = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(outputFileT)))) {

            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + Double.BYTES);
            double value;
            String line;
            String[] split;

            i = 0;
            while ((line = rowReader.readLine()) != null){
                rowOut.putNextEntry(new ZipEntry(i+".line"));
                i++;

                split = line.split(",");
                for(j=0;j<split.length;j++){
                    value = Double.parseDouble(split[j]);
                    if(value != 0) {
                        buffer.putInt(j);
                        buffer.putDouble(value);
                        buffer.flip();

                        rowOut.write(buffer.array());
                    }
                }

                rowOut.closeEntry();
            }

            i = 0;
            while ((line = colReader.readLine()) != null){
                colOut.putNextEntry(new ZipEntry(i+".line"));
                i++;

                split = line.split(",");
                for(j=0;j<split.length;j++){
                    value = Double.parseDouble(split[j]);
                    if(value != 0) {
                        buffer.putInt(j);
                        buffer.putDouble(value);
                        buffer.flip();

                        colOut.write(buffer.array());
                    }
                }

                colOut.closeEntry();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}