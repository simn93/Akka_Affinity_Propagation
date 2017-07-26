import scala.util.control.Exception;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ByteFileWriter {
    public static void main(String[] args){
        String inputMatrixRow = "C://Users/Simone/Dropbox/Università/Affinity Propagation/dataset/exons_10k.txt";
        String inputMatrixCol = "C://Users/Simone/Dropbox/Università/Affinity Propagation/dataset/exonsT_10k.txt";
        String outputFile = "C://Users/Simone/Dropbox/Università/Affinity Propagation/dataset/exonsByte.zip";
        String outputFileT = "C://Users/Simone/Dropbox/Università/Affinity Propagation/dataset/exonsByteT.zip";

        int size = 10001;

        try(BufferedReader rowReader = new BufferedReader( new InputStreamReader( new FileInputStream(inputMatrixRow), "UTF-8"));
            BufferedReader colReader = new BufferedReader( new InputStreamReader( new FileInputStream(inputMatrixCol), "UTF-8"));
            ZipOutputStream rowOut = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile)));
            ZipOutputStream colOut = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(outputFileT)))) {

            rowOut.putNextEntry(new ZipEntry("matrix"));
            colOut.putNextEntry(new ZipEntry("matrix"));

            ByteBuffer buffer;
            String line;
            String[] split;

            buffer = ByteBuffer.allocate(size*Double.BYTES);

            while ((line = rowReader.readLine()) != null){
                split = line.split(",");
                for(String s : split)buffer.putDouble(Double.parseDouble(s));
                buffer.flip();
                rowOut.write(buffer.array());
            }

            while ((line = colReader.readLine()) != null){
                buffer = ByteBuffer.allocate(size*Double.BYTES);
                split = line.split(",");
                for(String s : split)buffer.putDouble(Double.parseDouble(s));
                buffer.flip();
                colOut.write(buffer.array());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
