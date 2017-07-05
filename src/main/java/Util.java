import akka.actor.ActorRef;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 * Created by Simo on 10/06/2017.
 */
class Util {
    //The IEEE 754 format has one bit reserved for the sign
    // and the remaining bits representing the magnitude.
    // This means that it is "symmetrical" around origo
    // (as opposed to the Integer values, which have one more negative value).
    // Thus the minimum value is simply the same as the maximum value,
    // with the sign-bit changed, so yes,
    // -Double.MAX_VALUE is the smallest possible actual number you can represent with a double.

    //We are more interested to keep this value constant
    public static double min_double = Double.NEGATIVE_INFINITY;
    public static boolean isMinDouble(double value){return Double.isInfinite(value);}

    public static double[][] buildGraph(String similarity_file, String similarity_regex, String preference_file, boolean median_preference, double sigma){
        //File Read
        ArrayList<double[]> input = new ArrayList<>();
        try(BufferedReader reader = new BufferedReader( new InputStreamReader( new FileInputStream(similarity_file), "UTF-8"))) {
            String line;
            while ((line = reader.readLine()) != null){
                String[] splitted = line.split(similarity_regex);

                double[] element = new double[3];
                element[0] = Double.parseDouble(splitted[0]);
                element[1] = Double.parseDouble(splitted[1]);
                element[2] = Double.parseDouble(splitted[2]);
                input.add(element);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        //find size
        int size = -1;
        for(double[] e : input){
            size = Math.max(size,(int) Math.round(e[0]));
            size = Math.max(size,(int) Math.round(e[1]));
        }

        //Graph build
        double[][] Graph = new double[size][size];

        //Graph preload
        for(int i = 0; i < size; i++){
            for(int j = 0; j < size; j++){
                Graph[i][j] = min_double;
            }
        }

        //Preference load
        if(median_preference) {
            double median = GetMedian(input);
            for(int i = 0; i < size; i++) Graph[i][i] = median;
        } else {
            try(BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(preference_file),"UTF-8"))) {
                String line;
                int i = 0;
                while ((line = reader.readLine()) != null){
                    Graph[i][i] = Double.parseDouble(line);
                    i++;
                }
                assert (i == size);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        //Graph load
        for(double[] e : input){
            int i = (int) Math.round(e[0]);
            int j = (int) Math.round(e[1]);
            Graph[i-1][j-1] = e[2] + getNoise(sigma);
        }

        return Graph;
    }

    public static double[][] buildGraph(String matrix_file, double preference, boolean use_preference, boolean zeroAsInf, double sigma){
        //file read
        ArrayList<double[]> input = new ArrayList<>();
        int size = -1;
        boolean first = true;
        double[][] Graph = new double[0][0];
        int row = 0;

        try(BufferedReader reader = new BufferedReader( new InputStreamReader( new FileInputStream(matrix_file), "UTF-8"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] split = line.split(",");
                if(first){
                    size = split.length;
                    Graph = new double[size][size];
                    first = false;
                }

                for(int col = 0; col < size; col++){
                    Graph[row][col] = Double.parseDouble(split[col]) + getNoise(sigma);
                    if(zeroAsInf && Graph[row][col] == 0.0) Graph[row][col] = Util.min_double;
                    if(use_preference && row == col) Graph[row][col] = preference;
                }
                row++;
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        return Graph;
    }

    public static double[][] buildGraphRandom(int size){
        double[][] graph = new double[size][size];

        for(int r = 0; r < size; r++){
            for(int c = 0; c < size; c++){
                double rand = -Math.random() * 1000;
                graph[r][c] = rand < -300 ? rand : min_double;
                if(r==c)graph[r][c] = rand;
            }
        }

        return graph;
    }

    private static double GetMedian(ArrayList<double[]> input){
        ArrayList<Double> median = new ArrayList<>();
        for(double[] d : input) median.add(d[2]);

        // si ordinano gli n dati in ordine crescente (o decrescente);
        median.sort((o1, o2) -> (int)(o1-o2));

        // se il numero n di dati è pari
        if(median.size() % 2 == 0){
            //la mediana è stimata utilizzando i due valori che occupano le posizione (n/2) e ((n/2)+1)
            return  ((median.get((median.size()-2)/2)) + median.get(((median.size()-2)/2)+1))/2;
        } else {
            //la mediana corrisponde al valore centrale, ovvero al valore che occupa la posizione (n+1)/2
            return median.get((median.size()-1)/2);
        }
        //Sottraggo due per compensare il fatto che si conti a partire da 0
    }

    private static double getNoise(double sigma){
        double random = Math.random();
        random -= 0.5;
        random *= sigma;
        return random;
    }

    public static void printSimilarity(double[][] Graph, int size){
        StringBuilder builder = new StringBuilder();
        builder.append("Similarity Matrix:");
        builder.append("\n");
        for(int i = 0; i < size; i++){
            for(int j = 0; j < size; j++){
                builder.append(Graph[i][j]);
                builder.append(" ");
            }
            builder.append("\n");
        }

        System.out.println(builder.toString());
    }

    public static boolean contains(ActorRef[] array, ActorRef sender){
        for(ActorRef element : array){
            if(element != null && element.compareTo(sender) == 0) return true;
        }
        return false;
    }
}
