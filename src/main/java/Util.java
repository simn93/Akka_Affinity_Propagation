import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Class for file load
 *
 * @author Simone Schirinzi
 */
@SuppressWarnings("DefaultFileTemplate")
class Util {
    /**
     * The IEEE 754 format has one bit reserved for the sign
     * and the remaining bits representing the magnitude.
     * This means that it is "symmetrical" around origo
     * (as opposed to the Integer values, which have one more negative value).
     * Thus the minimum value is simply the same as the maximum value,
     * with the sign-bit changed, so yes,
     * -Double.MAX_VALUE is the smallest possible actual number you can represent with a double.
     *
     * We are more interested to keep this value constant.
     * By setting in this way the similarity between a pair of nodes
     * whose true similarity is unknown,
     * we reserve the possibility of not making communications between that pair,
     * thus optimizing the algorithm.
     */
    public static final double min_double = Double.NEGATIVE_INFINITY;
    public static boolean isMinDouble(double value){return Double.isInfinite(value);}

    /**
     * Build a graph from a file which each line contains {i, j, s(i,j)}
     * @param similarity_file input file location
     * @param similarity_regex Regular expression for the division of each line.
     *                         It is assumed that there are no more than one triple per line.
     * @param preference_file Location of the preference file. Line i contains s(i, i)
     *                        It is assumed that there are no more than one preference per line.
     * @param median_preference If false : preference are loaded from file
     *                          If true : preference are computed locally
     * @param sigma Preference noise's order of magnitude
     * @return similarity graph
     */
    @SuppressWarnings({"SameParameterValue","unused"})
    public static double[][] buildGraph(String similarity_file, String similarity_regex, String preference_file, boolean median_preference, double sigma){
        /* File Read */
        ArrayList<double[]> input = new ArrayList<>();
        try(BufferedReader reader = new BufferedReader( new InputStreamReader( new FileInputStream(similarity_file), "UTF-8"))) {
            String line;
            while ((line = reader.readLine()) != null){
                String[] split = line.split(similarity_regex);
                input.add(new double[]{
                        Double.parseDouble(split[0]), Double.parseDouble(split[1]), Double.parseDouble(split[2])
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        /* find size */
        int size = -1;
        for(double[] e : input)
            size = Math.max(Math.max(size,(int) Math.round(e[0])),(int) Math.round(e[1]));

        /* Graph build
        * May take a lot of memory
        * */
        double[][] Graph = new double[size][size];

        /* Graph preload */
        for(int i = 0; i < size; i++)for(int j = 0; j < size; j++)Graph[i][j] = min_double;

        /* Preference load */
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

        /* Graph load */
        for(double[] e : input){
            int i = (int) Math.round(e[0]);
            int j = (int) Math.round(e[1]);
            Graph[i-1][j-1] = e[2] + getNoise(sigma);
        }

        return Graph;
    }

    /**
     * Build a graph from a file which each line contains {s(i,0),s(i,1),...,s(i,n)}
     * @param matrix_file location of file
     * @param preference constant value of preference
     * @param use_preference If true : forAll s(i,i) = preference
     *                       If false : do_nothing
     * @param zeroAsInf If true : if(s(i,j) == 0) s(i,j) = -INF
     * @param sigma Preference noise's order of magnitude
     * @return similarity graph
     */
    @SuppressWarnings("unused")
    public static double[][] buildGraph(String matrix_file, double preference, boolean use_preference, boolean zeroAsInf, double sigma){
        /* file read */
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

    /**
     * Create a random and for test only graph of size node.
     * 30% of s(i,j) is -INF
     * @param size of graph
     * @return graph
     */
    @SuppressWarnings({"unused", "SameParameterValue"})
    public static double[][] buildGraphRandom(int size){
        double[][] graph = new double[size][size];

        for(int r = 0; r < size; r++) for(int c = 0; c < size; c++){
            double rand = -Math.random() * 1000;
            graph[r][c] = rand < -300 ? rand : min_double;
            if(r==c)graph[r][c] = rand;
        }

        return graph;
    }

    /**
     * You sort the n data in ascending order (or decreasing order)
     * If n number of data is equal
     * The median is estimated using
     * The two values that occupy the positions (n / 2) and ((n / 2) +1)
     * The median corresponds to the central value,
     * Or the value occupying the position (n + 1) / 2
     * I subtract two to compensate for the fact that counts from 0
     *
     * @param input triple of loaded values like {i, j, s(i,j)}
     *              We only use third value
     * @return median value
     */
    private static double GetMedian(ArrayList<double[]> input){
        ArrayList<Double> median = new ArrayList<>();
        for(double[] d : input) median.add(d[2]);

        median.sort((o1, o2) -> (int)(o1-o2));
        if(median.size() % 2 == 0) return ((median.get((median.size()-2)/2)) + median.get(((median.size()-2)/2)+1))/2;
        else return median.get((median.size()-1)/2);
    }

    /**
     * Generate noise
     * @param sigma order of magnitude
     * @return random noise
     */
    private static double getNoise(double sigma){
        double random = Math.random();
        random -= 0.5;
        random *= sigma;
        return random;
    }

    /**
     * Print graph
     * @param Graph to print
     * @param size of graph
     */
    @SuppressWarnings("unused")
    public static void printSimilarity(double[][] Graph, int size){
        StringBuilder builder = new StringBuilder();

        builder.append("Similarity Matrix:");
        builder.append("\n");
        for(int i = 0; i < size; i++){ for(int j = 0; j < size; j++){
                builder.append(Graph[i][j]);
                builder.append(" ");
            }
            builder.append("\n");
        }

        System.out.println(builder.toString());
    }

    /**
     * Converts a matlab matrix string line to double vector
     * @param s string
     * @return double vector represents string
     */
    @SuppressWarnings("unused")
    public static double[] stringToVector(String s){
        String[] split = s.split(",");
        double[] d = new double[split.length];
        for(int i = 0; i < d.length; i++){
            d[i] = Double.parseDouble(split[i]);
            if(d[i]==0) d[i] = min_double;
        }
        return d;
    }

    /**
     * Converts a matlab matrix string line to double vector.
     * Optimize memory allocation
     * @param s string
     * @param d vector of proper size
     * @return double vector represents string
     */
    public static double[] stringToVector(String s, double[] d){
        String[] split = s.split(",");
        for(int i = 0; i < d.length; i++){
            d[i] = Double.parseDouble(split[i]);
            if(d[i]==0) d[i] = min_double;
        }
        return d;
    }

    public static double[] bytesToVector(byte[] v, double[] d) {
        ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
        int k = 0;

        for (int i = 0; i < d.length; i++) {
            buffer.clear();
            for (int j = 0; j < Double.BYTES; j++) {
                buffer.put(v[k]);
                k++;
            }
            buffer.flip();
            d[i] = buffer.getDouble();
            if(d[i]==0)d[i]=Util.min_double;
        }

        return d;
    }
}
