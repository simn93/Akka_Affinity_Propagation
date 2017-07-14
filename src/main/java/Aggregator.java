import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Class for cluster computing
 *
 * @author Simone Schirinzi
 */
class Aggregator extends AbstractActor {
    /**
     * Position of file of matrix memorized by lines
     */
    private final String lineMatrix;

    /**
     * number of nodes
     */
    @SuppressWarnings("CanBeFinal")
    private int size;

    /**
     * Record of received values
     * It only contains those received for which
     * the cluster has not yet been calculated
     */
    private final HashMap<Long,double[]> values;

    /**
     * Reference to the last cluster calculated
     * other than the ones previously calculated.
     * Useful to check if a new cluster emerged.
     */
    private int[] previousCluster;

    /**
     * Indicates which iteration refers to the cluster
     * stored in the variable previousCluster.
     */
    private long previousClusterIteration;

    /**
     * Reference list of nodes.
     * It is used at termination of the algorithm.
     * It is used to send all the nodes
     * at the same time a termination message.
     */
    private ActorRef[] nodes;

    /**
     * Collection of similarity optimized.
     * Each row contains the indexes of the nodes
     * with which the row number index node has not infinite similarity.
     * These indexes are sorted in descending order
     * with respect to the similarity it has
     * with the index number node of the row.
     * Ex. M[3][0] = 2 <=> s(3,0) = max {s(3,i)} , 0 <= i < size
     */
    private int[][] compactSimilarity;

    /**
     * if true : print all cluster
     */
    private final boolean verbose = false;

    /**
     * if true : show visual graph
     */
    private final boolean showGraph = false;

    /**
     * Timer for time control.
     */
    @SuppressWarnings("CanBeFinal")
    private Timer timer;

    /**
     * Akka logger
     * Logging in Akka is not tied to a specific logging backend.
     * By default log messages are printed to STDOUT,
     * but you can plug-in a SLF4J logger or your own logger.
     * Logging is performed asynchronously
     * to ensure that logging has minimal performance impact.
     * Logging generally means IO and locks,
     * which can slow down the operations of your code if it was performed synchronously.
     */
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    static Props props(String lineMatrix, int size) {
        return Props.create(Aggregator.class, () -> new Aggregator(lineMatrix,size));
    }

    /**
     * Create the class
     *
     * Start the timer
     *
     * @param size of nodes
     */
    private Aggregator(String lineMatrix, int size){
        this.lineMatrix = lineMatrix;
        this.size = size;
        this.values = new HashMap<>();
        this.compactSimilarity = new int[size][];

        timer = new Timer();
        timer.start();

        buildSimilarity();
    }

    /**
     * Get a value from a given node in reference to a certain iteration.
     * {value, iteration, senderID} : values[iteration][senderID] = value.
     *
     * values[iteration] is a size+1 sized vector :
     * values[iteration][size] contains The number of values received by the nodes for that iteration
     *
     * Once I get values from all nodes for a given iteration I can calculate the cluster.
     * Value = a(i,i) + r(i,i) for node i.
     * value greater then 0 identify node i as an exemplar
     *
     * The message passing procedure may be terminated
     * after the local decisions stay constant
     * for some number of iterations
     * /**
     * Receiving active handler at system termination.
     * It tracks the nodes from which it receives requests
     * in a specially crafted vector.
     * Once he get references to all active nodes,
     * send them a poisoned message so they can terminate them.
     *
     * Akka poisonPill
     * To kill an actor You can also send the akka.actor.PoisonPill message,
     * which will stop the actor when the message is processed.
     * PoisonPill is enqueued as ordinary messages
     * and will be handled after messages
     * that were already queued in the mailbox.
     *
     * @see Value
     * @return receive handler
     */
    @Override
    public Receive createReceive() {
        return receiveBuilder()
        .match(Value.class, value -> {
            /* HashMap vector creation */
            if(! values.containsKey(value.iteration))
                values.put(value.iteration,new double[size+1]);

            /* save the value into the vector refereed to iteration value */
            double[] current = values.get(value.iteration);
            current[value.sender] = value.value;

            /* increase the number of value received refereed to iteration value */
            current[size] += 1;

            /* can compute the cluster */
            if(current[size] == size){
                ArrayList<Integer> exemplars = new ArrayList<>();
                for(int i = 0; i < size; i++) if(current[i] > 0 && !exemplars.contains(i)) exemplars.add(i);

                int[] e = new int[exemplars.size()];
                for(int i = 0; i < exemplars.size(); i++) e[i] = exemplars.get(i);

                if(!isChanged(buildCluster(e,verbose,showGraph), value.iteration)
                        && value.iteration - previousClusterIteration > Constant.enoughIterations) {
                    timer.stop();
                    log.info("Job done U_U after " + previousClusterIteration + " iterations and " + timer);

                    for(ActorRef node : nodes) node.tell(akka.actor.PoisonPill.getInstance(),ActorRef.noSender());
                    context().system().terminate();
                }
                values.remove(value.iteration);
            }
        })
        .match(Neighbors.class, msg -> this.nodes = msg.array)
        .build();
    }

    /**
     * Creating the cluster
     *
     * It is said that a node "n" belongs to a cluster with reference to an exemplar "e"
     * if forall of the other exemplars emerged "o" : s(n,e) greater than s(n,o)
     *
     * @param exemplars vector
     * @param verbose if True : print all cluster
     * @param showGraph if True : show visual graph
     * @return cluster : i belong to cluster[i]
     */
    private int[] buildCluster(int[] exemplars, boolean verbose, boolean showGraph){
        int[] cluster = new int[size];
        int index;

        for(int i = 0; i < size; i++){
            cluster[i] = -1;
            for(int k : compactSimilarity[i]) if((index = java.util.Arrays.binarySearch(exemplars, k)) >= 0){
                cluster[i] = exemplars[index]; break;
            }
        }

        if(verbose) for(int i = 0; i < size; i++) log.info(i + " belong to " + cluster[i]);
        if(showGraph) (new VisualGraph(exemplars,cluster)).show(800,800);
        return cluster;
    }

    /**
     * Checks if the calculated cluster is different from the last different calculated previously
     * @param cluster to check if is changed
     * @param iteration to which it refers
     * @return  True if cluster != previous cluster
     *          False if cluster == previous cluster
     */
    private boolean isChanged(int[] cluster, long iteration){
        /* I'm calculating a cluster for an old iteration */
        if(previousClusterIteration > iteration) return false;

        /* Computing the first cluster */
        if(previousCluster == null){
            previousCluster = cluster;
            previousClusterIteration = iteration;
            return true;
        }

        /* Is changed */
        for(int i = 0; i < size; i++){
            if(previousCluster[i] != cluster[i]){
                previousCluster = cluster;
                previousClusterIteration = iteration;
                return true;
            }
        }

        /* Is not changed */
        return false;
    }

    /**
     * Read from file and memorized a property structure
     * for fast cluster build.
     */
    private void buildSimilarity() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(lineMatrix), "UTF-8"))) {
            double[] line_similarity = new double[size];
            for (int i = 0; i < size; i++) {
                line_similarity = Util.stringToVector(reader.readLine(), line_similarity);
                TreeMap<Double, Integer> not_inf_s = new TreeMap<>((o1, o2) -> (new Double(o2 - o1)).intValue());
                for(int j = 0; j < line_similarity.length; j++) if(!Util.isMinDouble(line_similarity[j])) not_inf_s.put(line_similarity[j], j);

                compactSimilarity[i] = new int[not_inf_s.values().size()];
                int j = 0; for(Integer value : not_inf_s.values()){
                    compactSimilarity[i][j] = value;
                    j++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}