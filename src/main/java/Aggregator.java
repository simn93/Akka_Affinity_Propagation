import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Class for cluster computing
 *
 * @author Simone Schirinzi
 */
class Aggregator extends AbstractActor {
    /**
     * Similarity matrix
     */
    private final double[][] similarity;

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
    private ArrayList<Integer> previousCluster;

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
    private ArrayList<ActorRef> nodes;

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

    static Props props(double[][] similarity, int size) {
        return Props.create(Aggregator.class, () -> new Aggregator(similarity,size));
    }

    /**
     * Create the class
     *
     * Start the timer
     *
     * @param similarity graph
     * @param size of nodes
     */
    private Aggregator(double[][] similarity, int size){
        this.similarity = similarity;
        this.size = size;
        this.values = new HashMap<>();

        timer = new Timer();
        timer.start();
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
                for(int i = 0; i < size; i++)
                    if(current[i] > 0 && !exemplars.contains(i))
                        exemplars.add(i);

                int[] e = new int[exemplars.size()];
                for(int i= 0; i < exemplars.size(); i++) e[i] = exemplars.get(i);

                if(!exemplars.equals(previousCluster)){
                    previousCluster = exemplars;
                    previousClusterIteration = value.iteration;
                    //System.out.println(previousClusterIteration);

                }
                if(value.iteration - previousClusterIteration > Constant.enoughIterations){
                    getContext().become(killMode,true);
                }
                values.remove(value.iteration);
            }
        })
        .build();
    }

    /**
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
     */
    private final Receive killMode = receiveBuilder()
            .match(Value.class, msg -> {
                if(nodes == null) nodes = new ArrayList<>();
                if(!nodes.contains(sender())) nodes.add(sender());
                if(nodes.size() == size){
                    for(ActorRef actorRef : nodes)
                        actorRef.tell(akka.actor.PoisonPill.getInstance(),ActorRef.noSender());

                    timer.stop();
                    log.info("Job done U_U after " + previousClusterIteration + " iterations and " + timer);
                    context().system().terminate();
                }
            })
            .build();

    /*
    *//**
     * Creating the cluster
     *
     * It is said that a node "n" belongs to a cluster with reference to an exemplar "e"
     * if forall of the other exemplars emerged "o" : s(n,e) greater than s(n,o)
     *
     * @param exemplars vector
     * @param verbose if True : print all cluster
     * @param showGraph if True : show visual graph
     * @return cluster : i belong to cluster[i]
     *//*
    private int[] buildCluster(int[] exemplars, boolean verbose, boolean showGraph){
        int[] cluster = new int[size];

        for (int i = 0; i < size; i++) {
            Double max = Util.min_double;
            int index = -1;
            for (int k : exemplars) {
                if (max < similarity[i][k]) {
                    max = similarity[i][k];
                    index = k;
                }
            }
            cluster[i] = index;
        }

        if(verbose)
            for (int i = 0; i < size; i++)
                log.info(i + " belong to " + cluster[i]);

        if(showGraph) (new VisualGraph(exemplars,cluster)).show(800,800);
        return cluster;
    }

    *//**
     * Checks if the calculated cluster is different from the last different calculated previously
     * @param cluster to check if is changed
     * @param iteration to which it refers
     * @return  True if cluster != previous cluster
     *          False if cluster == previous cluster
     *//*
    private boolean isChanged(int[] cluster, long iteration){
        *//* I'm calculating a cluster for an old iteration *//*
        if(previousClusterIteration > iteration) return false;

        *//* Computing the first cluster *//*
        if(previousCluster == null){
            previousCluster = cluster;
            previousClusterIteration = iteration;
            return true;
        }

        *//* Is changed *//*
        for(int i = 0; i < size; i++){
            if(previousCluster[i] != cluster[i]){
                previousCluster = cluster;
                previousClusterIteration = iteration;
                return true;
            }
        }

        *//* Is not changed *//*
        return false;
    }*/
}