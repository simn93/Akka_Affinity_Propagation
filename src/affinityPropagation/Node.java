package affinityPropagation;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import java.util.HashMap;

/**
 * Class for cloud node management
 *
 * @author Simone Schirinzi
 */
class Node extends AbstractActor{
    // Connection variable

    /**
     * Link to the actor responsible for generating the cluster.
     * Received when creating the node.
     * Each node periodically sends a Value () message to check the algorithm's performance.
     */
    private final ActorRef aggregator;

    /**
     * lambda factor for dumping messages update
     */
    private final double lambda;

    /**
     * Set how many iterations wait for send message to aggregator
     */
    private final int sendEach;

    /**
     * Flag for detailed log
     */
    private final boolean verbose;

    /**
     * Ref to log
     */
    private final ActorRef log;

    //------------------

    // Received from Initialize
    //Similarity

    /**
     * Vector line of similarity of interest for the node.
     * Received from the Initialize message.
     */
    private HashMap<Integer,Double> s_row;

    //Id of Actor

    /**
     * Node identifier
     * Received from the Initialize message.
     */
    private int self;

    //------------------

    // Iteration variable

    /**
     * Iteration variable.
     * Indicates the current iteration number.
     */
    private long iteration;

    // Memorization of value received

    /**
     * Remembers the responsibilities received from other nodes.
     * A node k receives r(i, k) from node i.
     * R_col[i] = r[i][k]
     */
    private HashMap<Integer,Double> r_col;

    /**
     * Remembers the availability received from other nodes.
     * A node k receives a(k, i) from node i.
     * A_row[i] = a[k][i]
     */
    private HashMap<Integer,Double> a_row;

    // Received values counter

    /**
     * Counter of Responsibility received from Other Nodes.
     * It is initialized at 0 at the beginning of each iteration
     */
    private int r_received;

    /**
     * Counter of Availability received from Other Nodes.
     * It is initialized at 0 at the beginning of each iteration
     */
    private int a_received;

    //------------------

    // Optimizations for less computation

    /**
     * Denotes the number of nodes such that s_row[i] = -INF
     */
    private int r_received_size;

    /**
     * Denotes the number of nodes such that s_col[i] = -INF
     */
    private int a_received_size;

    /**
     * Vector of references to such actors that s_row [i]! = -INF
     */
    private ActorRef[] r_not_infinite_neighbors;

    /**
     * Vector of references to such actors that s_col [i]! = -INF
     */
    private ActorRef[] a_not_infinite_neighbors;

    /**
     * Vector of indexes of such actors that s_row [i]! = -INF
     * r_not_infinite_neighbors[i].ID == r_reference[i]
     */
    private int[] r_reference;

    /**
     * Vector of indexes of such actors that s_col [i]! = -INF
     * a_not_infinite_neighbors[i].ID == a_reference[i]
     */
    private int[] a_reference;

    /**
     * Create a node
     * Initializes iteration variables
     * Send a hello message to the dispatcher
     * @param aggregator link to aggregator
     * @param lambda dumping factor
     * @param sendEach tic for aggregator message
     * @param verbose flag for detailed log
     * @param log Ref to log
     */
    public Node(ActorRef aggregator, double lambda, int sendEach, boolean verbose, ActorRef log) {
        this.aggregator = aggregator;
        this.lambda = lambda;
        this.sendEach = sendEach;
        this.verbose = verbose;
        this.log = log;
    }

    /**
     * Receive builder
     * @return Node message manager
     * @see NodeSetting
     * @see Start
     * @see Responsibility
     * @see Availability
     */
    @Override public Receive createReceive() {
        return receiveBuilder()
                .match(NodeSetting.class, this::initializeHandler)
                .match(Start.class, msg -> sendResponsibility())
                .match(Responsibility.class, this::responsibilityHandler)
                .match(Availability.class, this::availabilityHandler)
                .build();
    }

    /**
     * Handler for the initialization message
     * @param init received message
     */
    private void initializeHandler(NodeSetting init){
        this.s_row = init.s_row;
        this.self = init.selfID;
        this.r_received_size = init.r_received_size;
        this.a_received_size = init.a_received_size;
        this.r_not_infinite_neighbors = init.r_not_infinite_neighbors;
        this.a_not_infinite_neighbors = init.a_not_infinite_neighbors;
        this.r_reference = init.r_reference;
        this.a_reference = init.a_reference;
        this.a_row = init.a_row;
        this.r_col = init.r_col;
        sender().tell(new Ready(), self());
    }

    /**
     * Receive the message.
     * Refresh the saved value by damping it with a lambda factor.
     *
     * Increments the responsibility counter received,
     * and possibly submits the availability
     *
     * @param responsibility received
     * @see Responsibility
     */
    private void responsibilityHandler(Responsibility responsibility){
        r_col.put(responsibility.sender, (r_col.get(responsibility.sender) * lambda) + (responsibility.value * (1 - lambda)));
        r_received++;

        if (r_received == r_received_size) {
            r_received = 0;

            sendAvailability();
        }
    }

    /**
     * Receive the message.
     * Refresh the saved value by damping it with a lambda factor.
     *
     * Increments the availability counter received,
     * and possibly submits the responsibility
     *
     * It also undertakes to send a Value() message to the aggregator.
     * This value is calculated
     * as the sum of self-responsibility and self-availability.
     *
     * Increase iteration counter
     *
     * @param availability received
     * @see Availability
     */
    private void availabilityHandler(Availability availability){
        a_row.put(availability.sender, (a_row.get(availability.sender) * lambda) + (availability.value * (1 - lambda)));
        a_received++;

        if (a_received == a_received_size) {
            a_received = 0;

            /* End of an iteration. Check whether or not to send an update. */
            if (this.iteration % (sendEach) == (sendEach - 1))
                aggregator.tell(new Value(r_col.get(self) + a_row.get(self), self, iteration), self());

            if (verbose && self == 0) log.tell("Iteration " + iteration + " completed!", ActorRef.noSender());
            sendResponsibility();

            this.iteration++;
        }
    }

    /**
     * Send responsibility to other node
     *
     * If it optimizes the sending of messages,
     * it only sends to r_not_infinite_neighbors.
     * Otherwise he sends it to neighbors.
     *
     * If it optimizes the responsibility calculation,
     * it pre-calculates the maximum
     * of the set {a_row [i] + s_row [i]},
     * possibly iterating only on such nodes
     * that s_row [i] is not infinite.
     * Keeps Memory The first bigger value,
     * the index of the node to which it refers, is k,
     * and the second largest value.
     * The calculation of the responsibility is obtained
     * by subtracting the maximum from s_row[j],
     * except if k == j. In this case
     * the second maximum is subtracted.
     *
     * @see Responsibility
     */
    private void sendResponsibility(){
        double firstMax, secondMax;
        int firstK = -1;
        firstMax = secondMax = Double.NEGATIVE_INFINITY;

        for (int i : r_reference) {
            double value = a_row.get(i) + s_row.get(i);
            if (firstMax <= value) {
                secondMax = firstMax;
                firstMax = value;
                firstK = i;
            } else if (secondMax <= value) secondMax = value;
        }

        for (int i = 0; i < r_not_infinite_neighbors.length; i++)
            r_not_infinite_neighbors[i].tell(new Responsibility((r_reference[i] == firstK ? s_row.get(r_reference[i]) - secondMax : s_row.get(r_reference[i]) - firstMax), self), self());
    }

    /**
     * Send Availability to other node
     *
     * If it optimizes the sending of messages,
     * it only sends to a_not_infinite_neighbors.
     * Otherwise he sends it to neighbors.
     *
     * If it optimizes the responsibility calculation,
     * it undertakes to pre-compute
     * the sum of the positive values
     * of the set {r_col [i]},
     * possibly iterating only on such nodes
     * that s_col[i] is not infinite.
     * The calculation of availability is obtained
     * by subtracting r_col[j] from that sum
     * only if r_col[j] is positive.
     */
    private void sendAvailability(){
        double sum = r_col.get(self);
        for(int q : a_reference)
            if(q != self && r_col.get(q) > 0.0)
                sum += r_col.get(q);

        for (int i = 0; i < a_not_infinite_neighbors.length; i++)
            a_not_infinite_neighbors[i].tell(new Availability(a(i,sum), self), self());
    }

    /**
     * Support function for "a" function computation
     *
     * @param i node index
     * @param sum global sum for this node
     * @return a(i,j)
     */
    private double a(int i, double sum){
        double r_col_i = r_col.get(a_reference[i]);
        double sumLess = r_col_i > 0.0 ? sum - r_col_i : sum;
        return a_reference[i] != self ? (0 < sumLess ? 0 : sumLess) : sum - r_col.get(self);
    }
}