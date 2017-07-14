import akka.actor.*;
import java.util.stream.IntStream;

/**
 * Class for cloud node management
 *
 * @author Simone Schirinzi
 */
class Node extends AbstractActor{
    // Connection variable

    /**
     * Link to the actor responsible for initializing the nodes.
     * Received when creating the node.
     * A newly created node sends a self () message
     */
    @SuppressWarnings("CanBeFinal")
    private ActorRef dispatcher;

    /**
     * Link to the actor responsible for generating the cluster.
     * Received when creating the node.
     * Each node periodically sends a Value () message to check the algorithm's performance.
     */
    @SuppressWarnings("CanBeFinal")
    private ActorRef aggregator;

    //------------------

    // Received from Initialize
    //Similarity

    /**
     * Vector line of similarity of interest for the node.
     * Received from the Initialize message.
     */
    private double[] s_row;

    /**
     * Column vector of similarity of interest for the node.
     * Received from the Initialize message.
     */
    private double[] s_col;

    //Id of Actor

    /**
     * Node identifier
     * Received from the Initialize message.
     */
    private int self;

    //------------------

    // Received from Neighbors

    /**
     * Vector containing references to all actors in the cluster.
     * Received from the Neighbors message.
     */
    private ActorRef[] neighbors;

    /**
     * Indicates the number of nodes initiated by the algorithm.
     * Received from the Neighbors message.
     */
    private int size;

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
    private double[] r_col;

    /**
     * Remembers the availability received from other nodes.
     * A node k receives a(k, i) from node i.
     * A_row[i] = a[k][i]
     */
    private double[] a_row;

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
    private int row_infinity;

    /**
     * Denotes the number of nodes such that s_col[i] = -INF
     */
    private int col_infinity;

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
     * @param dispatcher link to dispatcher
     */
    public Node(ActorRef aggregator, ActorRef dispatcher){
        this.aggregator = aggregator;
        this.dispatcher = dispatcher;
        standardSetting();
        dispatcher.tell(new Self(),self());
    }

    /**
     * Initializes iteration variables.
     * Enables both optimization flags
     */
    private void standardSetting(){
        iteration = 0;
        r_received = 0;
        a_received = 0;
        row_infinity = 0;
        col_infinity = 0;
    }

    /**
     * The termination phase is started by the aggregator.
     * It sends a poison pill to the node.
     * Akka lets you run a last function on the node.
     * This also ends the systems connected to the nodes.
     */
    @Override public void postStop() {getContext().system().terminate();}

    /**
     * Receive builder
     * @return Node message manager
     * @see Initialize
     * @see Neighbors
     * @see Start
     * @see Die
     * @see Responsibility
     * @see Availability
     */
    @Override public Receive createReceive() {
        return receiveBuilder()
                .match(Initialize.class, this::initializeHandler)
                .match(Neighbors.class, this::neighborsHandler)
                .match(Die.class, this::dieHandler)
                .match(Start.class, msg -> sendResponsibility())
                .match(Responsibility.class, this::responsibilityHandler)
                .match(Availability.class, this::availabilityHandler)
                .build();
    }

    /**
     * Handler for the initialization message
     * @param init received message
     */
    private void initializeHandler(Initialize init){
        this.s_col = init.similarity_col;
        this.s_row = init.similarity_row;
        this.self = init.selfID;
    }

    /**
     * Handler for neighbors message
     *
     * If optimize is True:
     * let -/&gt; : not send to
     *
     * i -/&gt; k responsibility if s(i,k) = -INF
     * r(i,k) = -INF
     * i -/&gt; k availability if s(k,i) = -INF
     * a(i,k) != -INF
     * but is not influential for k
     * when he compute r(k,j) = s(k,j) - max{a(k,i) + -INF}
     *
     * col_infinity : The node which cannot reach me
     * row_infinity : The node i cannot reach
     *
     * if s_col[i] is infinity
     * r_col[i] must be set to infinity
     *
     * In addition, the carriers of links to infinite nodes are initialized
     *
     * When finished all operations
     * send a Ready() message to the dispatcher.
     * The node thus remains awaiting a Start() message
     *
     * @param neigh received message
     */
    private void neighborsHandler(Neighbors neigh){
        this.neighbors = neigh.array;
        this.size = neigh.size;
        a_row = new double[size];
        r_col = new double[size];

        for (int i = 0; i < size; i++) {
            if (Util.isMinDouble(s_col[i])) {
                col_infinity++;

                /* r initialize */
                r_col[i] = Util.min_double;
            }
            if (Util.isMinDouble(s_row[i])) row_infinity++;
        }

        /* size - row_infinite nodes must receive responsibility message */
        r_not_infinite_neighbors = new ActorRef[size - row_infinity];

        /* size - col_infinite nodes must receive availability message */
        a_not_infinite_neighbors = new ActorRef[size - col_infinity];

        r_reference = new int[size - row_infinity];
        a_reference = new int[size - col_infinity];

        /* Vector are set */
        int j = 0, k = 0;
        for (int i = 0; i < size; i++) {
            if (!Util.isMinDouble(s_row[i])) {
                r_not_infinite_neighbors[j] = neighbors[i];
                r_reference[j] = i;
                j++;
            }
            if (!Util.isMinDouble(s_col[i])) {
                a_not_infinite_neighbors[k] = neighbors[i];
                a_reference[k] = i;
                k++;
            }
        }

        /* node are ready to start */
        dispatcher.tell(new Ready(), self());
    }

    /**
     * If more nodes are created than necessary,
     * the dispatcher sends a message to the node.
     * The node is useless for computing purposes.
     *
     * @param msg received
     */
    private void dieHandler(Die msg){
        System.out.println(msg + " Not useful actor...");
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
        r_col[responsibility.sender] = (r_col[responsibility.sender] * Constant.lambda) + (responsibility.value * (1 - Constant.lambda));
        r_received++;

        if (r_received == size - col_infinity) {
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
        a_row[availability.sender] = (a_row[availability.sender] * Constant.lambda) + (availability.value * (1 - Constant.lambda));
        a_received++;

        if (a_received == size - row_infinity) {
            a_received = 0;

            /* End of an iteration. Check whether or not to send an update. */
            if (this.iteration % (Constant.sendEach) == (Constant.sendEach - 1))
                aggregator.tell(new Value(r_col[self] + a_row[self], self, iteration), self());

            if (self == 0) System.out.println("Iteration " + iteration + " completed!");
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
        firstMax = secondMax = Util.min_double;

        for (int i : r_reference) {
            double value = a_row[i] + s_row[i];
            if (firstMax <= value) {
                secondMax = firstMax;
                firstMax = value;
                firstK = i;
            } else if (secondMax <= value) secondMax = value;
        }

        for (int i = 0; i < r_not_infinite_neighbors.length; i++)
            r_not_infinite_neighbors[i].tell(new Responsibility((r_reference[i] == firstK ? s_row[r_reference[i]] - secondMax : s_row[r_reference[i]] - firstMax), self), self());
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
        double sum = r_col[self];
        for(int q : a_reference)
            if(q != self && r_col[q] > 0.0)
                sum += r_col[q];

        for (int i = 0; i < a_not_infinite_neighbors.length; i++)
            a_not_infinite_neighbors[i].tell(new Availability(a_reference[i] != self ? (0 < (r_col[a_reference[i]] > 0.0 ? sum - r_col[a_reference[i]] : sum) ? 0 : (r_col[a_reference[i]] > 0.0 ? sum - r_col[a_reference[i]] : sum)) : sum - r_col[self], self), self());
    }
}