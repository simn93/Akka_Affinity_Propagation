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
     * Flags to enable some optimizations to reduce the complexity of the calculations
     *
     * In the calculation of responsibilities,
     * it computes only one time
     * the maximum of the set {a_row [i] + s_row [i]}
     * and only between nodes with non infinite similarity
     */
    private boolean sendOptimize;

    // Optimizations for less messages

    /**
     * Flags to enable some optimizations to reduce the number of exchanged node messages.
     * Avoid sending messages to the nodes with which it has infinite similarity
     */
    private boolean optimize;

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
        sendOptimize = true;
        optimize = true;
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

        if (optimize) {
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
            //assert(j == r_reference.length);
            //assert(j + row_infinity == size);
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
        ActorRef[] sendVector;
        int[] sendIndex;
        int sendSize;
        double[] sendValue;

        if(optimize){
            sendVector = r_not_infinite_neighbors;
            sendIndex = r_reference;
            sendSize = r_not_infinite_neighbors.length;
        } else {
            sendVector = neighbors;
            sendIndex = IntStream.range(0, size).toArray();
            sendSize = size;
        }
        sendValue = new double[sendSize];

        if(sendOptimize) {
            double firstMax, secondMax;
            int firstK = -1;
            firstMax = secondMax = Util.min_double;

            for (int i : sendIndex) {
                double value = a_row[i] + s_row[i];
                if (firstMax <= value) {
                    secondMax = firstMax;
                    firstMax = value;
                    firstK = i;
                } else if (secondMax <= value) secondMax = value;
            }

            for(int i = 0; i < sendSize; i++)
                sendValue[i] = r(sendIndex[i],firstK,firstMax,secondMax);
                //assert (sendValue[i] == r(sendIndex[i]));
        } else
            for(int i = 0; i < sendSize; i++)
                sendValue[i] = r(sendIndex[i]);

        for (int i = 0; i < sendSize; i++)
            sendVector[i].tell(new Responsibility(sendValue[i], self), self());
            //assert (sendValue[i] == r(r_reference[i]));
            //assert (sendVector[i].compareTo(r_not_infinite_neighbors[i]) == 0);
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
        ActorRef[] sendVector;
        int[] sendIndex;
        int sendSize;
        double[] sendValue;

        if(optimize){
            sendVector = a_not_infinite_neighbors;
            sendIndex = a_reference;
            sendSize = a_not_infinite_neighbors.length;
        } else {
            sendVector = neighbors;
            sendIndex = IntStream.range(0, size).toArray();
            sendSize = size;
        }
        sendValue = new double[sendSize];

        if(sendOptimize){
            double sum = r_col[self];
            for(int q : sendIndex)
                if(q != self && r_col[q] > 0.0)
                    sum += r_col[q];

            for(int i = 0; i < sendSize; i++){
                if(sendIndex[i] != self) {
                    sendValue[i] = a(sendIndex[i], sum);
                    //assert ((a(sendIndex[i],sum) - a(sendIndex[i])) < 10e-6);
                    //assert (a(sendIndex[i]) == a(a_reference[i]));
                } else
                    sendValue[i] = a(sum);
                    //assert ((a(sum) - a()) < 10e-6);
            }
        } else
            for(int i = 0; i < sendSize; i++)
                if (sendIndex[i] != self) { sendValue[i] = a(sendIndex[i]); }
                    else sendValue[i] = a();

        for (int i = 0; i < sendSize; i++)
            sendVector[i].tell(new Availability(sendValue[i], self), self());
    }

    /**
     * Compute r(i,k).
     * Responsibility r(i,k) sent from node i candidate exemplar k
     * reflects the accumulated evidence for how well-suited
     * point k is to serve as exemplar for point i
     *
     * r(i,k) = s(i,k) - max, k' s.t. k' != k, {a(i,k')+s(i,k')}
     * @param k index of node which send r(i,k)
     * @return r(i,k)
     */
    private double r(int k){
        double max = Util.min_double;
        /* foreach except k */
        /* pre condition : max = -INF */
        for (int i = 0; i < k; i++)
            max = (max > (a_row[i] + s_row[i])) ? max : (a_row[i] + s_row[i]);//Math.max(max, a_row[i] + s_row[i]);
        for (int i = k + 1; i < size; i++)
            max = (max > (a_row[i] + s_row[i])) ? max : (a_row[i] + s_row[i]);//Math.max(max, a_row[i] + s_row[i]);
        /* post condition : max = maximum, k' != k, 0 <= k' <= size,
        { a(i,k') + s(i,k') } */

        return s_row[k] - max;
    }

    /**
     * Compute r(i,k) in an optimized way.
     * @param k index of node which send r(i,k)
     * @param firstK max of {a(i,j)+s(i,j)}
     * @param firstMax index q such that firstK == a(i,q)+s(i,q)
     * @param secondMax max of {a(i,j)+s(i,j), j!=firstMax }
     * @return r(i,k)
     */
    private double r(int k, int firstK, double firstMax, double secondMax){
        return (k == firstK ? s_row[k] - secondMax : s_row[k] - firstMax);
    }

    /**
     * Compute a(i,k).
     * Availability a(i,k), sent from candidate exemplar point k
     * to point i, reflects the accumulated evidence for
     * how appropriate it would be for point i to choose
     * point k as its exemplars
     *
     * a(i,k) = min{0, r(k,k)+ Σ, i' s.t. i' != i &amp;&amp; i' != k, (max{0,r(i',k)})}
     *
     * @param i index of node which send a(i,k)
     * @return a(i,k)
     */
    private double a(int i){
        double ret = r_col[self];

        /* pre condition : ret = r(k,k) */
        for(int q = 0; q < size; q++)
            if(q != i && q != self && r_col[q] > 0.0)
                ret += r_col[q];
        /* post condition : ret = r(k,k) + Σ r(q,k) ,
         * s.t. q != i && q != self && r[q][i]) > 0.0
         */

        /* return min { 0 , ret } */
        return 0 < ret ? 0 : ret;
    }

    /**
     * Compute r(i,k) in an optimized way
     * @param i index of the node which send a(i,k)
     * @param sum r(k,k) + Σ max{0,r(i,q)} s.t. q != self
     * @return a(i,k)
     */
    private double a(int i, double sum){
        /* We added too much.
         * To get the correct result
         * you have to subtract the possible member too.
         */
        if(r_col[i] > 0.0) sum -= r_col[i];

        /* return min { 0 , ret } */
        return 0 < sum ? 0 : sum;
    }

    /**
     * Self availability is updated differently.
     * This message reflects accumulated evidence that point k
     * is an exemplar, based on the positive responsibilities
     * sent to candidate exemplar k from other points
     *
     * @return a(k,k)
     */
    private double a(){
        double ret = 0.0;
        for(int q = 0; q < size; q++)
            if(q != self && r_col[q] > 0.0)
                ret += r_col[q];

        return ret;
    }

    /**
     * Compute r(k,k) in an optimized way
     * We don't need r(k,K) in "sum"
     *
     * @param sum r(k,k) + Σ max{0,r(i,q)} s.t. q != self
     * @return r(k,k)
     */
    private double a(double sum){
        return sum - r_col[self];
    }
}