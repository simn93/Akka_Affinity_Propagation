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
    private ActorRef dispatcher;

    /**
     * Link to the actor responsible for generating the cluster.
     * Received when creating the node.
     * Each node periodically sends a Value () message to check the algorithm's performance.
     */
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
     *
     * @param aggregator
     * @param dispatcher
     */
    public Node(ActorRef aggregator, ActorRef dispatcher){
        this.aggregator = aggregator;
        this.dispatcher = dispatcher;
        standardSetting();
        dispatcher.tell(new Self(),self());
    }

    private void standardSetting(){
        iteration = 0;
        r_received = 0;
        a_received = 0;
        row_infinity = 0;
        col_infinity = 0;
        sendOptimize = true;
        optimize = true;
    }

    @Override public void postStop() {getContext().system().terminate();}

    @Override public Receive createReceive() {
        return receiveBuilder()
                .match(Initialize.class, this::initializeHandler) // Messaggio di init
                .match(Neighbors.class, this::neighborsHandler) //Messaggio di pre-start
                .match(Start.class, this::sendResponsibility) // Messaggio di start
                .match(Die.class, this::dieHandler) //Not useful actor
                .match(Responsibility.class, this::responsibilityHandler)
                .match(Availability.class, this::availabilityHandler)
                .build();
    }

    private void initializeHandler(Initialize init){
        this.s_col = init.similarity_col;
        this.s_row = init.similarity_row;
        this.self = init.selfID;
    }

    private void neighborsHandler(Neighbors neigh){
        this.neighbors = neigh.array;
        this.size = neigh.size;

        //To begin the availabilities are initialized to 0
        a_row = new double[size];
        //In the first iteration is set to s_col[i] - max{s_col[j]} j != i
        r_col = new double[size];

        if (optimize) {
            // -/> : not send to
            // i -/> k responsibility if s(i,k) = -INF
            //r(i,k) = -INF
            // i -/> k availability if s(k,i) = -INF
            //a(i,k) != -INF
            //but is not influential for k when he compute r(k,j) = s(k,j) - max{a(k,i) + -INF}
            for (int i = 0; i < size; i++) {
                if (Util.isMinDouble(s_col[i])) { //The node which cannot reach me
                    col_infinity++;
                    r_col[i] = Util.min_double; // r initialize
                    // i will not receive message from node which have s[k][i] = -INF
                }
                if (Util.isMinDouble(s_row[i])) { //The node i cannot reach
                    row_infinity++;
                    // a initialize : no need to do. is 0 to default
                }
            }

            r_not_infinite_neighbors = new ActorRef[size - row_infinity];
            a_not_infinite_neighbors = new ActorRef[size - col_infinity];
            r_reference = new int[size - row_infinity];
            a_reference = new int[size - col_infinity];

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

        dispatcher.tell(new Ready(), self());
    }

    private void dieHandler(Die msg){
        System.out.println("Not usefull actor...");
    }

    private void responsibilityHandler(Responsibility responsibility){
        r_col[responsibility.sender] = (r_col[responsibility.sender] * Constant.lambda) + (responsibility.value * (1 - Constant.lambda));
        r_received++;

        if (r_received == size - col_infinity) {
            r_received = 0;

            sendAvailability();
        }
    }

    private void availabilityHandler(Availability availability){
        a_row[availability.sender] = (a_row[availability.sender] * Constant.lambda) + (availability.value * (1 - Constant.lambda));
        a_received++;

        if (a_received == size - row_infinity) {
            a_received = 0;

            //Iteration's end
            if (this.iteration % (Constant.sendEach) == (Constant.sendEach - 1))
                aggregator.tell(new Value(r_col[self] + a_row[self], self, iteration), self());

            if (self == 0) System.out.println("Iterazione " + iteration + " completata!");
            sendResponsibility();

            this.iteration++;
        }
    }

    private void sendResponsibility(Object ignore){sendResponsibility();}
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
                //String debug = "";
                //for(int j = 0; j < size; j++) if(!Util.isMinDouble(a_row[j]+s_row[j])) debug+=a_row[j]+s_row[j]+" ";
                //assert (sendValue[i] == r(sendIndex[i])) : sendValue[i] + " " + r(sendIndex[i]) + " " +firstK + " " + firstMax + " " + secondMax + " " + debug;
                //assert (r(sendIndex[i]) == r(r_reference[i]));
        } else
            for(int i = 0; i < sendSize; i++)
                sendValue[i] = r(sendIndex[i]);

        for (int i = 0; i < sendSize; i++)
            sendVector[i].tell(new Responsibility(sendValue[i], self), self());
            //assert (sendValue[i] == r(r_reference[i]));
            //assert (sendVector[i].compareTo(r_not_infinite_neighbors[i]) == 0);
    }

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

    private double r(int k){
        double max = Util.min_double;
        // foreach except k
        for (int i = 0; i < k; i++)
            max = (max > (a_row[i] + s_row[i])) ? max : (a_row[i] + s_row[i]);//Math.max(max, a_row[i] + s_row[i]);
        for (int i = k + 1; i < size; i++)
            max = (max > (a_row[i] + s_row[i])) ? max : (a_row[i] + s_row[i]);//Math.max(max, a_row[i] + s_row[i]);

        return s_row[k] - max;
    }

    private double r(int k, int firstK, double firstMax, double secondMax){
        return (k == firstK ? s_row[k] - secondMax : s_row[k] - firstMax);
    }

    private double a(int i){
        double ret = r_col[self];
        for(int q = 0; q < size; q++)
            if(q != i && q != self && r_col[q] > 0.0)
                ret += r_col[q];

        return 0 < ret ? 0 : ret;//Math.min(0,ret);
    }

    private double a(int i, double sum){
        if(r_col[i] > 0.0) sum -= r_col[i];
        return 0 < sum ? 0 : sum;
    }

    private double a(){
        double ret = 0.0;
        for(int q = 0; q < size; q++)
            if(q != self && r_col[q] > 0.0)
                ret += r_col[q];

        return ret;
    }

    private double a(double sum){
        return sum - r_col[self];
    }
}