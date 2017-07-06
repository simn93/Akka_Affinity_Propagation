import akka.actor.*;
import java.util.stream.IntStream;

/**
 * Class for cloud node management
 *
 * @author Simone Schirinzi
 */
class Node extends AbstractActor{
    // Connection variable
    private ActorRef dispatcher;
    private ActorRef aggregator;
    //------------------

    // Received from Initialize
    //Similarity
    private double[] s_row;
    private double[] s_col;
    //Id of Actor
    private int self;
    //------------------

    // Received from Neighbors
    private ActorRef[] neighbors;
    private int size;
    //------------------

    // Iteration variable
    // Iteration counter
    private long iteration;
    // Memorization of value received
    // x_col[q] = x[q][this]
    private double[] r_col;
    // x_row[q] = x[this][q]
    private double[] a_row;
    // Received values counter
    private int r_received;
    private int a_received;
    //------------------

    // Optimizations for less computation
    private boolean sendOptimize;
    // Optimizations for less messages
    private boolean optimize;
    //...
    private int row_infinity;
    private int col_infinity;
    private ActorRef[] r_not_infinite_neighbors;
    private ActorRef[] a_not_infinite_neighbors;
    private int[] r_reference;
    private int[] a_reference;

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
                // Messaggio di init
                .match(Initialize.class, init -> {
                    this.s_col = init.similarity_col;
                    this.s_row = init.similarity_row;
                    this.self = init.selfID;
                })

                //Messaggio di pre-start
                .match(Neighbors.class, neighbors1 -> {
                    this.neighbors = neighbors1.array;
                    this.size = neighbors1.size;

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
                })

                // Messaggio di start
                .match(Start.class, msg -> {
                    //System.out.println(self + " started!");
                    sendResponsibility();
                })

                .match(Responsibility.class, responsibility -> {
                    r_col[responsibility.sender] = (r_col[responsibility.sender] * Constant.lambda) + (responsibility.value * (1 - Constant.lambda));
                    r_received++;

                    if (r_received == size - col_infinity) {
                        r_received = 0;

                        sendAvailability();
                    }
                })

                .match(Availability.class, availability -> {
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
                })

                .match(Die.class, msg -> System.out.println("Not usefull actor..."))
                .build();
    }

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

            for (int i = 0; i < size; i++) {
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
            for(int q = 0; q < size; q++)
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