import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

/**
 * Class for assigning and initializing nodes
 *
 * @author Simone Schirinzi
 */
class Dispatcher extends AbstractActor {
    /**
     * number of nodes
     */
    private final int size;

    /**
     * graph of similarity
     */
    private final double[][] Graph;

    /**
     * vector of link to nodes
     */
    private final ActorRef[] array;

    /**
     * Initialized value at 0.
     * At any time it indicates what is the index
     * of the next node to initiate.
     * When it comes to size,
     * we know that we have initiated a sufficient number of nodes
     * to start the algorithm.
     */
    private int index;

    /**
     * Initialized value at 0
     * At any time it indicates how many nodes
     * Have finished their initialization procedures.
     * We expect all actors to receive the message of creation before they start,
     * otherwise they receive messages from other actors already ready,
     * but they put them in unprepared structures
     */
    private int ready;

    static Props props(double[][] Graph, int size) {
        return Props.create(Dispatcher.class, () -> new Dispatcher(Graph,size));
    }

    private Dispatcher(double[][] Graph, int size){
        this.size = size;
        this.Graph = Graph;

        this.array = new ActorRef[size];
        this.index = 0;
        this.ready = 0;
    }

    /**
     * Actor messages handler
     * @see Self
     * @see Ready
     * @return receive handler
     */
    @Override
    public Receive createReceive() {
        return receiveBuilder()
        .match(Self.class, msg -> selfHandler(sender()))
        .match(Ready.class, msg -> {
            this.ready++;
            if(ready == size) for(ActorRef node : array) node.tell(new Start(),ActorRef.noSender());
        })
        .build();
    }

    /**
     * Collect links to nodes
     *
     * @param sender : Ref to sender
     */
    private void selfHandler(ActorRef sender){
        /* if we have initialized enough nodes */
        if (index >= size) {
            sender.tell(new Die(), self());
            return;
        }

        /*
         * create row and col vector
         * Instead of sending the whole graph to all nodes
         * Reliably forward the values of interest
         */
        double[] row = new double[size];
        double[] col = new double[size];

        for (int j = 0; j < size; j++) {
            row[j] = Graph[index][j];
            col[j] = Graph[j][index];
        }

        /* save node link */
        array[index] = sender;

        /* send initialize message */
        sender.tell(new Initialize(row, col, index), ActorRef.noSender());

        /* increase index value */
        /* wait next node */
        index++;

        /* check if we have initialized enough nodes */
        if (index == size) {
            Neighbors neighbors = new Neighbors(array, size);
            for (ActorRef node : array) node.tell(neighbors, ActorRef.noSender());

            System.out.println("Started " + index + " actor");
        }
    }
}
