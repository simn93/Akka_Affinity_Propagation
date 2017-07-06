import akka.actor.ActorRef;

/**
 * Class for collecting exchanged messages
 *
 * @author Simone Schirinzi
 */
interface Messages {
}

/**
 * Responsibility
 */
class Responsibility implements Messages {
    public final double value;
    public final int sender;

    public Responsibility (double value, int sender){
        this.value = value;
        this.sender = sender;
    }
}

/**
 * Availability
 */
class Availability implements Messages {
    public final double value;
    public final int sender;

    public Availability (double value, int sender){
        this.value = value;
        this.sender = sender;
    }
}

/**
 * Neighbors class
 * Provides information on links to cluster nodes
 */
class Neighbors implements Messages {
    public final ActorRef[] array;
    public final int size;

    public Neighbors(ActorRef[] array, int size){
        this.array = array;
        this.size = size;
    }
}

/**
 * Initialize class
 * Provides information about the identity that the node will have to assume
 */
class Initialize implements Messages {
    public final double[] similarity_row;
    public final double[] similarity_col;
    public final int selfID;

    public Initialize(double[] similarity_row, double[] similarity_col, int selfID) {
        this.similarity_row = similarity_row;
        this.similarity_col = similarity_col;
        this.selfID = selfID;
    }
}

/**
 * Value for cluster's creation
 * It's send from nodes to aggregator.
 * Defines which iteration is related
 */
class Value implements Messages {
    public final Double value;
    public final int sender;
    public final long iteration;

    public Value(Double value, int sender, long iteration){
        this.value = value;
        this.sender = sender;
        this.iteration = iteration;
    }
}

/**
 * Node hello message for the dispatcher
 */
class Self implements Messages {

}

/**
 * Error dispatcher message for node: too many nodes were created.
 */
class Die implements Messages{

}

/**
 * Start Message for Nodes: All nodes have completed initialization operations.
 * The algorithm can begin.
 */
class Start implements Messages {

}

/**
 * Message from the node to the dispatcher:
 * Report that the node has completed initialization operations
 */
class Ready implements Messages {

}