import akka.actor.ActorRef;

import java.util.HashMap;
import java.util.HashSet;

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
    @SuppressWarnings("unused")
    public final int size;

    public Neighbors(ActorRef[] array, int size){
        this.array = array;
        this.size = size;
    }
}
/**
 *
 */
class NodeSetting implements Messages{
    public final HashMap<Integer,Double> s_row;
    public final int selfID;
    public final int r_received_size;
    public final int a_received_size;
    public final ActorRef[] r_not_infinite_neighbors;
    public final ActorRef[] a_not_infinite_neighbors;
    public final int[] r_reference;
    public final int[] a_reference;
    public final HashMap<Integer,Double> a_row;
    public final HashMap<Integer,Double> r_col;

    NodeSetting(HashMap<Integer,Double> s_row, int selfID,
                int r_received_size, int a_received_size,
                ActorRef[] r_not_infinite_neighbors, ActorRef[] a_not_infinite_neighbors,
                int[] r_reference, int[] a_reference,
                HashMap<Integer,Double> a_row, HashMap<Integer,Double> r_col) {
        this.s_row = s_row;
        this.selfID = selfID;
        this.r_received_size = r_received_size;
        this.a_received_size = a_received_size;
        this.r_not_infinite_neighbors = r_not_infinite_neighbors;
        this.a_not_infinite_neighbors = a_not_infinite_neighbors;
        this.r_reference = r_reference;
        this.a_reference = a_reference;
        this.a_row = a_row;
        this.r_col = r_col;
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

/**
 *
 */
class LocalExemplars implements Messages {
    public final long iteration;
    public final HashSet<Integer> exemplars;

    public LocalExemplars(long iteration, HashSet<Integer> exemplars){
        this.iteration = iteration;
        this.exemplars = exemplars;
    }
}