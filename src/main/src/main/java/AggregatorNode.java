import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Class for aggregate and filter node output
 *
 * @author Simone Schirinzi
 */
public class AggregatorNode extends AbstractActor {
    /**
     * Number of node to aggregate
     */
    private final int localSize;

    /**
     * Record of received values
     * It only contains those received for which
     * the cluster has not yet been calculated
     */
    class Pair {
        long counter;
        final HashSet<Integer> indices;
        Pair(){counter = 0; indices = new HashSet<>();}
    } private final HashMap<Long,Pair> values;

    /**
     * Ref to master
     */
    private final ActorRef master;

    /**
     * Create actor
     * @param localSize Number of node to aggregate
     * @param master Ref to master
     */
    public AggregatorNode(int localSize, ActorRef master){
        this.localSize = localSize;
        this.master = master;
        this.values = new HashMap<>();

        master.tell(new Hello(),self());
    }

    /**
     * Receive value from localSize nodes.
     * Check and memorize only which their message is > 0.
     * Send partial exemplar set to master
     *
     * @see Value
     * @return actor message receiver
     */
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Value.class, value ->{
                    if(! values.containsKey(value.iteration)) values.put(value.iteration, new Pair());
                    Pair current = values.get(value.iteration);
                    current.counter++;
                    if(value.value > 0) current.indices.add(value.sender);
                    if(current.counter == localSize){
                        master.tell(new LocalExemplars(value.iteration,current.indices),self());
                        values.remove(value.iteration);
                    }
                })
                .match(Ready.class, msg ->{
                    //if is deployed on the same system with other node, it kill every nodes
                    context().system().terminate();
                })
                .build();
    }
}
