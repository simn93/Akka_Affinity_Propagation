import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import scala.Int;

import java.util.*;

/**
 * Class for cluster computing
 *
 * @author Simone Schirinzi
 */
class Aggregator extends AbstractActor {
    /**
     * number of nodes
     */
    private final int size;

    /**
     * Record of received values
     * It only contains those received for which
     * the cluster has not yet been calculated
     * <Iteration_number,Values_received,Exemplar_list>
     */
    private class Pair {
        public long counter;
        public HashSet<Integer> indices;
        public Pair(){counter = 0; indices = new HashSet<>();}
    } private final HashMap<Long,Pair> values;

    /**
     * Reference to the last cluster calculated
     * other than the ones previously calculated.
     * Useful to check if a new cluster emerged.
     */
    private Pair exemplar;

    /**
     * Reference list of nodes.
     * It is used at termination of the algorithm.
     * It is used to send all the nodes
     * at the same time a termination message.
     */
    private ActorRef[] nodes;

    /**
     * Timer for time control.
     */
     private final Timer timer;

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

    static Props props(int size) {
        return Props.create(Aggregator.class, () -> new Aggregator(size));
    }

    /**
     * Create the class
     *
     * Start the timer
     *
     * @param size of nodes
     */
    private Aggregator(int size){
        this.size = size;
        this.values = new HashMap<>();
        this.exemplar = new Pair();
        this.exemplar.counter = -1;

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
     * /**
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
     *
     * @see Value
     * @return receive handler
     */
    @Override
    public Receive createReceive() {
        return receiveBuilder()
        .match(Value.class, value -> {
            /* HashMap vector creation */
            if(! values.containsKey(value.iteration)) values.put(value.iteration, new Pair());
            //if(values.size() > 10) System.out.println(values.size() + " too big");
            Pair current = values.get(value.iteration);

            /* save the value into the vector refereed to iteration value */
            /* increase the number of value received refereed to iteration value */
            current.counter++;
            if(value.value > 0) current.indices.add(value.sender);

            /* can compute the cluster */
            if(current.counter == size){
                current.counter = value.iteration;
                assert(exemplar.counter < current.counter) : exemplar.counter + " " + current.counter;
                if(! exemplar.indices.equals(current.indices)){
                    log.info(current.counter + " " + difference(exemplar.indices,current.indices).toString());
                    exemplar = current;
                    //log.info(exemplar.indices.toString());
                }

                values.remove(value.iteration);

                if(value.iteration - exemplar.counter > Constant.enoughIterations){
                    timer.stop();
                    log.info("Job done U_U after " + exemplar.counter + " iterations and " + timer);

                    for(ActorRef node : nodes) node.tell(akka.actor.PoisonPill.getInstance(),ActorRef.noSender());
                    context().system().terminate();
                }
            }
        })
        .match(Neighbors.class, msg -> this.nodes = msg.array)
        .build();
    }

    public HashSet<Integer> union(HashSet<Integer> list1, HashSet<Integer> list2) {
        HashSet<Integer> set = new HashSet<Integer>();

        set.addAll(list1);
        set.addAll(list2);

        return set;
    }

    public HashSet<Integer> intersection(HashSet<Integer> list1, HashSet<Integer> list2) {
        HashSet<Integer> list = new HashSet<Integer>();

        for (Integer t : list1) {
            if(list2.contains(t)) {
                list.add(t);
            }
        }

        return list;
    }

    public HashSet<Integer> difference(HashSet<Integer> list1, HashSet<Integer> list2){
        HashSet<Integer> listU = union(list1,list2);
        HashSet<Integer> listI = intersection(list1,list2);
        listU.removeAll(listI);
        return listU;
    }
}