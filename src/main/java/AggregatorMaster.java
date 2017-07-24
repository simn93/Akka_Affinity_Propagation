import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Aggregator master
 * Receive partial exemplars set from other aggregator.
 * join them and check difference
 *
 * @author Simone Schirinzi
 */

public class AggregatorMaster extends AbstractActor{
    /**
     * Timer for counting time used
     */
    private final Timer timer;

    /**
     * Number of partial aggregator started
     */
    private final int localAggregatorSize;

    /**
     *
     */
    private final ArrayList<ActorRef> aggregatorRef;

    /**
     * Last set of calculated specimens.
     * We also remember the iteration to which it corresponds
     */
    private Pair exemplar;

    /**
     * Value of the last iteration in which the cluster has changed
     */
    private long exemplarIt;

    /**
     * Sets of partial parcels received by local aggregator
     */
    private final HashMap<Long,Pair> exemplars;

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

    /**
     * Reference list of nodes.
     * It is used at termination of the algorithm.
     * It is used to send all the nodes
     * at the same time a termination message.
     */
    private ActorRef[] nodes;

    /**
     * Create the class
     *
     * Start the timer
     *
     * @param localAggregatorSize size of local aggregator
     */
    public AggregatorMaster(int localAggregatorSize){
        this.localAggregatorSize = localAggregatorSize;
        this.exemplar = new Pair();
        this.exemplars = new HashMap<>();
        this.aggregatorRef = new ArrayList<>();
        this.exemplar.counter = -1;

        this.timer = new Timer();
        timer.start();
    }

    /**
     * Akka poisonPill
     * To kill an actor You can also send the akka.actor.PoisonPill message,
     * which will stop the actor when the message is processed.
     * PoisonPill is enqueued as ordinary messages
     * and will be handled after messages
     * that were already queued in the mailbox.
     *
     * @see Value
     * @see Neighbors
     * @return receive handler
     */
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                /* Partial set of exemplar */
                .match(LocalExemplars.class, localExemplar -> {
                    if(! exemplars.containsKey(localExemplar.iteration)) exemplars.put(localExemplar.iteration, new Pair());
                    Pair current = exemplars.get(localExemplar.iteration);
                    /* Count the number of messages received */
                    current.counter++;
                    current.indices.addAll(localExemplar.exemplars);

                    /* All local aggregator have send their set */
                    if(current.counter == localAggregatorSize){

                        /* Semantics Change: Stores the iteration it refers to */
                        current.counter = localExemplar.iteration;

                        /* I'm calculating an old cluster: there is a framework problem */
                        assert(exemplar.counter < current.counter) : exemplar.counter + " " + current.counter;

                        /* Check that the set of specimens has not changed.
                         * If it has changed, I see the differences
                         * and update the reference to the iteration counter.
                         */
                        if(! exemplar.indices.equals(current.indices)){
                            log.info(current.counter + " " + difference(exemplar.indices,current.indices).toString());
                            exemplarIt = current.counter;
                        }

                        /* In any case I update the reference to the last calculated cluster. */
                        exemplar = current;
                        exemplars.remove(localExemplar.iteration);

                        /* End check */
                        if(localExemplar.iteration - exemplarIt > Constant.enoughIterations){
                            timer.stop();
                            log.info("Job done U_U after " + exemplarIt + " iterations and " + timer);

                            for(ActorRef node : aggregatorRef) node.tell(new Ready(),ActorRef.noSender());
                            context().system().terminate();
                        }
                    }
                })
                .match(Hello.class, msg ->{
                    aggregatorRef.add(sender());})
                .match(Neighbors.class, msg -> this.nodes = msg.array)
                .build();
    }

    /**
     * Perform union over 2 set
     * @param list1 first set
     * @param list2 second set
     * @return union of sets
     */
    public HashSet<Integer> union(HashSet<Integer> list1, HashSet<Integer> list2) {
        HashSet<Integer> set = new HashSet<>();

        set.addAll(list1);
        set.addAll(list2);

        return set;
    }

    /**
     * Perform intersection over 2 set
     * @param list1 first set
     * @param list2 second set
     * @return intersection of sets
     */
    public HashSet<Integer> intersection(HashSet<Integer> list1, HashSet<Integer> list2) {
        HashSet<Integer> list = new HashSet<>();

        for (Integer t : list1) {
            if(list2.contains(t)) {
                list.add(t);
            }
        }

        return list;
    }

    /**
     * Return set contains only difference between due sets
     * @param list1 first set
     * @param list2 second set
     * @return difference of sets
     */
    public HashSet<Integer> difference(HashSet<Integer> list1, HashSet<Integer> list2){
        HashSet<Integer> listU = union(list1,list2);
        HashSet<Integer> listI = intersection(list1,list2);
        listU.removeAll(listI);
        return listU;
    }
}
