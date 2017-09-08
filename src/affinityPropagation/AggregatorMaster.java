package affinityPropagation;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
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
    class Pair {
        long counter;
        final HashSet<Integer> indices;
        Pair(){counter = 0; indices = new HashSet<>();}
    }  private Pair exemplar;

    /**
     * Value of the last iteration in which the cluster has changed
     */
    private long exemplarIt;

    /**
     * Sets of partial parcels received by local aggregator
     */
    private final HashMap<Long,Pair> exemplars;

    /**
     *
     */
    private final long enoughIterations;

    /**
     *
     */
    private final boolean verbose;

    /**
     *
     */
    private final ActorRef log;

    /**
     * Create the class
     *
     * Start the timer
     *
     * @param localAggregatorSize size of local aggregator
     */
    public AggregatorMaster(int localAggregatorSize, long enoughIterations, boolean verbose, ActorRef log){
        this.localAggregatorSize = localAggregatorSize;
        this.enoughIterations = enoughIterations;
        this.verbose = verbose;
        this.log = log;

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
                        if(verbose) log.tell("Waiting exemplars list: " + exemplars.size(),ActorRef.noSender());

                        /* Semantics Change: Stores the iteration it refers to */
                        current.counter = localExemplar.iteration;

                        /* I'm calculating an old cluster: there is a framework problem */
                        assert(exemplar.counter < current.counter) : exemplar.counter + " " + current.counter;

                        /* Check that the set of specimens has not changed.
                         * If it has changed, I see the differences
                         * and update the reference to the iteration counter.
                         */
                        if(! exemplar.indices.equals(current.indices)){
                            if(verbose)log.tell("It: " + current.counter + " Ex Size: " + exemplar.indices.size() + " Diff: " + difference(exemplar.indices,current.indices).toString(),ActorRef.noSender());
                            exemplarIt = current.counter;
                        }

                        /* In any case I update the reference to the last calculated cluster. */
                        exemplar = current;
                        exemplars.remove(localExemplar.iteration);

                        /* End check */
                        if(localExemplar.iteration - exemplarIt > enoughIterations){
                            timer.stop();
                            log.tell("Job done U_U after " + exemplarIt + " iterations and " + timer, ActorRef.noSender());

                            for(ActorRef node : aggregatorRef) node.tell(new Ready(),ActorRef.noSender());
                            context().system().terminate();
                        }
                    }
                })
                .match(Hello.class, msg -> aggregatorRef.add(sender()))
                .build();
    }

    /**
     * Perform union over 2 set
     * @param list1 first set
     * @param list2 second set
     * @return union of sets
     */
    private HashSet<Integer> union(HashSet<Integer> list1, HashSet<Integer> list2) {
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
    private HashSet<Integer> intersection(HashSet<Integer> list1, HashSet<Integer> list2) {
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
    private HashSet<Integer> difference(HashSet<Integer> list1, HashSet<Integer> list2){
        HashSet<Integer> listU = union(list1,list2);
        HashSet<Integer> listI = intersection(list1,list2);
        listU.removeAll(listI);
        return listU;
    }
}