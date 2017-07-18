import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import java.util.HashMap;
import java.util.HashSet;

/**
 *
 */

public class AggregatorMaster extends AbstractActor{
    /**
     *
     */
    private final Timer timer;

    /**
     *
     */
    private final int localAggregatorSize;

    /**
     *
     */
    private Pair exemplar;

    /**
     *
     */
    private HashMap<Long,Pair> exemplars;

    /**
     *
     */
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    /**
     *
     */
    private ActorRef[] nodes;

    /**
     *
     */
    public AggregatorMaster(int localAggregatorSize){
        this.localAggregatorSize = localAggregatorSize;
        this.exemplar = new Pair();
        this.exemplars = new HashMap<>();

        this.exemplar.counter = -1;

        this.timer = new Timer();
        timer.start();
    }

    /**
     *
     * @return
     */
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(LocalExemplars.class, localExemplar -> {
                    if(! exemplars.containsKey(localExemplar.iteration)) exemplars.put(localExemplar.iteration, new Pair());
                    Pair current = exemplars.get(localExemplar.iteration);
                    current.counter++;
                    current.indices.addAll(localExemplar.exemplars);
                    if(current.counter == localAggregatorSize){
                        current.counter = localExemplar.iteration;
                        assert(exemplar.counter < current.counter) : exemplar.counter + " " + current.counter;
                        if(! exemplar.indices.equals(current.indices)){
                            log.info(current.counter + " " + difference(exemplar.indices,current.indices).toString());
                            exemplar = current;
                        }
                        exemplars.remove(localExemplar.iteration);
                        if(localExemplar.iteration - exemplar.counter > Constant.enoughIterations){
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
