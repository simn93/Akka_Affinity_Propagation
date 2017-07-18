import akka.actor.AbstractActor;
import akka.actor.ActorRef;

/**
 * Class for actor start synchronization
 * @author Simone Schirinzi
 */
public class DispatcherMaster extends AbstractActor {
    /**
     * Refs to nodes
     */
    private final ActorRef[] nodes;

    /**
     * Size of active local dispatcher
     */
    private final int dispatcherSize;

    /**
     * Number of local dispatcher
     * who have finished to initialize
     * their nodes
     */
    private int ready;

    /**
     * Create master
     * @param nodes Ref to nodes
     * @param dispatcherSize size of active dispatcher
     */
    public DispatcherMaster(ActorRef[] nodes, int dispatcherSize){
        this.nodes = nodes;
        this.dispatcherSize = dispatcherSize;
        this.ready = 0;
    }


    /**
     * Synchronization for partial dispatcher
     * At the end we start all nodes
     * @return receiver
     */
    @Override public Receive createReceive() {
        return receiveBuilder()
                .match(Ready.class, msg ->{
                    this.ready++;
                    if(ready == dispatcherSize){
                        for(ActorRef node: nodes) node.tell(new Start(), self());
                        self().tell(akka.actor.PoisonPill.getInstance(), ActorRef.noSender());
                    }
                })
                .build();
    }
}
