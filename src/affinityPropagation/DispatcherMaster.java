package affinityPropagation;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import java.util.ArrayList;

/**
 * Class for actor start synchronization
 * @author Simone Schirinzi
 */
public class DispatcherMaster extends AbstractActor {
    /**
     *
     */
    private ArrayList<ActorRef> dispatcher;

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
     * @param dispatcherSize size of active dispatcher
     */
    public DispatcherMaster(int dispatcherSize){
        this.dispatcherSize = dispatcherSize;
        this.dispatcher = new ArrayList<>();
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
                    dispatcher.add(sender());

                    if(ready == dispatcherSize){
                        for(ActorRef node: dispatcher){
                            node.tell(new Start(), self());
                        }
                        self().tell(akka.actor.PoisonPill.getInstance(), ActorRef.noSender());
                    }
                })
                .build();
    }
}
