import akka.actor.AbstractActor;
import akka.actor.ActorRef;

/**
 * Created by Simo on 18/07/2017.
 */
public class DispatcherMaster extends AbstractActor {
    /**
     *
     */
    private final ActorRef[] nodes;

    /**
     *
     */
    private final int dispatcherSize;

    private int ready;

    public DispatcherMaster(ActorRef[] nodes, int dispatcherSize){
        this.nodes = nodes;
        this.dispatcherSize = dispatcherSize;
        this.ready = 0;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Ready.class, msg ->{
                    this.ready++;
                    if(ready == dispatcherSize)
                        for(ActorRef node: nodes)
                            node.tell(new Start(),self());
                })
                .build();
    }
}
