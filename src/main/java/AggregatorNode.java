import akka.actor.AbstractActor;
import akka.actor.ActorRef;

import java.util.HashMap;

/**
 * Created by Simo on 18/07/2017.
 */
public class AggregatorNode extends AbstractActor {
    /**
     *
     */
    private final int localSize;

    /**
     *
     */
    private final HashMap<Long,Pair> values;

    /**
     *
     */
    private final ActorRef master;

    /**
     *
     * @param localSize
     */
    public AggregatorNode(int localSize, ActorRef master){
        this.localSize = localSize;
        this.master = master;

        this.values = new HashMap<>();
    }

    /**
     *
     * @return
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
                .build();
    }
}
