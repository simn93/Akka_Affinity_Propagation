package affinityPropagation;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;

import java.util.HashMap;

public class NodeActor extends AbstractActor {
    // Connection variable

    /**
     * Link to the actor responsible for generating the cluster.
     * Received when creating the node.
     * Each node periodically sends a Value () message to check the algorithm's performance.
     */
    public final ActorRef aggregator;

    /**
     * lambda factor for dumping messages update
     */
    public final double lambda;

    /**
     * Set how many iterations wait for send message to aggregator
     */
    public final int sendEach;

    /**
     * Flag for detailed log
     */
    public final boolean verbose;

    /**
     * Ref to log
     */
    public final ActorRef log;

    /*
     * Number of node to run
     *
    private final int nodes;
    */
    //------------------

    /**
     * Map of nodes runned
     */
    private final HashMap<Integer,Node> nodesMap;

    public NodeActor(ActorRef aggregator, double lambda, int sendEach, boolean verbose, ActorRef log) {
        this.aggregator = aggregator;
        this.lambda = lambda;
        this.sendEach = sendEach;
        this.verbose = verbose;
        this.log = log;
        this.nodesMap = new HashMap<>();
    }

    /**
     * Receive builder
     * @return Node message manager
     * @see NodeSetting
     * @see Start
     * @see Responsibility
     * @see Availability
     */
    @Override public Receive createReceive() {
        return receiveBuilder()
                .match(NodeSetting.class, msg -> {
                    nodesMap.computeIfAbsent(msg.selfID, k -> new Node(this));
                    nodesMap.get(msg.selfID).initializeHandler(msg);
                    sender().tell(new Ready(), self());
                })
                .match(Start.class, msg -> {
                    for(Node node : nodesMap.values())node.sendResponsibility();
                })
                .match(Responsibility.class, msg -> {
                    nodesMap.get(msg.selfID).responsibilityHandler(msg);
                })
                .match(Availability.class, msg -> {
                    nodesMap.get(msg.selfID).availabilityHandler(msg);
                })
                .build();
    }

    public void tell(ActorRef dest, Object msg){
        dest.tell(msg,self());
    }
}
