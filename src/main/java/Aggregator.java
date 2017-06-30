import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.ArrayList;

/**
 * Created by Simo on 05/06/2017.
 */
public class Aggregator extends AbstractActor {
    private double[][] similarity;
    private int size;

    private ArrayList<ActorRef> nodes;
    private ArrayList<Integer> exemplars;
    private int[] cluster;
    private int received;

    private LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);


    static public Props props(double[][] similarity, int size) {
        return Props.create(Aggregator.class, () -> new Aggregator(similarity,size));
    }

    public Aggregator(double[][] similarity, int size){
        this.similarity = similarity;
        this.size = size;

        init();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
        .match(Value.class, value -> {
            nodes.add(sender());
            received++;

            if (value.value > 0 && !exemplars.contains(value.sender))
                exemplars.add(value.sender);

            if(received == size){
                buildCluster();

                ActorRef[] nodes_copy = new ActorRef[nodes.size()];
                nodes.toArray(nodes_copy);
                init();

                for(ActorRef node : nodes_copy)
                    node.tell(new Start(),ActorRef.noSender());
            }
        })
        .build();
    }

    private void init(){
        this.nodes = new ArrayList<>();
        this.exemplars = new ArrayList<>();
        this.cluster = new int[size];
        this.received = 0;
    }

    private void buildCluster(){
        //Creazione del cluster
        for (int i = 0; i < size; i++) {
            Double max = Util.min_double;
            int index = -1;
            for (Integer k : exemplars) {
                if (max < similarity[i][k]) {
                    max = similarity[i][k];
                    index = k;
                }
            }
            cluster[i] = index;
        }

        for (int i = 0; i < size; i++) {
            log.info(i + " belong to " + cluster[i]);
        }

        // exemplars : arraylist of integer
        Object[] o = exemplars.toArray();
        int[] e = new int[exemplars.size()];

        for(int i=0;i<e.length;i++)
            e[i]=(int)o[i];

        (new VisualGraph(e,cluster)).show(800,800);
    }
}
