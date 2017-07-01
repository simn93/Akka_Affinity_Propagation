import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Simo on 05/06/2017.
 */
class Aggregator extends AbstractActor {
    private double[][] similarity;
    private int size;

    private HashMap<Long,double[]> values;
    //private ArrayList<ActorRef> nodes;
    //private ArrayList<Integer> exemplars;
    //private int[] cluster;

    private LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    static public Props props(double[][] similarity, int size) {
        return Props.create(Aggregator.class, () -> new Aggregator(similarity,size));
    }

    private Aggregator(double[][] similarity, int size){
        this.similarity = similarity;
        this.size = size;
        this.values = new HashMap<>();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
        .match(Value.class, value -> {
            if(! values.containsKey(value.iteration))
                values.put(value.iteration,new double[size+1]);

            //current[size] = numero di valori ricevuti dai nodi per quell'iterazione
            double[] current = values.get(value.iteration);
            current[value.sender] = value.value;
            current[size] += 1;

            if(current[size] == size){
                ArrayList<Integer> exemplars = new ArrayList<>();
                for(int i = 0; i < size; i++)
                    if(current[i] > 0 && !exemplars.contains(i))
                        exemplars.add(i);

                int[] e = new int[exemplars.size()];
                for(int i= 0; i < exemplars.size(); i++) e[i] = exemplars.get(i);
                buildCluster(e);

                values.remove(value.iteration);
            }
        })
        .build();
    }

    private void buildCluster(int[] exemplars){
        int[] cluster = new int[size];
        //Creazione del cluster
        for (int i = 0; i < size; i++) {
            Double max = Util.min_double;
            int index = -1;
            for (int k : exemplars) {
                if (max < similarity[i][k]) {
                    max = similarity[i][k];
                    index = k;
                }
            }
            cluster[i] = index;
        }

        for (int i = 0; i < size; i++)
            log.info(i + " belong to " + cluster[i]);

        (new VisualGraph(exemplars,cluster)).show(800,800);
    }
}
