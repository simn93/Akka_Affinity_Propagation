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
@SuppressWarnings("DefaultFileTemplate")
class Aggregator extends AbstractActor {
    private final double[][] similarity;
    @SuppressWarnings("CanBeFinal")
    private int size;

    private final HashMap<Long,double[]> values;
    private int[] previousCluster;
    private long previousClusterIteration;

    private ArrayList<ActorRef> nodes;

    @SuppressWarnings("CanBeFinal")
    private Timer timer;
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    static Props props(double[][] similarity, int size) {
        return Props.create(Aggregator.class, () -> new Aggregator(similarity,size));
    }

    private Aggregator(double[][] similarity, int size){
        this.similarity = similarity;
        this.size = size;
        this.values = new HashMap<>();

        timer = new Timer();
        timer.Start();
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

                //se il cluster non cambia per k volte
                if(!isChanged(buildCluster(e),value.iteration) &&
                        value.iteration - previousClusterIteration > Constant.enoughIterations){
                    //termino tutto
                    getContext().become(killMode,true);
                }
                values.remove(value.iteration);
            }
        })
        .build();
    }

    private final Receive killMode = receiveBuilder()
            .match(Value.class, msg -> {
                if(nodes == null) nodes = new ArrayList<>();
                if(!nodes.contains(sender())) nodes.add(sender());
                if(nodes.size() == size){
                    for(ActorRef actorRef : nodes)
                        actorRef.tell(akka.actor.PoisonPill.getInstance(),ActorRef.noSender());

                    timer.Stop();
                    log.info("Job done U_U after " + previousClusterIteration + " iterations and " + timer);
                    context().system().terminate();
                }
            })
            .build();

    private int[] buildCluster(int[] exemplars){
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

        //for (int i = 0; i < size; i++)
            //log.info(i + " belong to " + cluster[i]);

        //(new VisualGraph(exemplars,cluster)).show(800,800);
        return cluster;
    }

    private boolean isChanged(int[] cluster, long iteration){
        //Se non esiste : prima iterazione
        if(previousCluster == null){
            previousCluster = cluster;
            previousClusterIteration = iteration;
            return true;
        }

        //Se c'è qualche differenza dal precedente
        for(int i = 0; i < size; i++){
            if(previousCluster[i] != cluster[i]){
                previousCluster = cluster;
                previousClusterIteration = iteration;
                return true;
            }
        }

        //Se non è cambiato
        return false;
    }
}