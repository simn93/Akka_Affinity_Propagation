import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

/**
 * Created by Simo on 18/06/2017.
 */
@SuppressWarnings("DefaultFileTemplate")
class Dispatcher extends AbstractActor {
    private final int size;
    private final double[][] Graph;
    private final ActorRef aggregator;

    private final ActorRef[] array;
    private int index;

    private int ready;

    static Props props(double[][] Graph, int size, ActorRef aggregator) {
        return Props.create(Dispatcher.class, () -> new Dispatcher(Graph,size,aggregator));
    }

    private Dispatcher(double[][] Graph, int size, ActorRef aggregator){
        this.size = size;
        this.Graph = Graph;
        this.aggregator = aggregator;

        this.array = new ActorRef[size];
        this.index = 0;
        this.ready = 0;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
        .match(Self.class, msg -> {
            try {
                if(! Util.contains(array,sender())) {
                    double[] row = new double[size];
                    double[] col = new double[size];

                    for (int j = 0; j < size; j++) {
                        row[j] = Graph[index][j];
                        col[j] = Graph[j][index];
                    }

                    array[index] = sender();

                    sender().tell(new Initialize(row, col, index), ActorRef.noSender());
                    index++;

                    //System.out.println("Actor " + (index-1) + " " + sender() +" started!");
                    //Can start
                    if (index == size) {
                        Neighbors neighbors = new Neighbors(array, size, aggregator);
                        for (int i = 0; i < size; i++) {
                            array[i].tell(neighbors, ActorRef.noSender());
                        }
                        System.out.println("Started " + index + " actor");
                    }
                }
            } catch (IndexOutOfBoundsException e){
                sender().tell(new Die(),self());
            }
        })
        .match(Ready.class, msg -> {
            // aspetto che tutti gli attori ricevano il messaggio di creazione prima di cominciare, altrimenti ricevono messaggi da altri attori già pronti, ma li mettono in strutture non pronte
            this.ready++;
            if(ready == size)
                for(int i = 0; i < size; i++)
                    array[i].tell(new Start(),ActorRef.noSender());
        })
        .build();
    }
}
