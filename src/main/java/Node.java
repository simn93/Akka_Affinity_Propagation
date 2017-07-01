import akka.actor.*;
import scala.concurrent.duration.Duration;
import java.util.Optional;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Created by Simo on 03/06/2017.
 */
class Node extends AbstractActor{
    // Connection variable
    private ActorRef dispatcher;
    private String path;
    //------------------
    // Received from Initialize
    //Similarity
    private double[] s_row;
    private double[] s_col;

    //Lambda factor
    private double lambda;

    //Id of Actor
    private int self;
    //------------------
    // Received from Neighbors
    private ActorRef[] neighbors;
    private ActorRef aggregator;
    private int size;
    //------------------
    // Iteration variable
    // Iteration counter
    private long iteration;

    // Memorization of value received
    // x_col[q] = x[q][this]
    private double[] r_col;
    // x_row[q] = x[this][q]
    private double[] a_row;

    // Received values counter
    private int r_received;
    private int a_received;
    //------------------

    // Optimizations for less messages
    private boolean optimize;
    private int is_infinity;
    private ActorRef[] not_infinite_neighbors;
    private int[] reference;

    public Node(String path){
        this.path = path;
        standardSetting();

        sendIdentifyRequest();
    }

    public Node(ActorRef dispatcher){
        this.dispatcher = dispatcher;
        standardSetting();

        dispatcher.tell(new Self(),self());
        getContext().watch(dispatcher);
        getContext().become(active, true);
    }

    private void standardSetting(){
        iteration = 0;

        r_received = 0;
        a_received = 0;

        optimize = true;
        is_infinity = 0;
    }

    private void sendIdentifyRequest() {
        getContext().actorSelection(path).tell(new Identify(path), self());
        getContext().system().scheduler()
                .scheduleOnce(Duration.create(3, SECONDS), self(), ReceiveTimeout.getInstance(), getContext().dispatcher(), self());
    }

    @Override
    public Receive createReceive() {
        return waiting;
    }

    private Receive active = receiveBuilder()
            // Messaggio di init
            .match(Initialize.class, init -> {
                this.lambda = init.lambda;
                this.s_col = init.similarity_col;
                this.s_row = init.similarity_row;
                this.self = init.selfID;
            })

            //Messaggio di pre-start
            .match(Neighbors.class, neighbors1 -> {
                this.neighbors = neighbors1.array;
                this.size = neighbors1.size;
                this.aggregator = neighbors1.aggregator;

                //To begin the availabilities are initialized to 0
                a_row = new double[size];
                //In the first iteration is set to s_col[i] - max{s_col[j]} j != i
                r_col = new double[size];

                if(optimize) {
                    int not_linked_neighbors = 0;
                    for (int i = 0; i < size; i++) {
                        if (Util.isMinDouble(s_col[i])){ //The node which cannot reach me
                            is_infinity++;
                            r_col[i] = Util.min_double; // r initialize
                        }

                        if(Util.isMinDouble(s_row[i])){ //The node i cannot reach
                            not_linked_neighbors++;
                            // a initialize : no need to do. is 0 to default
                        }
                    }
                    this.not_infinite_neighbors = new ActorRef[size - not_linked_neighbors];
                    this.reference = new int[size - not_linked_neighbors];

                    int j = 0;
                    for (int i = 0; i < size; i++) {
                        if (! Util.isMinDouble(s_row[i])) {
                            this.not_infinite_neighbors[j] = neighbors[i];
                            reference[j] = i;
                            j++;
                        }
                    }
                    assert(j == this.reference.length);
                    assert(j + this.not_infinite_neighbors.length == size);
                }

                dispatcher.tell(new Ready(),self());
            })

            // Messaggio di start
            .match(Start.class, msg -> {
                //System.out.println(self + " started!");
                sendResponsibility();
            })

            .match(Responsibility.class, responsibility -> {
                r_col[responsibility.sender] = (r_col[responsibility.sender] * lambda) + (responsibility.value * (1 - lambda));
                r_received++;

                if (r_received == size - is_infinity) {
                    r_received = 0;

                    sendAvailability();
                }
            })

            .match(Availability.class, availability -> {
                a_row[availability.sender] = (a_row[availability.sender] * lambda) + (availability.value * (1 - lambda));
                a_received++;

                if (a_received == size) {
                    a_received = 0;

                    //Iteration's end
                    if (this.iteration % 100 == 99)
                        aggregator.tell(new Value(r_col[self] + a_row[self], self, iteration), self());

                    if(self == 0)System.out.println("Iterazione " + iteration + " completata!");
                    sendResponsibility();

                    this.iteration++;
                }
            })

            .match(Die.class, msg -> System.out.println("Not usefull actor..."))
            .match(ReceiveTimeout.class, x -> { /*ignore*/ })
            .build();

    private Receive waiting = receiveBuilder()
            .match(ActorIdentity.class, identity -> {
                Optional<ActorRef> maybe_actor = identity.getActorRef();
                if (! maybe_actor.isPresent()) {
                    System.out.println("Remote actor not available: " + path);
                } else {
                    dispatcher = maybe_actor.get();
                    dispatcher.tell(new Self(),self());
                    getContext().watch(dispatcher);
                    getContext().become(active, true);
                    System.out.println("Remote actor available: " + path);
                }
            })
            .match(ReceiveTimeout.class, msg -> sendIdentifyRequest())
            .build();


    private void sendResponsibility(){
        int i = 0;
        if(optimize) {for (ActorRef neighbor : not_infinite_neighbors) {
                neighbor.tell(new Responsibility(r(reference[i]), self), self());
                i++;
            }
        } else {for (ActorRef neighbor : neighbors) {
                neighbor.tell(new Responsibility(r(i), self), self());
                i++;
            }
        }
    }

    private void sendAvailability(){
        // E' necessario inviare tutte le availability
        // perchè non è possibile sapere staticamente, guardando solo la similarity,
        // quale valore riceverò da nodi a distanza infinita.
        for (int i = 0; i < self; i++)
            neighbors[i].tell(new Availability(a(i), self), self());
        for (int i = self + 1; i < size; i++)
            neighbors[i].tell(new Availability(a(i), self), self());
        self().tell(new Availability(a(),self),self());
    }

    private double r(int k){
        double max = Util.min_double;
        // foreach except k
        for (int i = 0; i < k; i++)
            max = (max > (a_row[i] + s_row[i])) ? max : (a_row[i] + s_row[i]);//Math.max(max, a_row[i] + s_row[i]);
        for (int i = k + 1; i < size; i++)
            max = (max > (a_row[i] + s_row[i])) ? max : (a_row[i] + s_row[i]);//Math.max(max, a_row[i] + s_row[i]);

        return s_row[k] - max;
    }

    private double a(int i){
        double ret = r_col[self];
        for(int q = 0; q < size; q++)
            if(q != i && q != self && r_col[q] > 0.0)
                ret += r_col[q];

        return 0 < ret ? 0 : ret;//Math.min(0,ret);
    }

    private double a(){
        double ret = 0.0;
        for(int q = 0; q < size; q++)
            if(q != self && r_col[q] > 0.0)
                ret += r_col[q];

        return ret;
    }
}