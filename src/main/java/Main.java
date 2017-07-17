import akka.actor.*;
import akka.remote.RemoteScope;
import com.typesafe.config.ConfigFactory;

/**
 * Main class
 * first Load file
 * then Start system
 *
 * @author Simone Schirinzi
 */
public class Main {
    /**
     * List of IPs to deploy nodes
     */
    private final static String[] nodes_IP = new String[]{"10.101.5.30"};

    /**
     * The port on which the remote system stays awaiting deploy requests
     */
    @SuppressWarnings({"FieldCanBeLocal"}) private final static int remotePort = 2553;

    /**
     * Debug variable
     * Deploys loopback nodes
     */
    private static final boolean debug = true;

    /**
     * main method
     * @param args "listen" for remote system
     *             "create" for control system
     */
    public static void main(String[] args) {
        if(debug) nodes_IP[0] = "127.0.0.1";

        if (args.length == 0 || args[0].equals("listen"))
            startLookupSystem();
        if (args.length == 0 || args[0].equals("create"))
            startSystem(args);
    }

    /**
     * Load file
     * Start system
     * deploy nodes
     * @param args eventually input files
     */
    private static void startSystem(String[] args) {
        /* Graph load */
        String lineMatrix = "C:/Users/Simone/Dropbox/Università/Affinity Propagation/Dataset/infTest.txt";//exons_10k.txt";
        String colMatrix = "C:/Users/Simone/Dropbox/Università/Affinity Propagation/Dataset/infTestT.txt";//exonsT_10k.txt";
        int size = 456;
        int dispatcherSize = 5;

        /* Address build */
        Address[] nodes_address = new Address[nodes_IP.length];
        for(int i = 0; i < nodes_IP.length; i++)
            nodes_address[i] = new Address("akka.tcp", "lookupSystem", nodes_IP[i], remotePort);

        /* System start */
        ActorSystem system;
        if(debug)
            system = ActorSystem.create("creationSystem", ConfigFactory.load("testCreation"));
        else
            system = ActorSystem.create("creationSystem", ConfigFactory.load("creation"));

        /* create control actors */
        ActorRef aggregator = system.actorOf(Aggregator.props(size),"aggregator");

        /* Node deploy */
        ActorRef[] nodes = new ActorRef[size];
        for(int i = 0; i < size && nodes_address.length > 0 ; i++)
            nodes[i] = system.actorOf(Props.create(Node.class,aggregator)
                    .withDeploy(new Deploy(new RemoteScope(nodes_address[i % nodes_address.length]))));

        int interval = Math.round(size/dispatcherSize), from, to;
        for(int i = 0; i < dispatcherSize; i++){
            from = i * interval;
            to = (i+1) * interval;
            if(i == dispatcherSize - 1) to = size;
            system.actorOf(Dispatcher.props(lineMatrix, colMatrix, from, to, size, nodes), "creator"+i);
        }
        aggregator.tell(new Neighbors(nodes,size),ActorRef.noSender());

        System.out.println("Started CalculatorSystem");
    }

    private static void startLookupSystem() {
        if(debug)
            ActorSystem.create("lookupSystem", ConfigFactory.load("testLookup"));
        else
            ActorSystem.create("lookupSystem", ConfigFactory.load("remoteLookup"));

        System.out.println("Started LookupSystem");
    }
}