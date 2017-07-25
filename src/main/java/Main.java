import akka.actor.*;
import akka.remote.RemoteScope;
import com.typesafe.config.ConfigFactory;

import java.sql.Time;

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
        String base = "C://Users/Simone/Dropbox/Università/Affinity Propagation/Dataset/";
        String default_file = base + "actorOutS.txt";
        if(args.length > 1) //noinspection UnusedAssignment
            default_file = args[1];

        Timer timer = new Timer();
        timer.start();
        double[][] graph = Util.buildGraph(default_file," ",null,true,Constant.sigma);
        timer.stop();
        System.out.println("File read in: " + timer);
        int size = graph.length;

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
        ActorRef aggregator = system.actorOf(Aggregator.props(graph, size),"aggregator");
        ActorRef dispatcher = system.actorOf(Dispatcher.props(graph, size), "creator");

        /* Node deploy */
        for(int i = 0; i < size && nodes_address.length > 0 ; i++)
            system.actorOf(Props.create(Node.class,aggregator,dispatcher)
                    .withDeploy(new Deploy(new RemoteScope(nodes_address[i % nodes_address.length]))));

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