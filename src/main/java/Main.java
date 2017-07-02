import akka.actor.*;
import akka.remote.RemoteScope;
import com.typesafe.config.ConfigFactory;

/**
 * Created by Simo on 17/06/2017.
 */
public class Main {
    //Cluster nodes_IP
    //Elenco di IP su cui effettuare il deploy dei nodi
    private static String[] nodes_IP = new String[]{"10.101.5.30"};
    private static int remotePort = 2553;

    //Algorithm lambda
    private final static Double lambda = 0.8;

    //Variabile di debug
    //Effettua il deploy dei nodi in loopback
    private static boolean debug = true;

    public static void main(String[] args) {
        if(debug)
            nodes_IP[0] = "127.0.0.1";

        if (args.length == 0 || args[0].equals("create"))
            startSystem(args);
        if (args.length == 0 || args[0].equals("listen"))
            startLookupSystem();
    }

    private static void startSystem(String[] args) {
        // args = ["create", "similarity_file", ...]
        ActorSystem system;
        if(debug){
            system = ActorSystem.create("creationSystem", ConfigFactory.load("testCreation"));
        } else {
            system = ActorSystem.create("creationSystem", ConfigFactory.load("creation"));
        }

        String default_file = "C:/Users/Simo/Downloads/FaceClusteringSimilarities.txt";//"./infinity_test.txt";
        if(args.length > 1) default_file = args[1];

        String pref = "C:/Users/Simo/Downloads/FaceClusteringPreferences.txt";
        double[][] graph = Util.buildGraph(default_file, "  ", pref, false, 0);
        int size = graph.length;

        Util.printSimilarity(graph,size);

        ActorRef aggregator = system.actorOf(Aggregator.props(graph, size),"aggregator");
        ActorRef dispatcher = system.actorOf(Dispatcher.props(graph, size,lambda,aggregator), "creator");

        //Address build
        Address[] nodes_address = new Address[nodes_IP.length];
        for(int i = 0; i < nodes_IP.length; i++)
            nodes_address[i] = new Address("akka.tcp", "lookupSystem", nodes_IP[i], remotePort);

        //Node deploy
        for(int i = 0; i < size && nodes_address.length > 0 ; i++)
            system.actorOf(Props.create(Node.class,dispatcher).withDeploy(new Deploy(new RemoteScope(nodes_address[i % nodes_address.length]))));

        System.out.println("Started CalculatorSystem");
    }

    private static void startLookupSystem() {
        if(debug){
            ActorSystem.create("lookupSystem", ConfigFactory.load("testLookup"));
        } else {
            ActorSystem.create("lookupSystem", ConfigFactory.load("remoteLookup"));
        }
        System.out.println("Started LookupSystem");
    }
}