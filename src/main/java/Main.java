import akka.actor.*;
import akka.remote.RemoteScope;
import com.typesafe.config.ConfigFactory;

/**
 * Created by Simo on 17/06/2017.
 */
@SuppressWarnings("DefaultFileTemplate")
public class Main {
    //Cluster nodes_IP
    //Elenco di IP su cui effettuare il deploy dei nodi
    @SuppressWarnings("CanBeFinal")
    private static String[] nodes_IP = new String[]{"10.101.5.30"};
    @SuppressWarnings({"FieldCanBeLocal", "CanBeFinal"})
    private static int remotePort = 2553;

    //Variabile di debug
    //Effettua il deploy dei nodi in loopback
    private static final boolean debug = true;

    public static void main(String[] args) {
        if(debug)
            nodes_IP[0] = "127.0.0.1";

        if (args.length == 0 || args[0].equals("listen"))
            startLookupSystem();
        if (args.length == 0 || args[0].equals("create"))
            startSystem(args);

    }

    private static void startSystem(String[] args) {
        //Graph load
        String default_file = "./infinity_test.txt";
        if(args.length > 1) default_file = args[1];

        //String pref = "C:\\Users\\Simo\\Documents\\Dataset\\GeneFinding_p.txt";
        double[][] graph = Util.buildGraph(default_file, "  ", "", true, Constant.sigma);
        //double[][] graph = Util.buildGraph("C://Users/Simo/Documents/Dataset/exons.txt",-2,true,true, Constant.sigma);
        //double[][] graph = Util.buildGraphRandom(5000);
        int size = graph.length;

        //Util.printSimilarity(graph,size);
        //Address build
        Address[] nodes_address = new Address[nodes_IP.length];
        for(int i = 0; i < nodes_IP.length; i++)
            nodes_address[i] = new Address("akka.tcp", "lookupSystem", nodes_IP[i], remotePort);

        // args = ["create", "similarity_file", ...]
        // System start
        ActorSystem system;
        if(debug){  system = ActorSystem.create("creationSystem", ConfigFactory.load("testCreation")); }
        else {      system = ActorSystem.create("creationSystem", ConfigFactory.load("creation")); }

        // control actors
        ActorRef aggregator = system.actorOf(Aggregator.props(graph, size),"aggregator");
        ActorRef dispatcher = system.actorOf(Dispatcher.props(graph, size, aggregator), "creator");

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