import akka.actor.*;
import akka.remote.RemoteScope;
import com.typesafe.config.ConfigFactory;

/**
 * Created by Simo on 17/06/2017.
 */
public class Main {
    //Graph size
    private static int size = -1;
    //Cluster nodes_IP
    private static String[] nodes_IP;
    //Graph matrix
    private static double[][] Graph;
    //Algorithm lambda
    private final static Double lambda = 0.8;

    private static boolean debug = true;

    public static void main(String[] args) {
        // TODO: aggiustare input
        nodes_IP = new String[]{"10.101.5.30"};
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

        String default_file = "./infinity_test.txt";
        if(args.length > 1) default_file = args[1];

        Graph = Util.buildGraph(default_file,"  ","",true,0);
        //double d = Util.min_double;
        //Graph = new double[][]{{-5,-3,d,d},{d,-5,d,-2},{d,-3,-5,-2},{d,d,-6,-5}};
        size = Graph.length;

        ActorRef aggregator = system.actorOf(Aggregator.props(Graph,size),"aggregator");
        ActorRef dispatcher = system.actorOf(Dispatcher.props(size,Graph,lambda,aggregator), "creator");

        Address[] nodes_address = new Address[nodes_IP.length];
        for(int i = 0; i < nodes_IP.length; i++)
            nodes_address[i] = new Address("akka.tcp", "lookupSystem", nodes_IP[i], 2553);

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