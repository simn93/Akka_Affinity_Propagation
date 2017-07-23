import akka.actor.*;
import akka.remote.RemoteScope;
import com.typesafe.config.ConfigFactory;

import java.util.HashMap;

/**
 * Main class
 * first Load file
 * then Start system
 *
 * @author Simone Schirinzi
 */
@SuppressWarnings("SpellCheckingInspection")
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
    @SuppressWarnings("unused")
    private static void startSystem(String[] args) {
        /* Graph load */
        /* Compressed File: zip of lines of bytes of similarity matrix*/
        String lineMatrix = "C:/Users/Simo/Dropbox/Università/Affinity Propagation/Dataset/geneFind.zip";
        String colMatrix = "C:/Users/Simo/Dropbox/Università/Affinity Propagation/Dataset/geneFindT.zip";
        int size = 75067;
        int subClusterSize = 1000;

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


        int interval, from, to;

        /**/
        HashMap<Integer,Integer> clustMap = new HashMap<>();
        HashMap<Integer,Integer> clustSize = new HashMap<>();
        for(int i=0, j=0; i<size;j++){
            if(i + subClusterSize < size) {
                clustSize.put(j, subClusterSize);
                i += subClusterSize;
            } else {
                clustSize.put(j, size - i);
                i += size - i;
            }
        }

        ActorRef[] nodes = new ActorRef[size];

        ActorRef aggregatorMaster = system.actorOf(Props.create(AggregatorMaster.class,clustSize.size()),"aggregator");
        ActorRef dispatcherMaster = system.actorOf(Props.create(DispatcherMaster.class,clustSize.size()),"dispatcher");

        int t = 0;
        for(int i=0; i<clustSize.size(); i++) {
            Deploy deploy = new Deploy(new RemoteScope(nodes_address[i % nodes_address.length]));

            ActorRef aggregator = system.actorOf(Props.create(AggregatorNode.class, clustSize.get(i),aggregatorMaster).withDeploy(deploy));
            for(int j=0; j<clustSize.get(i); j++){
                nodes[t] = system.actorOf(Props.create(Node.class,aggregator).withDeploy(deploy));
                t++;
            }
            //ActorRef dispatcher = system.actorOf(Props.create(DispatcherNode.class,lineMatrix, colMatrix, t-clustSize.get(i), t, size, nodes, dispatcherMaster).withDeploy(deploy));
        }
        assert (t==size);

        t = 0;
        for(int i=0; i<clustSize.size(); i++){
            Deploy deploy = new Deploy(new RemoteScope(nodes_address[i % nodes_address.length]));
            t+= clustSize.get(i);
            ActorRef dispatcher = system.actorOf(Props.create(DispatcherNode.class,lineMatrix, colMatrix, t-clustSize.get(i), t, size, nodes, dispatcherMaster).withDeploy(deploy));
        }
        aggregatorMaster.tell(new Neighbors(nodes,size),ActorRef.noSender());
        dispatcherMaster.tell(new Neighbors(nodes,size),ActorRef.noSender());
        /**/

        /* create control actors *//*
        interval = Math.floorDiv(size,aggregatorSize);


        ActorRef[] aggregator = new ActorRef[aggregatorSize];
        int[] aggLink = new int[size];

        for(int i = 0; i < aggregatorSize; i++) {
            from = i * interval;
            to = (i+1) * interval;
            if(i == aggregatorSize - 1) to = size;
            aggregator[i] = system.actorOf(Props.create(AggregatorNode.class,to-from,aggregatorMaster));
        }

        int q;
        for(q = 0; q < interval*aggregatorSize; q++) aggLink[q] = (q % aggregatorSize);
        for(;q<size;q++) aggLink[q] = aggregatorSize-1;

        *//* Node deploy *//*
        ActorRef[] nodes = new ActorRef[size];
        for(int i = 0; i < size && nodes_address.length > 0 ; i++) {
            nodes[i] = system.actorOf(Props.create(Node.class, aggregator[aggLink[i]])
                    .withDeploy(new Deploy(new RemoteScope(nodes_address[i % nodes_address.length]))));
        }

        *//* DispatcherNode build *//*
        ActorRef dispatcherMaster = system.actorOf(Props.create(DispatcherMaster.class,nodes,dispatcherSize));

        interval = Math.round(size/dispatcherSize);
        for(int i = 0; i < dispatcherSize; i++){
            from = i * interval;
            to = (i+1) * interval;
            if(i == dispatcherSize - 1) to = size;
            system.actorOf(Props.create(DispatcherNode.class,lineMatrix, colMatrix, from, to, size, nodes, dispatcherMaster), "creator"+i);
        }
        aggregatorMaster.tell(new Neighbors(nodes,size),ActorRef.noSender());*/

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