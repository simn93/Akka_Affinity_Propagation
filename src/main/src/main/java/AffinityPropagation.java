import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.remote.RemoteScope;

import java.util.Date;
import java.util.HashMap;

/**
 * Main class
 * Start main system
 * Load setting
 * Parse args
 *
 * Deploy node
 *
 * @author Simone Schirinzi
 */

public class AffinityPropagation {
    public AffinityPropagation(
            String lineMatrix,
            String colMatrix,
            String lineFormat,
            int size,
            int subClusterSize,
            ActorSystem system,
            Address[] nodes_address,
            boolean verbose,
            double lambda,
            long enoughIterations,
            int sendEach,
            double sigma
    ){
        /*
         * Akka logger
         * Logging in Akka is not tied to a specific logging backend.
         * By default log messages are printed to STDOUT,
         * but you can plug-in a SLF4J logger or your own logger.
         * Logging is performed asynchronously
         * to ensure that logging has minimal performance impact.
         * Logging generally means IO and locks,
         * which can slow down the operations of your code if it was performed synchronously.
         */
        LoggingAdapter log = Logging.getLogger(system, this);

        StringBuilder infoMsg = new StringBuilder();
        infoMsg.append("Program Launched with: \n");
        infoMsg.append("DataSet: ").append(lineMatrix).append("\n");
        infoMsg.append("Line format: ").append(lineFormat).append("\n");
        infoMsg.append("Size: ").append(size).append("\n");
        infoMsg.append("Sub cluster size: ").append(subClusterSize).append("\n");
        infoMsg.append("Lambda: ").append(lambda).append("\n");
        infoMsg.append("Enough iteration: ").append(enoughIterations).append("\n");
        infoMsg.append("Send each: ").append(sendEach).append("\n");
        infoMsg.append("Sigma: ").append(sigma).append("\n");
        infoMsg.append("On Servers: ").append("\n");
        for(Address address : nodes_address){
            infoMsg.append("\t");
            infoMsg.append(address.toString());
            infoMsg.append("\n");
        }

        log.info(infoMsg.toString());

        /* Map for deploy */
        HashMap<Integer,Integer> clusterSize = new HashMap<>();
        for(int i=0, j=0; i<size;j++){
            if(i + subClusterSize < size) {
                clusterSize.put(j, subClusterSize);
                i += subClusterSize;
            } else {
                clusterSize.put(j, size - i);
                i += size - i;
            }
        }

        /* Nodes refs */
        ActorRef[] nodes = new ActorRef[size];

        /* Master actors */
        ActorRef aggregatorMaster = system.actorOf(Props.create(AggregatorMaster.class,clusterSize.size(),enoughIterations,verbose,log),"aggregator");
        ActorRef dispatcherMaster = system.actorOf(Props.create(DispatcherMaster.class,clusterSize.size()),"dispatcher");

        /* Aggregator and nodes deploy */
        int t = 0;
        for(int i=0; i<clusterSize.size(); i++) {
            Deploy deploy = new Deploy(new RemoteScope(nodes_address[i % nodes_address.length]));

            ActorRef aggregator = system.actorOf(Props.create(AggregatorNode.class, clusterSize.get(i),aggregatorMaster).withDeploy(deploy));
            for(int j=0; j<clusterSize.get(i); j++){
                nodes[t] = system.actorOf(Props.create(Node.class,aggregator,lambda,sendEach,verbose).withDeploy(deploy));
                t++;
            }
        }

        /* Dispatcher deploy */
        t = 0;
        for(int i=0; i<clusterSize.size(); i++){
            Deploy deploy = new Deploy(new RemoteScope(nodes_address[i % nodes_address.length]));
            t+= clusterSize.get(i);
            system.actorOf(Props.create(DispatcherNode.class,lineMatrix, colMatrix, lineFormat, sigma, t-clusterSize.get(i), t, nodes, dispatcherMaster).withDeploy(deploy));
        }
    }
}
