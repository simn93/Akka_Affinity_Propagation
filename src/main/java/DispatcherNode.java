import akka.actor.AbstractActor;
import akka.actor.ActorRef;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.zip.ZipInputStream;

/**
 * Class for assigning and initializing nodes
 *
 * @author Simone Schirinzi
 */
class DispatcherNode extends AbstractActor {
    /**
     * Number of node to Start
     */
    private final int localSize;

    /**
     * Timer for counting time used
     */
    private final Timer timer;

    /**
     * Ref to DispatcherMaster
     */
    private final ActorRef master;

    /**
     * Initialized value at 0
     * At any time it indicates how many nodes
     * Have finished their initialization procedures.
     * We expect all actors to receive the message of creation before they start,
     * otherwise they receive messages from other actors already ready,
     * but they put them in unprepared structures
     */
    private int ready;

    /**
     * Handler for neighbors message
     *
     * If optimize is True:
     * let -/&gt; : not send to
     *
     * i -/&gt; k responsibility if s(i,k) = -INF
     * r(i,k) = -INF
     * i -/&gt; k availability if s(k,i) = -INF
     * a(i,k) != -INF
     * but is not influential for k
     * when he compute r(k,j) = s(k,j) - max{a(k,i) + -INF}
     *
     * col_infinity : The node which cannot reach me
     * row_infinity : The node i cannot reach
     *
     * if s_col[i] is infinity
     * r_col[i] must be set to infinity
     *
     * In addition, the carriers of links to infinite nodes are initialized
     *
     * When finished all operations
     * send a Ready() message to the dispatcher.
     * The node thus remains awaiting a Start() message
     *
     * @param lineMatrix file with matrix memorized by lines
     * @param colMatrix file with matrix memorized by column
     * @param size of the graph
     * @param nodes ref to all nodes
     */
    private DispatcherNode(String lineMatrix, String colMatrix, int from, int to, int size, ActorRef[] nodes, ActorRef master){
        this.localSize = to - from;
        this.master = master;

        this.timer = new Timer();
        timer.start();

        this.ready = 0;

        try(ZipInputStream lineReader = new ZipInputStream(new BufferedInputStream(new FileInputStream(lineMatrix)));
            ZipInputStream colReader = new ZipInputStream(new BufferedInputStream(new FileInputStream(colMatrix)))){

            byte[] rowBuffer = new byte[size*Double.BYTES];
            byte[] colBuffer = new byte[size*Double.BYTES];
            double[] s_row = new double[size];
            double[] s_col = new double[size];

            for(int i=0; i<from;i++){
                lineReader.getNextEntry();
                colReader.getNextEntry();
                lineReader.closeEntry();
                colReader.closeEntry();
            }

            int readLen;
            for(int i = from; i < to; i++){
                lineReader.getNextEntry();
                colReader.getNextEntry();

                readLen = 0;
                while (readLen < size * Double.BYTES)
                    readLen += lineReader.read(rowBuffer, readLen, (size * Double.BYTES) - readLen);
                assert (readLen == size * Double.BYTES);

                readLen = 0;
                while (readLen < size * Double.BYTES)
                    readLen += colReader.read(colBuffer, readLen, (size * Double.BYTES) - readLen);
                assert (readLen == size * Double.BYTES);

                s_row = Util.bytesToVector(rowBuffer, s_row);
                s_col = Util.bytesToVector(colBuffer, s_col);

                sendNodeSetting(i,size,s_row,s_col,nodes);

                lineReader.closeEntry();
                colReader.closeEntry();
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void sendNodeSetting(int i, int size, double[] s_row, double[] s_col, ActorRef[] nodes){
        HashMap<Integer, Double> a_row = new HashMap<>();
        HashMap<Integer, Double> r_col = new HashMap<>();
        HashMap<Integer, Double> s2_row = new HashMap<>();

        int col_infinity = 0;
        int row_infinity = 0;

        for (int j = 0; j < size; j++) {
            if (Util.isMinDouble(s_col[j])) col_infinity++;
            if (Util.isMinDouble(s_row[j])) row_infinity++;
        }

        /* size - row_infinite nodes must receive responsibility message */
        ActorRef[] r_not_infinite_neighbors = new ActorRef[size - row_infinity];

        /* size - col_infinite nodes must receive availability message */
        ActorRef[] a_not_infinite_neighbors = new ActorRef[size - col_infinity];

        int[] r_reference = new int[size - row_infinity];
        int[] a_reference = new int[size - col_infinity];

        /* Vector are set */
        int j = 0, k = 0;
        for (int q = 0; q < size; q++) {
            if (!Util.isMinDouble(s_row[q])) {
                r_not_infinite_neighbors[j] = nodes[q];
                r_reference[j] = q;

                s2_row.put(q, s_row[j]);
                a_row.put(q, 0.0);

                j++;
            }
            if (!Util.isMinDouble(s_col[q])) {
                a_not_infinite_neighbors[k] = nodes[q];
                a_reference[k] = q;

                r_col.put(q, 0.0);

                k++;
            }
        }

        nodes[i].tell(new NodeSetting(s2_row, i, size - col_infinity, size - row_infinity, r_not_infinite_neighbors, a_not_infinite_neighbors, r_reference, a_reference, a_row, r_col), self());
    }

    /**
     * Actor messages handler
     * @see Ready
     * @return receive handler
     */
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Ready.class, msg -> {
                    this.ready++;
                    if(ready == localSize) {
                        master.tell(new Ready(),self());
                        self().tell(akka.actor.PoisonPill.getInstance(), ActorRef.noSender());
                    }
                })
                .build();
    }

    @Override
    public void postStop(){
        timer.stop();
        System.out.println(localSize + " Dispatched in " + timer);
    }
}