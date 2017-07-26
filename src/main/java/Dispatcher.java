import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.*;
import java.util.HashMap;
import java.util.zip.ZipFile;

/**
 * Class for assigning and initializing nodes
 *
 * @author Simone Schirinzi
 */
class Dispatcher extends AbstractActor {
    /**
     * number of nodes
     */
    private final int size;

    /**
     * vector of link to nodes
     */
    private final ActorRef[] nodes;

    /**
     *
     */
    private final Timer timer;
    /**
     * Initialized value at 0
     * At any time it indicates how many nodes
     * Have finished their initialization procedures.
     * We expect all actors to receive the message of creation before they start,
     * otherwise they receive messages from other actors already ready,
     * but they put them in unprepared structures
     */
    private int ready;

    static Props props(String lineMatrix, String colMatrix, int size, ActorRef[] nodes) {
        return Props.create(Dispatcher.class, () -> new Dispatcher(lineMatrix,colMatrix,size,nodes));
    }

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
    private Dispatcher(String lineMatrix, String colMatrix, int size, ActorRef[] nodes){
        this.size = size;
        this.nodes = nodes;

        this.timer = new Timer();
        timer.start();

        this.ready = 0;

        try(
                ZipFile rowFile = new ZipFile(lineMatrix);
                ZipFile colFile = new ZipFile(colMatrix);
                BufferedInputStream lineReader = new BufferedInputStream(rowFile.getInputStream(rowFile.getEntry("matrix")));
                BufferedInputStream colReader = new BufferedInputStream(colFile.getInputStream(colFile.getEntry("matrix")))){

            int readLen;
            int rowSize, colSize;
            rowSize = colSize = size*Double.BYTES;

            double[] s_row = new double[size];
            double[] s_col = new double[size];
            byte[] rowBuffer = new byte[rowSize];
            byte[] colBuffer = new byte[colSize];

            for(int i = 0; i < size; i++) {
                readLen = 0;
                while (readLen < rowSize)
                    readLen += lineReader.read(rowBuffer, readLen, rowSize - readLen);
                assert (readLen == rowSize);

                readLen = 0;
                while (readLen < colSize)
                    readLen += colReader.read(colBuffer, readLen, colSize - readLen);
                assert (readLen == colSize);


                s_row = Util.bytesToVector(rowBuffer,s_row);
                s_col = Util.bytesToVector(colBuffer,s_col);

                HashMap<Integer,Double> a_row = new HashMap<>();
                HashMap<Integer,Double> r_col = new HashMap<>();
                HashMap<Integer,Double> s2_row = new HashMap<>();

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

                        s2_row.put(q,s_row[q]);
                        a_row.put(q,0.0);

                        j++;
                    }
                    if (!Util.isMinDouble(s_col[q])) {
                        a_not_infinite_neighbors[k] = nodes[q];
                        a_reference[k] = q;

                        r_col.put(q,0.0);

                        k++;
                    }
                }
                nodes[i].tell(new NodeSetting(s2_row,i,size-col_infinity,size-row_infinity,r_not_infinite_neighbors,a_not_infinite_neighbors,r_reference,a_reference,a_row,r_col),self());
            }

        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
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
            if(ready == size){
                for(ActorRef node : nodes) node.tell(new Start(),ActorRef.noSender());
                timer.stop();
                System.out.println(size + " Nodes ready in " + timer);
            }
        })
        .build();
    }
}