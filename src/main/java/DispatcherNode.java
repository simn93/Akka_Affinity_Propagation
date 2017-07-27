import akka.actor.AbstractActor;
import akka.actor.ActorRef;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Class for assigning and initializing nodes
 *
 * @author Simone Schirinzi
 */
class DispatcherNode extends AbstractActor {
    /**
     *
     */
    private final int from;
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
     *
     */
    private final ActorRef[] nodes;

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
        this.from = from;
        this.localSize = to - from;
        this.master = master;
        this.nodes = nodes;

        this.timer = new Timer();
        timer.start();

        this.ready = 0;

        byte[] rowBuffer, colBuffer;
        HashMap<Integer,Double> s_row;
        HashMap<Integer,Double> s_col;

        int readLen, rowSize, colSize;
        ZipEntry rowEntry, colEntry;
        try(ZipFile lineFile = new ZipFile(lineMatrix); ZipFile colFile = new ZipFile(colMatrix)) {
            for (int i = from; i < to; i++) {
                rowEntry = lineFile.getEntry(i+".line");
                colEntry = colFile.getEntry(i+".line");
                rowSize = (int)(long) rowEntry.getSize();
                colSize = (int)(long) colEntry.getSize();
                rowBuffer = new byte[rowSize];
                colBuffer = new byte[colSize];

                try (BufferedInputStream lineReader = new BufferedInputStream(lineFile.getInputStream(rowEntry));
                     BufferedInputStream colReader = new BufferedInputStream(colFile.getInputStream(colEntry))) {

                    readLen = 0;
                    while (readLen < rowSize)
                        readLen += lineReader.read(rowBuffer, readLen, rowSize - readLen);
                    assert (readLen == rowSize);

                    readLen = 0;
                    while (readLen < colSize)
                        readLen += colReader.read(colBuffer, readLen, colSize - readLen);
                    assert (readLen == colSize);

                    s_row = new HashMap<>();
                    s_col = new HashMap<>();

                    Util.hashBytesToHashMap(rowBuffer, s_row);
                    Util.hashBytesToHashMap(colBuffer, s_col);

                    sendNodeSetting(i, s_row, s_col, nodes);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void sendNodeSetting(int i, HashMap<Integer,Double> s_row, HashMap<Integer,Double> s_col, ActorRef[] nodes){
        HashMap<Integer, Double> a_row = new HashMap<>();
        HashMap<Integer, Double> r_col = new HashMap<>();

        int row_unInfinity = s_row.size();
        int col_unInfinity = s_col.size();

        /* size - row_infinite nodes must receive responsibility message */
        ActorRef[] r_not_infinite_neighbors = new ActorRef[row_unInfinity];

        /* size - col_infinite nodes must receive availability message */
        ActorRef[] a_not_infinite_neighbors = new ActorRef[col_unInfinity];

        int[] r_reference = new int[row_unInfinity];
        int[] a_reference = new int[col_unInfinity];

        /* Vector are set */
        int j = 0, k = 0;

        for (int q : s_row.keySet()) {
            r_not_infinite_neighbors[j] = nodes[q];
            r_reference[j] = q;

            a_row.put(q, 0.0);

            j++;
        }
        for(int q : s_col.keySet()){
            a_not_infinite_neighbors[k] = nodes[q];
            a_reference[k] = q;

            r_col.put(q, 0.0);

            k++;
        }

        nodes[i].tell(new NodeSetting(s_row, i, col_unInfinity, row_unInfinity, r_not_infinite_neighbors, a_not_infinite_neighbors, r_reference, a_reference, a_row, r_col), self());
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
                    }
                })
                .match(Start.class, msg ->{
                    for(int i=from; i< from + localSize; i++)nodes[i].tell(new Start(),self());
                    self().tell(akka.actor.PoisonPill.getInstance(), ActorRef.noSender());
                })
                .build();
    }

    @Override
    public void postStop(){
        timer.stop();
        System.out.println(localSize + " Dispatched in " + timer);
    }
}