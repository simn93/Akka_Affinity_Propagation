import com.mxgraph.swing.mxGraphComponent;
import com.mxgraph.view.mxGraph;
import javax.swing.*;
import java.awt.*;

/**
 * Class for graph visualization
 * @see JFrame
 *
 * @author Simone Schirinzi
 */
@SuppressWarnings({"unused"})
class VisualGraph extends JFrame {
    /**
     * @param exemplars : int vector of cluster's exemplars ID
     * @param cluster : i belongs to cluster[i]
     */
    public VisualGraph(int[] exemplars, int[] cluster){
        /* Graphic graph */
        mxGraph graph = new mxGraph();

        /* Default graph node's parent
        * Specifies the default parent to be used to insert new cells.
        * */
        Object parent = graph.getDefaultParent();

        /* Inserting nodes into the graph */
        graph.getModel().beginUpdate();
        try{/*
             * Create an imaginary circle
             * and position the equidistant exemplars on that circle.
             * Cluster nodes are placed on a smaller circle,
             * created around each specimen.
             */
            Point center = new Point(350,350);
            Object[] objects = circle_node(30,200,center,cluster,exemplars,parent,graph);

            /* Connect nodes between them. */
            for (int i = 0; i < cluster.length; i++) if(cluster[i] > -1)
                    graph.insertEdge(parent, null, "", objects[i], objects[cluster[i]]);

        } finally { graph.getModel().endUpdate(); }

        mxGraphComponent graphComponent = new mxGraphComponent(graph);
        getContentPane().add(graphComponent);
    }

    /**
     * Make this frame visible
     * @param width of the window
     * @param height of the window
     */
    public void show(int width, int height){
        this.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        this.setSize(width, height);
        this.setVisible(true);
    }

    /**
     *  Check if j is an exemplar */
    private boolean isExemplar(int j, int[] exemplars){
        for(int i : exemplars) if(i==j) return true;
        return false;
    }

    /**
     * Set the node coordinates to get a circular structure of exemplars and clusters
     * @param size of single node
     * @param ray of main circle
     * @param center of main circle
     * @param cluster vector of cluster: i belong to cluster[i]
     * @param exemplars vector of exemplar's ID
     * @param parent node's standard graph parent
     * @param graph graph structure
     *
     * @return object's vector of node link
     */
    @SuppressWarnings("SameParameterValue")
    private Object[] circle_node(int size, int ray, Point center, int[] cluster, int[] exemplars, Object parent, mxGraph graph){
        Object[] objects = new Object[cluster.length];

        /* How many degrees of angle must be two exemplars away */
        double c_a = (2*Math.PI) / exemplars.length;
        /* First exemplar is in 0 degree */
        double a = 0;

        for (int e : exemplars) {
            int x = (new Double(center.x + ray * Math.cos(a))).intValue();
            int y = (new Double(center.y + ray * Math.sin(a))).intValue();
            objects[e] = graph.insertVertex(parent, "" + e, e, x, y, size, size);

            int localRay;
            if (exemplars.length > 1) {
                localRay = (new Double(0.9 * ray * Math.sin(Math.PI / exemplars.length))).intValue();
            } else
                localRay = ray;

            int localCluster = 0;
            for (int j = 0; j < cluster.length; j++)
                if (cluster[j] == e && j != e) localCluster++;

            if (localCluster > 0) {
                double c_localA = (2 * Math.PI) / localCluster , localA = 0;
                for (int j = 0; j < cluster.length; j++) if (cluster[j] == e && !isExemplar(j, exemplars)) { //avoid duplicate
                        int localX = (new Double(x + localRay * Math.cos(localA))).intValue();
                        int localY = (new Double(y + localRay * Math.sin(localA))).intValue();
                        objects[j] = graph.insertVertex(parent, "" + j, j, localX, localY, size, size);
                        localA += c_localA;
                }
            }
            a += c_a;
        }
        return objects;
    }
}
