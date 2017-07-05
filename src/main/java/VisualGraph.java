import com.mxgraph.swing.mxGraphComponent;
import com.mxgraph.view.mxGraph;

import javax.swing.*;
import java.awt.*;

/**
 * Created by Simone on 26/06/2017.
 */
@SuppressWarnings({"unused", "DefaultFileTemplate"})
class VisualGraph extends JFrame {
    public VisualGraph(int[] exemplars, int[] cluster){
        mxGraph graph = new mxGraph();
        Object parent = graph.getDefaultParent();
        graph.getModel().beginUpdate();

        try{
            Object[] objects = new Object[cluster.length];

            int size = 30;
            int ray = 200;
            Point center = new Point(350,350);

            circle_node(size,ray,center,cluster,exemplars,objects,parent,graph);

            //cluster = archResolution(cluster);
            for (int i = 0; i < cluster.length; i++)
                if(cluster[i] > -1)
                    graph.insertEdge(parent, null, "", objects[i], objects[cluster[i]]);

        } finally {
            graph.getModel().endUpdate();
        }

        mxGraphComponent graphComponent = new mxGraphComponent(graph);
        getContentPane().add(graphComponent);
    }

    public void show(int width, int height){
        this.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        this.setSize(width, height);
        this.setVisible(true);
    }

    //Check if j is an exemplar
    private boolean isExemplar(int j, int[] exemplars){
        for(int i : exemplars)
            if(i==j)
                return true;
        return false;
    }

    private void chess_nodes(int size, int[] cluster, Object[] objects, mxGraph graph, Object parent){
        int side = new Double(Math.sqrt(cluster.length)).intValue();
        //Simple chess object
        for(int i = 0; i < cluster.length; i++){
            objects[i] = graph.insertVertex(parent,""+i, i, (i % side) * size * 2, i / side * size * 2, size, size);
        }
    }

    private void circle_node(int size, int ray, Point center, int[] cluster, int[] exemplars, Object[] objects, Object parent, mxGraph graph){
        double c_a = (2*Math.PI) / exemplars.length;
        double a = 0;

        for (int e : exemplars) {
            int x = (new Double(center.x + ray * Math.cos(a))).intValue();
            int y = (new Double(center.y + ray * Math.sin(a))).intValue();
            objects[e] = graph.insertVertex(parent, "" + e, e, x, y, size, size);

            int localRay;
            if (exemplars.length > 1) {
                localRay = (new Double(0.9 * ray * Math.sin(Math.PI / exemplars.length))).intValue();
            } else {
                localRay = ray;
            }

            int localCluster = 0;
            for (int j = 0; j < cluster.length; j++) {
                if (cluster[j] == e && j != e) {
                    localCluster++;
                }
            }

            if (localCluster > 0) {
                double c_localA = (2 * Math.PI) / localCluster;
                double localA = 0;
                for (int j = 0; j < cluster.length; j++) {
                    if (cluster[j] == e && !isExemplar(j, exemplars)) {
                        int localX = (new Double(x + localRay * Math.cos(localA))).intValue();
                        int localY = (new Double(y + localRay * Math.sin(localA))).intValue();
                        objects[j] = graph.insertVertex(parent, "" + j, j, localX, localY, size, size);
                        localA += c_localA;
                    }
                }
            }
            a += c_a;
        }
    }

    private int[] archResolution(int[] cluster){
        int[] newCluster = new int[cluster.length];
        for(int i = 0; i < cluster.length; i++){
            if(!loopDetect(i,cluster)) {
                int ref = cluster[i];
                while (ref != cluster[ref])
                    ref = cluster[ref];
                newCluster[i] = ref;
                System.out.println(i + " solved!");
            } else {
                newCluster[i] = cluster[i];
            }
        }
        return newCluster;
    }

    private boolean loopDetect(int i, int[] cluster){
        int hare = 0;
        int tortoise = i;
        while(hare < cluster.length){
            tortoise = cluster[tortoise];
            if(hare < cluster.length){
                hare++;
            }
            if(tortoise == hare){
                return true;
            }
        }
        return false;
    }
}
