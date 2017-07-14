/**
 * Constant class
 *
 * @author Simone Schirinzi
 */

class Constant {
    /**
     * Number of iterations where the result must be stable to finish the algorithm
     */
    public static final long enoughIterations = 25;

    /**
     * Each number of iterations the nodes must send the results to the aggregator
     */
    public static final int sendEach = 25;

    /**
     * Each message is set to lambda time its value from the previous iteration
     * plus 1 - lambda times its prescribed updated value.
     * 0 &lt;= lambda &lt;= 1
     * Default: 0.5
     */
    public static final double lambda = 0.8;

    /**
     * To avoid oscillations in case of similar similarities,
     * a random value is added to the input values.
     */
    public static final double sigma = 0;
}
