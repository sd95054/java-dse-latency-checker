import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;


public class LatencyChecker {

    static String[] CONTACT_POINTS = {"127.0.0.1"};
    static int PORT = 9042;

    public static void main(String[] args) {

        LatencyChecker client = new LatencyChecker();

        try {

            client.connect(CONTACT_POINTS, PORT);
            client.querySchema();

        } finally {
            client.close();
        }
    }

    private Cluster cluster;

    private Session session;

    /**
     * Initiates a connection to the cluster
     * specified by the given contact point.
     *
     * @param contactPoints the contact points to use.
     * @param port          the port to use.
     */
    public void connect(String[] contactPoints, int port) {

        cluster = Cluster.builder()
                .addContactPoints(contactPoints).withPort(port)
                .build();

        System.out.printf("Connected to cluster: %s%n", cluster.getMetadata().getClusterName());

        session = cluster.connect();
    }


    /**
     * Queries and displays data.
     */
    public void querySchema() {

        int recordCount=0;

        ResultSet numRecords = session.execute(
                "SELECT count(id) FROM latency_check.kvp;");
        for (Row row : numRecords) {
           recordCount = (int)row.getLong("system.count(id)");
        }

        long[] delta_arr = new long[recordCount]; //Create an array of deltas for statistics
        int i = 0;

        ResultSet results = session.execute(
                "SELECT writetime(nanosec), nanosec, id FROM latency_check.kvp;");

        for (Row row : results) {
            /**
            System.out.printf("%-30s\t%-30s\t%-20s%n",
                    row.getLong("writetime(nanosec)"),
                    row.getLong("nanosec"),
                    row.getUUID("id"));
             **/
            long delta_ns = row.getLong("writetime(nanosec)") - row.getLong("nanosec")/1000;

            System.out.println("Delta (nanoseconds) = " + delta_ns);

            delta_arr[i] = delta_ns;
            i++;
        }

        Statistics stats = new Statistics(delta_arr);

        System.out.println("Collection Size: " + stats.getSize());
        System.out.println("Mean: " + stats.getMean());
        System.out.println("Median: " + stats.median());
        System.out.println("Std Deviation: " + stats.getStdDev());

    }

    /**
     * Closes the session and the cluster.
     */
    public void close() {
        session.close();
        cluster.close();
    }

}
