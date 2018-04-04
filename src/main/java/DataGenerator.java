import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.UUIDs;
import java.time.Instant;

public class DataGenerator {

    static String[] CONTACT_POINTS = {"127.0.0.1"};
    static int PORT = 9042;
    static int numRecords = 100;

    public static void main(String[] args) {

        DataGenerator client = new DataGenerator();

        try {

            client.connect(CONTACT_POINTS, PORT);
            client.createSchema();
            client.loadData();

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
     * Creates the schema (keyspace) and tables
     * for this example.
     */
    public void createSchema() {

        //NOTE: The schema should use NetworkTopologyStrategy and RF=3 for Production testing
        session.execute("CREATE KEYSPACE IF NOT EXISTS latency_check WITH replication " +
                "= {'class':'SimpleStrategy', 'replication_factor':1};");

        session.execute(
                "CREATE TABLE IF NOT EXISTS latency_check.kvp (" +
                        "id uuid PRIMARY KEY," +
                        "nanosec bigint" +
                        ");");
    }

    /**
     * * Get current time in nanoseconds
     * @return nanosecs
     */
    private long getCurrentTime(){

        Instant now = Instant.now();
        long secs = now.getEpochSecond();
        int nanosecs = now.getNano();
        long timeinnanos = secs*1000000000 + nanosecs;

        return timeinnanos;

    }

    /**
     * Inserts data into the tables.
     */
    public void loadData() {

        for (int i=0; i<numRecords; i++) {
            Insert insert = QueryBuilder.insertInto("latency_check", "kvp")
                    .value("id", UUIDs.random())
                    .value("nanosec", getCurrentTime());

            //System.out.println(insert.toString());
            ResultSet result = session.execute(insert.toString());
            //System.out.println(result);
        }
    }

    /**
     * Closes the session and the cluster.
     */
    public void close() {
        session.close();
        cluster.close();
    }

}
