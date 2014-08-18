package edu.uiuc.morphous;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.QueryValidationException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;



/**
 * Created by Daniel on 7/27/14.
 */
public class Main {
    public static final int numNodes = 10;
    public static final String[] endpoints;
    public static final String cassandraPath = "/opt/cassandra";


    static {
        endpoints = new String[numNodes];
        for (int i = 0; i < numNodes; i++) {
            endpoints[i] = "node-" + i;
        }
    }

    public static void main(String[] args) {
        final int concurrency = 50;
        final int maxConnections = 1;
//        final String contactPoint = "127.0.0.1";
        boolean compression = false;
        final String ksName = "testkeyspace";
        final String cfName = "testcf";
        final List<String> columns = Arrays.asList("col0", "col1", "col2", "col3");
        final List<String> columnTypes = Arrays.asList("bigint PRIMARY KEY", "bigint", "varchar", "int");
        final int replicationFactor = 3;

        PoolingOptions pools = new PoolingOptions();
        pools.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, concurrency);
        pools.setCoreConnectionsPerHost(HostDistance.LOCAL, maxConnections);
        pools.setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnections);
        pools.setCoreConnectionsPerHost(HostDistance.REMOTE, maxConnections);
        pools.setMaxConnectionsPerHost(HostDistance.REMOTE, maxConnections);

        Cluster cluster = new Cluster.Builder()
                .addContactPoints(endpoints)
                .withPoolingOptions(pools)
                .withSocketOptions(new SocketOptions().setTcpNoDelay(true))
                .build();

        if (compression)
            cluster.getConfiguration().getProtocolOptions().setCompression(ProtocolOptions.Compression.SNAPPY);

        final Session session = cluster.connect();


        Metadata metadata = cluster.getMetadata();
        System.out.println(String.format("Connected to cluster '%s' on %s.", metadata.getClusterName(), metadata.getAllHosts()));

        System.out.println("Preparing test...");

        try {
            executeWithSession(session, String.format("DROP KEYSPACE %s;", ksName));
        } catch (QueryValidationException e) {}

        executeWithSession(session, String.format("CREATE KEYSPACE %s WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : %d};", ksName, replicationFactor));
        executeWithSession(session, String.format("USE %s;", ksName));

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("CREATE TABLE %s (", cfName));
        Iterator<String> columnIter = columns.iterator();
        Iterator<String> typeIter = columnTypes.iterator();
        while (columnIter.hasNext()) {
            sb.append(columnIter.next()).append(" ").append(typeIter.next());
            if (columnIter.hasNext()) {
                sb.append(", ");
            } else {
                sb.append(");");
            }
        }
        executeWithSession(session, sb.toString());

        final ExecutorService executorService = Executors.newFixedThreadPool(2);

        Random rand = new Random();
        for (int i = 0; i < 5000; i++) {
            sb = new StringBuilder();
            sb.append(String.format("INSERT INTO %s.%s (", ksName, cfName));
            columnIter = columns.iterator();
            while (columnIter.hasNext()) {
                sb.append(columnIter.next());
                if (columnIter.hasNext()) {
                    sb.append(", ");
                } else {
                    sb.append(") VALUES (");
                }
            }
            sb.append(String.format("%d, ", i))
                    .append(String.format("%d, ", i + 1000000))
                    .append(String.format("'testcf-%05d', ", i))
                    .append(String.format("%d);", rand.nextInt()));

            final StringBuilder finalSb = sb;
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    executeWithSession(session, finalSb.toString());
                }
            });

            // Execute Morphous Script in the middle of insertions
            if (i == 2500) {
                executorService.execute(new Runnable() {
                    @Override
                    public void run() {
                        executeMorphousScript(ksName, cfName);
                    }
                });
            }
        }


        try {
            executorService.awaitTermination(100, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    public static void executeWithSession(Session session, String statement) {
        System.out.println(statement);
        session.execute(statement);
    }

    public static void executeMorphousScript(String ksName, String cfName) {
        String cmd = String.format("%s/bin/nodetool -p 7100 -m \"{\"column\":\"col1\"}\" morphous %s %s", cassandraPath, ksName, cfName);

        StringBuilder sb = new StringBuilder();
        Process p;
        try {
            p = Runtime.getRuntime().exec(cmd);
            p.waitFor();

            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(p.getInputStream()));

            String line = "";
            while ((line = reader.readLine())!= null) {
                sb.append(line + "\n");
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
        System.out.println("Morphous script ended with " + sb.toString());
    }
}
