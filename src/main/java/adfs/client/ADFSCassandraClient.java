package adfs.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Scanner;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import adfs.core.*;

// --- COMMANDS CQLSH ---
// CREATE KEYSPACE adfs WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
// USE adfs;
// CREATE TABLE t1 (name text PRIMARY KEY, content text);
// INSERT INTO t1 (name, content) VALUES ('f1', 'Nuno frase bola carro amanha');
// SELECT * FROM t1;
// DROP TABLE t1;

public class ADFSCassandraClient {
	
	private static final String PROPERTIES_FILENAME = "ADFSClient.properties";
	private static final String PROP_CACHENAME = "infinispan_cache_name";
	private static final String PROP_CASSANDRA_ADDR = "cassandra_address";
	private static final String PROP_CASSANDRA_KEYS = "cassandra_keyspace";
	
	private final Logger LOG = LoggerFactory.getLogger(ADFSCassandraClient.class);
	
	private Cluster cluster;
	private Session session;
	private RemoteCache<String, ADFSFile> infinispanCache;
	
	
	public ADFSCassandraClient() {
		
		Properties p = readPropertiesFile(PROPERTIES_FILENAME);    	
    	if(p == null) {
			LOG.error("property file not found in the classpath");
			System.exit(1);
    	}
    	
    	String cacheName = p.getProperty(PROP_CACHENAME);
    	String cassandraAddr = p.getProperty(PROP_CASSANDRA_ADDR);
    	String cassandraKeyspace = p.getProperty(PROP_CASSANDRA_KEYS);
		
		// Cassandra
		this.cluster = Cluster.builder().addContactPoint(cassandraAddr).build();
		session = cluster.connect(cassandraKeyspace); // keyspace already created
		
		// Infinispan
    	RemoteCacheManager cacheContainer = new RemoteCacheManager();
    	this.infinispanCache = cacheContainer.getCache(cacheName);
	}
	
	
	private Properties readPropertiesFile(String filename) {
		Properties configProp = new Properties();
		InputStream inputStream = getClass().getClassLoader().getResourceAsStream(filename);
		
		if(inputStream == null) return null;
		
		try {
			configProp.load(inputStream);
			return configProp;
		} catch (IOException e) {
			return null;
		}
    }
	
	
	public String execute(String cmd) {
		ResultSet res = session.execute(cmd);
		return res.all().toString();
	}
	
	
	public String executeSelect(String cmd) {
		LOG.info(">> QUERY INTERCEPTED!");
		int index = cmd.indexOf("FROM") + 4;
		Scanner s = new Scanner(cmd.substring(index));
		String table = s.next();
		s.close();
		
		// Infinispan
		String tableMetaKey = ADFSFile.ATTR_PREFIX + table;
		ADFSFileMeta tableMeta = (ADFSFileMeta)infinispanCache.get(tableMetaKey);
		
		if(tableMeta == null) {
			infinispanCache.put(tableMetaKey, null);
			tableMeta = (ADFSFileMeta)infinispanCache.get(tableMetaKey);
		}
		
		// Conditions
		if(tableMeta == null) {
			LOG.error("Table does not exist!");
			return "";
		}
		
		if(tableMeta.isActive() && !tableMeta.isAvailable()) {
			LOG.error("Active table not available right now!");
			return "";
		}
		
		// Start computations if necessary
		ADFSFileContent tableContent = (ADFSFileContent)infinispanCache.get(table);
		
		if(tableContent == null) {
			LOG.error("Active table is processing...");
			return "";
		}
		
		// TODO Don't know if it is necessary
		if(tableMeta.isActive() &&
    			((ADFSActiveFileMeta)tableMeta).isStale()) {
			LOG.error("Active table is waiting for dependencies.");
			return "";
		}
		
		// Cassandra
		return this.execute(cmd);
	}
	
	
	public String executeInsert(String cmd) {
		LOG.info(">> QUERY INTERCEPTED!");
		int index = cmd.indexOf("INTO") + 4;
		Scanner s = new Scanner(cmd.substring(index));
		String table = s.next();
		s.close();
		
		// Infinispan
		String tableMetaKey = ADFSFile.ATTR_PREFIX + table;
		ADFSFileMeta tableMeta = (ADFSFileMeta)infinispanCache.get(tableMetaKey);
		
		if(tableMeta == null) {
			infinispanCache.put(tableMetaKey, null);
			tableMeta = (ADFSFileMeta)infinispanCache.get(tableMetaKey);
		}
		
		// Conditions
		if(tableMeta == null) {
			LOG.error("Table does not exist!");
			return "";
		}
		
		if(tableMeta.isActive()) {
			LOG.error("Not allowed to modify an active table!");
			return "";
		}
		
		if(!tableMeta.isAvailable()) {
			LOG.error("Table not available for writing right now!");
			return "";
		}
		
		// Content put, for now just null
		// It activates the active mechanisms in infinispan
		infinispanCache.put(table, null);

		// Cassandra
		return this.execute(cmd);
	}
	
	
	public void close() {
		LOG.info("Finalizing...");
		cluster.close();
	}
	
	
    public static void main( String[] args )
    {
        ADFSCassandraClient clt = new ADFSCassandraClient();
        Scanner sc = new Scanner(System.in);
        
        String cmd = "";
        System.out.println("---- ADFS Cassandra CQL Client ----");
        do {
        	System.out.println("\nSelect and Insert are intercetped, \'q\' to quit");
        	System.out.print("> ");
        	cmd = sc.nextLine();
        	
        	if(cmd.startsWith("SELECT ")) 
        		System.out.println(clt.executeSelect(cmd));
        	else if(cmd.startsWith("INSERT "))
        		System.out.println(clt.executeInsert(cmd));
        	else if(cmd.compareToIgnoreCase("q") != 0)
        		System.out.println(clt.execute(cmd));
        }
        while(cmd.compareTo("q") != 0);
        
        sc.close();
        clt.close();
    }
}
