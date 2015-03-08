package adfs.client;

import java.util.Scanner;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;


// COMMANDS

// keyspace creation
//     CREATE KEYSPACE adfs WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };


public class ADFSCassandraClient
{
	private Cluster cluster;
	Session session;
	
	public ADFSCassandraClient() {
		this.cluster = Cluster.builder().addContactPoint("192.168.1.202").build();
		session = cluster.connect("adfs");
	}
	
	public String execute(String cmd) {
		ResultSet res = session.execute(cmd);
		return res.all().toString();
	}
	
	
	public String executeSelect(String cmd) {
		int index = cmd.indexOf("FROM") + 4;
		Scanner s = new Scanner(cmd.substring(index));
		String table = s.next();
		s.close();
		
		System.out.println("GET TABLE: " + table);
		return this.execute(cmd);
	}
	
	
	public String executeInsert(String cmd) {
		int index = cmd.indexOf("INTO") + 4;
		Scanner s = new Scanner(cmd.substring(index));
		String table = s.next();
		s.close();
		
		System.out.println("INSERT INTO: " + table);
		return this.execute(cmd);
	}
	
	
	public void close() {
		System.out.println("Finalizing...");
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
