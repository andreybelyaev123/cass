package common;

import com.datastax.driver.core.*;
import com.datastax.driver.mapping.*;

public class Connector {
	final private String IPstring; 
	private Session session = null;
	private MappingManager manager = null;
	
	public Connector(String ip) {
		IPstring = ip;
	}
	public Connector() {
		this("localhost");
	}
		
	public Session getSession() {
		if (session != null)
			return session;
		Cluster cluster = null;
		try {
		    cluster = Cluster.builder()                                                    // (1)
		            .addContactPoint(IPstring)
		            .addContactPoint("samlib")
		            .build();
		    session = cluster.connect();                                           // (2)
		    manager = new MappingManager(session);
		    return session;
/*
		    ResultSet rs = session.execute("select release_version from system.local");    // (3)
		    Row row = rs.one();
		    System.out.println(row.getString("release_version"));                          // (4)
*/		    
		}
		catch (Exception ex) {
			if (cluster != null) cluster.close();
			ex.printStackTrace();
			return null;
		}
		/*
		finally {
		    if (cluster != null) cluster.close();                                          // (5)
		}*/
	}
	public MappingManager getManager() {
		if (manager == null)
			getSession();
		return manager;
	}
}
