package common;

import static org.junit.Assert.*;

import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class ConnectorTest {

	@Test
	public void testGetSession() {
		Connector connector = new Connector();
		ResultSet rs = connector.getSession().execute("select release_version from system.local");    // (3)
	    Row row = rs.one();
	    System.out.println(row.getString("release_version"));                          // (4)
	}

}
