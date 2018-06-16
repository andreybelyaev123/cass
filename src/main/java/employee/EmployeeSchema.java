package employee;

import javax.inject.Inject;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Truncate;

import common.Connector;

public class EmployeeSchema {
	
	private final String schema;
	private final String tableName;
	
	public EmployeeSchema(String schema, String tableName) {
		this.schema = schema;
		this.tableName = tableName;
	}
	
	public String getSchema() {
		return this.schema;
	}
	public String getTableName() {
		return tableName;
	}

	public boolean create(Session session) {
		session.execute("CREATE KEYSPACE IF NOT EXISTS employee_schema WITH replication " +
                "= {'class':'SimpleStrategy', 'replication_factor':1};");
		session.execute(
                "CREATE TABLE IF NOT EXISTS " +  schema + "." + tableName + "(" +
                        "id uuid," +
                        "position text," +
                        "first_name text, " +
                        "last_name text," +
                        "salary int," +
                        "seniority_level text," +
                        "employment_type text," +
                        "PRIMARY KEY ((position),last_name,first_name,id)" +
                        ");");
		return true;
	}
	public boolean drop(Session session) {
		session.execute("DROP TABLE " +  schema + "." + tableName);
		//session.execute(QueryBuilder.drop(schema,tableName));
		return true;
	}
	public boolean truncate(Session session) {
		session.execute(QueryBuilder.truncate(schema,tableName));
		return true;
	}
}
