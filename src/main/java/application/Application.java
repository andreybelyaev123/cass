package application;

import java.io.IOException;

import com.datastax.driver.core.Session;

import common.Connector;
import employee.EmployeeData;
import employee.EmployeeSchema;

public class Application {
	public static void main(String [] args) {
		String [] positions = {"Software engineer"
							  ,"Software development engineer"
							  ,"Senior Software engineer"
							  ,"Senior Software development engineer"
							  ,"Principal Software engineer"
							  ,"Principal Software development engineer"
							  ,"Senior Principal Software engineer"
							  ,"Senior Principal Software development engineer"
							  ,"Director"
							  ,"Software engineer I"
							  ,"Software engineer II"
							  ,"Software engineer III"
							  ,"Software engineer IV"
							  ,"Manager"
							  ,"Product manager"
							  ,"Project manager"
							  ,"Lead Software engineer"
							  ,"Lead Software development engineer"};
		Connector connector = new Connector();
		EmployeeSchema schema = new EmployeeSchema("employee_schema", "employees");
		EmployeeData data = new EmployeeData("employee_schema", "employees");
		Session session = connector.getSession();
		schema.truncate(session);
		try {
			data.processFileMapped(session, "C:\\dev\\java\\CasandraTests\\cassandra-tests\\employee_100K");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (args.length == 0) {
			for (String position : positions) {
				long start = System.nanoTime();
				System.out.println(position + ":" + data.avgByPositionAsync(session, position) 
								+ ", time - " + ((System.nanoTime() - start) / 1000000000.0));
			}
		}
		else {
			for (String position : args) {
				long start = System.nanoTime();
				System.out.println(position + ":" + data.avgByPositionAsync(session, position) 
								+ ", time - " + ((System.nanoTime() - start) / 1000000000.0));
			}
		}
	}

}
