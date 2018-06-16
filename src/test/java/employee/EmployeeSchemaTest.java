package employee;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import com.datastax.driver.core.Session;

import common.Connector;

public class EmployeeSchemaTest {
	
	@Test
	public void testAverage() {
		Connector connector = new Connector();
		Session session = connector.getSession();
		EmployeeData data = new EmployeeData("employee_schema", "employees");
		System.out.println(data.avgByPosition(session, "Software engineer"));
	}
	@Test
	public void testAverageMapper() {
		Connector connector = new Connector();
		Session session = connector.getSession();
		EmployeeData data = new EmployeeData("employee_schema", "employees");
		System.out.println(data.avgByPositionMapper(session, "Software engineer"));
	}

	@Test
	public void testAverageOne() {
		Connector connector = new Connector();
		Session session = connector.getSession();
		EmployeeData data = new EmployeeData("employee_schema", "employees");
		System.out.println(data.avgByPositionOne(session, "Software engineer"));
	}
	@Test
	public void testAverageAsync() {
		Connector connector = new Connector();
		Session session = connector.getSession();
		EmployeeData data = new EmployeeData("employee_schema", "employees");
		System.out.println(data.avgByPositionAsync(session, "Software engineer"));
	}

	@Test
	public void test() {
		Connector connector = new Connector();
		EmployeeSchema schema = new EmployeeSchema("employee_schema", "employees");
		Session session = connector.getSession();
		assertEquals(true, schema.create(session));
		EmployeeData data = new EmployeeData("employee_schema", "employees");
		try {
			assertEquals(true, schema.truncate(session));
			assertEquals(100000, data.insertBatch(session, "C:\\dev\\java\\CasandraTests\\cassandra-tests\\employee_100K"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Test
	public void testByOne() {
		Connector connector = new Connector();
		EmployeeSchema schema = new EmployeeSchema("employee_schema", "employees");
		Session session = connector.getSession();
		assertEquals(true, schema.create(session));
		EmployeeData data = new EmployeeData("employee_schema", "employees");
		try {
			assertEquals(true, schema.truncate(session));
			assertEquals(10000, data.insertByOne(session, "C:\\dev\\java\\CasandraTests\\cassandra-tests\\employee"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Test
	public void testNotPrepeared() {
		Connector connector = new Connector();
		EmployeeSchema schema = new EmployeeSchema("employee_schema", "employees");
		Session session = connector.getSession();
		assertEquals(true, schema.create(session));
		EmployeeData data = new EmployeeData("employee_schema", "employees");
		try {
			assertEquals(true, schema.truncate(session));
			assertEquals(10000, data.insertFile(session, "C:\\dev\\java\\CasandraTests\\cassandra-tests\\employee"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Test
	public void testMapper() {
		Connector connector = new Connector();
		EmployeeSchema schema = new EmployeeSchema("employee_schema", "employees");
		Session session = connector.getSession();
		assertEquals(true, schema.create(session));
		EmployeeData data = new EmployeeData("employee_schema", "employees");
		try {
			assertEquals(true, schema.truncate(session));
			assertEquals(100000, data.processFileMapped(session, "C:\\dev\\java\\CasandraTests\\cassandra-tests\\employee_100K"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Test
	public void testUpdate() {
		Connector connector = new Connector();
		Session session = connector.getSession();
		Session updSession = connector.getSession();
		EmployeeData data = new EmployeeData("employee_schema", "employees");
		assertEquals(10, data.updateSalary(session, updSession, "Software engineer", 10000));
	}
}
