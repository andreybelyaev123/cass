package employee;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.BindMarker;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import common.DataHelper;

public class EmployeeData {
	private static final int MAX_BATCH_SIZE = 400;
	private final String keySpace;
	private final String tableName;
	private int lines = 0;
	private int count = 0;
	private long total = 0;
	
	public EmployeeData(String keySpace, String tableName) {
		this.keySpace = keySpace;
		this.tableName = tableName;
	}
	
	public int insertBatch(final Session session, final String fileName) throws FileNotFoundException, IOException {
		final BatchStatement batch = new BatchStatement();
		lines = 0;
		return processFilePrepared(session
						 , fileName
						 , (Statement stmt) -> {
			if (stmt != null) {
				batch.add(stmt);
			}
			if (++lines == MAX_BATCH_SIZE || stmt == null && lines != 0) {
				session.execute(batch);
				batch.clear();
				lines = 0;
			}
		}
						 , "insertBatch");
	}
	public int insertByOne(final Session session, final String fileName) throws FileNotFoundException, IOException {
		return processFilePrepared(session
						 , fileName
						 , (Statement stmt) -> {
			if (stmt != null) {
				session.execute(stmt);
			}
		}
						 , "insertByOne");
	}
	
	private int processFilePrepared(final Session session
							, final String fileName
							, final Consumer<Statement> acceptor
							, String Oper) 
			throws FileNotFoundException, IOException 
	{
		List<Map<String,String>> data = DataHelper.readFromFile(fileName);
		long start = System.nanoTime();
		String [] propertiesNames = {"id"
				  ,"position"
				  ,"last_name"
				  ,"first_name"
				  ,"salary"
				  ,"seniority_level"
				  ,"employment_type"};
		BindMarker [] markers = new BindMarker [propertiesNames.length];
		for (int i = 0; i != markers.length; ++i)
			markers[i] = QueryBuilder.bindMarker();
		Insert insert = QueryBuilder.insertInto(keySpace,tableName)
				.values(propertiesNames, markers);
		final PreparedStatement stmt = session.prepare(insert);
		int lines = 0;
		for (Map<String,String> line : data) {
			String position = line.get("position");
			String first_name = line.get("first_name");
			String last_name = line.get("last_name");
			String salary = line.get("salary");
			String seniority_level = line.get("seniority_level");
			String employment_type = line.get("employment_type");
			acceptor.accept(stmt.bind(UUIDs.random()
					 ,position
					 ,last_name
					 ,first_name
					 ,Integer.valueOf(salary)
					 ,seniority_level
					 ,employment_type));
			++lines;
		}
		acceptor.accept(null);
		System.out.println(Oper + ": time - " + ((System.nanoTime() - start) / 1000000000.0) + " sec" +
				", lines - " + lines);
		return lines;
	}
	
	public int processFileMapped(final Session session
			, final String fileName) 
			throws FileNotFoundException, IOException 
	{
		List<Map<String,String>> data = DataHelper.readFromFile(fileName);
		long start = System.nanoTime();
		MappingManager manager = new MappingManager(session);
		Mapper<Employee> mapper = manager.mapper(Employee.class);
		final BatchStatement batch = new BatchStatement();
		int totalLines = 0;
		List<ResultSetFuture> futures = new ArrayList<>();
		for (Map<String,String> line : data) {
			Employee employee = new Employee();
			employee.setId(UUIDs.random());
			employee.setPosition(line.get("position"));
			employee.setLastName(line.get("last_name"));
			employee.setFirstName(line.get("first_name"));
			String salary = line.get("salary");
			employee.setSalary(salary == null ? null : Integer.valueOf(salary));
			employee.setSeniorityLevel(line.get("seniority_level"));
			employee.setEmploymentType(line.get("employment_type"));
			batch.add(mapper.saveQuery(employee));
			if (++lines == MAX_BATCH_SIZE) {
				futures.add(session.executeAsync(batch));
				batch.clear();
				lines = 0;
			}
			++totalLines;
		}
		if (lines != 0)
			futures.add(session.executeAsync(batch));
		for (ResultSetFuture future : futures)
			try {
				future.get();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		System.out.println("Mapper" + ": time - " + ((System.nanoTime() - start) / 1000000000.0) + " sec" +
		", lines - " + totalLines);
		return totalLines;
	}

	
	 public int insertFile(final Session session, final String fileName) throws FileNotFoundException, IOException {
		List<Map<String,String>> data = DataHelper.readFromFile(fileName);
		long start = System.nanoTime();
		int lines = 0;
		String insertStr = "INSERT INTO " + keySpace + "." + tableName + " (id," +
                "position," +
                "first_name, " +
                "last_name," +
                "salary," +
                "seniority_level," +
                "employment_type) VALUES (?,?,?,?,?,?,?)";
		for (Map<String,String> line : data) {
			String position = line.get("position");
			String first_name = line.get("first_name");
			String last_name = line.get("last_name");
			String salary = line.get("salary");
			String seniority_level = line.get("seniority_level");
			String employment_type = line.get("employment_type");
			session.execute(insertStr, UUIDs.random()
									 ,position
									 ,first_name
									 ,last_name
									 ,Integer.valueOf(salary)
									 ,seniority_level
									 ,employment_type);
			++lines;
		}
		System.out.println("insertFile: time - " + ((System.nanoTime() - start) / 1000000000.0) + " sec" +
							", lines - " + lines);
		return lines;
	}
	
	 
	public int updateSalary(final Session session, final Session updSession, final String position, int toAdd) {
		Statement select = QueryBuilder.select()
							.column("position")
							.column("last_name")
							.column("first_name")
							.column("id")
							.column("salary")
							.column("employment_type")
							.from("employee_schema","employees")
							.where(QueryBuilder.eq("position",position))
							.and(QueryBuilder.eq("last_name","SMITH"))
							.limit(10);
		System.out.println(select.toString());
		ResultSetFuture future = session.executeAsync(select);
		//ResultSet r = session.execute(select);
		ExecutorService executor = Executors.newFixedThreadPool(2);
		Futures.addCallback(future,
		    new FutureCallback<ResultSet>() {
		        @Override public void onSuccess(ResultSet result) {
		        	List<Row> rows = result.all();
		        	System.out.println("ResultSet size:" + rows.size());
		        	final BatchStatement batch = new BatchStatement();
		        	for (Row row : rows) {
		    			Statement update = QueryBuilder.update(keySpace,tableName)
		    										.with(QueryBuilder.set("salary",row.getInt(4) + toAdd))
		    										.where(QueryBuilder.eq("position",position))
		    										.and(QueryBuilder.eq("last_name",row.getString(1)))
		    										.and(QueryBuilder.eq("first_name",row.getString(2)))
		    										.and(QueryBuilder.eq("id",row.getUUID(3)));
		    			System.out.println(update.toString());
		    			batch.add(update);
		    		}
	    			updSession.execute(batch);
		        }
		 
		        @Override public void onFailure(Throwable t) {
		            System.out.println("Error while reading Cassandra: " + t.getMessage());
		        }
		    },
		    MoreExecutors.listeningDecorator(executor)
		);
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//executor.shutdown();
		return 10;
	}
	
	public int avgByPosition(final Session session, String position) {
		long start = System.nanoTime();
		for (Employee employee : readPosition(session,position)) {
			++count;
			total += employee.getSalary();
		}
		int avg = (int)(total / count);
		System.out.println("avgByPosition: time - " + ((System.nanoTime() - start) / 1000000000.0) + " sec");
		return avg;
	}
	
	public int avgByPositionMapper(final Session session, String position) {
		long start = System.nanoTime();
		Statement select = QueryBuilder.select().all().from(keySpace, tableName)
				.where(QueryBuilder.eq("position",position)).setFetchSize(2000);
		ResultSet results = session.execute(select);
		MappingManager manager = new MappingManager(session);
		Mapper<Employee> mapper = manager.mapper(Employee.class);
		Result<Employee> employees = mapper.map(results);
		employees.forEach((Employee employee)-> {
			++count;
			total += employee.getSalary();
		});
		/*
		for (Employee employee : employees) {
			++count;
			total += employee.getSalary();
		}*/
		int avg = (int)(total / count);
		System.out.println("avgByPositionAsync: time - " + ((System.nanoTime() - start) / 1000000000.0) + " sec"
				+ ", count - " + count);
		return avg;
	}
	
	public int avgByPositionOne(final Session session, String position) {
		long start = System.nanoTime();
		Statement select = QueryBuilder.select().column("salary").from(keySpace, tableName)
				.where(QueryBuilder.eq("position",position)).setFetchSize(2000);
		ResultSet results = session.execute(select);
		List<Row> lst = results.all();
		count = lst.size();
		total = lst.stream().mapToLong(row -> row.getInt(0)).sum();
		/*
		for (Row row : results) {
			++count;
			total += row.getInt(0);
		}*/
		int avg = (int)(total / count);
		System.out.println("avgByPositionAsync: time - " + ((System.nanoTime() - start) / 1000000000.0) + " sec"
				+ ", count - " + count);
		return avg;
	}
	
	public int avgByPositionAsync(final Session session, String position) {
		long start = System.nanoTime();
		Statement select = QueryBuilder.select().column("salary").from(keySpace, tableName)
				.where(QueryBuilder.eq("position",position)).setFetchSize(2000);
		ListenableFuture<ResultSet> resultSet = session.executeAsync(select);
		count = 0;
		total = 0;
		
		ListenableFuture<Integer> average = Futures.transform(resultSet,computeAvg());
		int avg = 0;
		try {
			avg = average.get();
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("avgByPositionAsync: time - " + ((System.nanoTime() - start) / 1000000000.0) + " sec"
				+ ", count - " + count);
		return avg;
	}
	
	private AsyncFunction<ResultSet, Integer> computeAvg() {
		return new AsyncFunction<ResultSet, Integer>() {
			@Override
			public ListenableFuture<Integer> apply(ResultSet rs) {
				int remainingInPage = rs.getAvailableWithoutFetching();
				count += remainingInPage;
				//total += rs.all().stream().limit(remainingInPage).mapToInt(row -> row.getInt(0)).sum();
				
				for (Row row : rs) {
					total += row.getInt(0);
					if (--remainingInPage == 0)
	                    break;
				}
				boolean wasLastPage = rs.getExecutionInfo().getPagingState() == null;
				if (wasLastPage) {
					int avg = (int)(total / count);
					return Futures.immediateFuture(avg);
				}
				else {
					ListenableFuture<ResultSet> future = rs.fetchMoreResults();
	                return Futures.transform(future, computeAvg());
				}
			}
		};
	}
	
	private Result<Employee> readPosition(final Session session, String position) {
		MappingManager manager = new MappingManager(session);
		EmployeeAccessor accessor = manager.createAccessor(EmployeeAccessor.class);
		try {
			return accessor.getAllByPosition(position).get();
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
}
