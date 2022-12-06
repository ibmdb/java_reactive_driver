package com.ibm.db2.r2dbc;

import java.sql.Date;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.db2.r2dbc.DB2Connection;
import com.ibm.db2.r2dbc.DB2Result;
import com.ibm.db2.r2dbc.PreparedStatementTests.Employee;

import io.r2dbc.spi.Row;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/*
 * We use fixed ordering of test execution, so note that the tests are named test1X, test2X etc..,
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DateTypeTests extends BaseTestCase
{
	private static final Logger _logger =  LoggerFactory.getLogger(DateTypeTests.class.getName());

	public DateTypeTests()
	{
		super();
		
		_logger.debug("constructor called");
	}
	
	@BeforeClass
	public static void createTable()
	{
		_logger.debug("createTable() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		Mono<Void> mono = getNewConnection()
								.doOnNext(c -> con.set(c))
								.flatMap(c -> c.executeUpdate("CREATE TABLE UNIT_TEST_"+_randomInt + " (ID INTEGER, START_DATE DATE NOT NULL, END_DATE DATE)"));
		
		StepVerifier.create(mono)
		            .expectComplete()
		            .verify();	
		
		con.get().close().block();
	}
	
	/* DDL Tests */
	
	@Test  
	public void test001CreateTableWithDateColumn()
	{
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		Mono<Void> mono = getNewConnection()
								.doOnNext(c -> con.set(c))
								.flatMap(c -> c.executeUpdate("CREATE TABLE UNIT_TEST_1_"+_randomInt + " (D DATE)"));
		
		StepVerifier.create(mono)
		            .expectComplete()
		            .verify();	
		
		con.get().close().block();	
	}
	
	@Test  
	public void test002DropTableWithDateColumn()
	{
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		Mono<Void> mono = getNewConnection()
								.doOnNext(c -> con.set(c))
								.flatMap(c -> c.executeUpdate("DROP TABLE UNIT_TEST_1_"+_randomInt));
		
		StepVerifier.create(mono)
		            .expectComplete()
		            .verify();	
		
		con.get().close().block();	
	}

	
	/* INSERT Tests */
	
	/**
	 * Test inserting a row using execute immediate
	 */
	@Test
	public void test101Insert()
	{	
		_logger.debug("running test101Insert");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// get connection, insert data and then the release connection
		Mono<Void> mono = _pool.getConnection()
							   .doOnNext(c -> con.set(c))
							   .flatMap(c -> c.executeUpdate("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, START_DATE) VALUES (100, '2019-12-25')"))
							   .doAfterTerminate(() -> con.get().release()); 

		StepVerifier.create(mono)
			        .expectComplete()	    
			        .verify();	
	}
	
	/**
	 * Test inserting a row using execute immediate using a new connection.
	 */
	@Test
	public void test102Insert()
	{
		_logger.debug("running test102Insert");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		Mono<Void> mono = getNewConnection()
								.doOnNext(c -> {con.set(c); _con = c;})
								.flatMap(c -> c.executeUpdate("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, START_DATE) VALUES (200, '2019-12-25')"));

		StepVerifier.create(mono)
			        .expectComplete()
			        .verify();	
		
		// close the connection used otherwise the drop table in cleanup will timeout and fail
		con.get().close().block();
		_con = null;
	}
	
	/**
	 * Test inserting a row using a prepared statement
	 */
	@Test
	public void test103Insert()
	{
		_logger.debug("running test103Insert");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		_logger.debug("inserting data - using prepared statement");
		Mono<DB2Result> mono = getNewConnection()
								   .doOnNext(c -> {con.set(c); _con = c;})
								   .flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, START_DATE) VALUES (25, '2019-12-25')")
												  .execute());
		
		_logger.debug("verifying..");
		StepVerifier.create(mono)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows inserted = 1, but received = "+rs.getNumRowsUpdated();
			        })
		            .expectComplete()
		            .verify();	
		
		_logger.debug("closing the connection");
		// close the connection used otherwise the drop table in cleanup will timeout and fail
		con.get().close().block();
		_con = null;
	}
	
	/**
	 * Test inserting a row using a prepared statement with bind values
	 */
	@Test
	public void test104Insert()
	{
		_logger.debug("running test104Insert");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// get connection, insert data and then release the connection
		Date date = Date.valueOf("2019-12-25");
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, START_DATE) VALUES (?, ?)")
										   		  	.bind(1, 35)
										   		  	.bind(2, date)
										   		  	.execute())
									.doAfterTerminate(() -> con.get().release());
		
		StepVerifier.create(mono)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows inserted = 1, but received = "+rs.getNumRowsUpdated();
			        })
		            .expectComplete()
		            .verify();	
	}
	
	/**
	 * Test inserting a row using execute immediate with Nullable values
	 */
	@Test
	public void test105Insert()
	{	
		_logger.debug("running test105Insert");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// get connection, insert data and then the release connection
		Mono<Void> mono = _pool.getConnection()
							   .doOnNext(c -> con.set(c))
							   .flatMap(c -> c.executeUpdate("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, START_DATE, END_DATE) VALUES (100, '2019-12-25', NULL)"))
							   .doAfterTerminate(() -> con.get().release()); 

		StepVerifier.create(mono)
			        .expectComplete()	    
			        .verify();	
	}
	
	/**
	 * Test inserting a row using a prepared statement with bind values and Null values
	 */
	@Test
	public void test106Insert()
	{
		_logger.debug("running test106Insert");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// get connection, insert data and then release the connection
		Date date = Date.valueOf("2019-12-25");
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, START_DATE, END_DATE) VALUES (?, ?, ?)")
										   		  	.bind(1, 35)
										   		  	.bind(2, date)
										   		  	.bindNull(3, Date.class)
										   		  	.execute())
									.doAfterTerminate(() -> con.get().release());
		
		StepVerifier.create(mono)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows inserted = 1, but received = "+rs.getNumRowsUpdated();
			        })
		            .expectComplete()
		            .verify();	
	}
	
	/* SELECT Tests */
	
	// Object to which Row data is mapped
	public static class Employee 
	{
		private int _id;
		private Date _startDate;
		
		public Employee(int id, Date startDate)
		{
			_id = id;
			_startDate = startDate;
		}
		
		public int getId() {return _id;}
		public Date getStartDate() {return _startDate;}
		
		public String toString() 
		{
			return 	"Employee {\n" +
					"  id: " + _id + "\n" +
					"  startDate: " + _startDate + "\n" +
					"}\n";
		}
	}
	
	/**
	 * Test querying for the data inserted. Try getting the field data as integer.
	 */
	@Test
	public void test201Select()
	{
		_logger.debug("running test201Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		Date date = Date.valueOf("2019-10-02");
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, START_DATE) VALUES (?, ?)")
										   		  .bind(1, 2001)
										   		  .bind(2, date)
												  .execute());
		// Test
		StepVerifier.create(mono)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows inserted = 1, but received = "+rs.getNumRowsUpdated();
			        })
		            .expectComplete()
		            .verify();

		// Query the inserted data and then release the connection
		Flux<Row> flux = Mono.just(con.get())
							   .flatMap(c -> c.createStatement("SELECT ID, START_DATE FROM UNIT_TEST_"+_randomInt+" WHERE ID = 2001")
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 2001 : "Expected value is 2001, but received = "+row.get("ID");
					 	assert row.get("START_DATE", Date.class).equals(date) : "Expected value is 2019-10-02, but received = "+row.get("START_DATE");
					})
					.expectComplete()
					.verify();
	}
	
	
	/**
	 * Test querying for the data inserted. Try getting the field data as integer. Insert and Query using independent connections
	 */
	@Test
	public void test202Select()
	{
		_logger.debug("running test202Select");
		
		// Get connection and insert data
		Date date = Date.valueOf("2019-10-02");
		AtomicReference<DB2Connection> con1 = new AtomicReference<DB2Connection>();
		Mono<DB2Result> mono = getNewConnection()
									.doOnNext(c -> con1.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, START_DATE) VALUES (?, ?)")
										   		  .bind(1, 2002)
										   		  .bind(2, date)
												  .execute());
		// Test
		StepVerifier.create(mono)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows inserted = 1, but received = "+rs.getNumRowsUpdated();
			        })
		            .expectComplete()
		            .verify();
		
		// close the connection used
		con1.get().close().block();
		
		// Query the inserted data using a new connection
		AtomicReference<DB2Connection> con2 = new AtomicReference<DB2Connection>();
		Flux<Row> flux = getNewConnection()
								.doOnNext(c -> con2.set(c))
							    .flatMap(c -> c.createStatement("SELECT ID, START_DATE FROM UNIT_TEST_"+_randomInt+" WHERE ID = 2002")
								 			  	.execute())
							    .flatMapMany(result -> result.map((row, md) -> row));
		
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 2002 : "Expected value is 2002, but received = "+row.get("ID");
					 	assert row.get("START_DATE", Date.class).equals(date) : "Expected value is 2019-10-02, but received = "+row.get("START_DATE");
					})
					.expectComplete()
					.verify();
		
		// close the connection used
		con2.get().close().block();

	}
	
	
	/**
	 * Test querying for the data inserted. Try getting the field data using its default type.
	 */
	@Test
	public void test203Select()
	{
		_logger.debug("running test203Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection, insert data and then release the connection
		Date date = Date.valueOf("2019-10-02");
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, START_DATE) VALUES (?, ?)")
											   		  .bind(1, 2003)
											   		  .bind(2, date)
													  .execute())
									.doAfterTerminate(() -> con.get().release());
		
		// Test insert
		StepVerifier.create(mono)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows inserted = 1, but received = "+rs.getNumRowsUpdated();
			        })
		            .expectComplete()
		            .verify();

		// Get connection, query the inserted data and then release the connection
		Flux<Row> flux = _pool.getConnection()
							  .doOnNext(c -> con.set(c))
							  .flatMap(c -> c.createStatement("SELECT ID, START_DATE FROM UNIT_TEST_"+_randomInt+" WHERE ID = 2003")
									  			.execute())
							  .flatMapMany(result -> result.map((row, md) -> row))
							  .doAfterTerminate(() -> con.get().release());

		// Test the queried data
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert (Integer)row.get("ID") == 2003 : "Expected value is 2003, but received = "+row.get("ID");
					 	assert row.get("START_DATE", Date.class).equals(date) : "Expected value is 2019-10-02, but received = "+row.get("START_DATE");
					})
					.expectComplete()
					.verify();
	}
	
	
	/**
	 * Test querying for the data inserted using as null valued column in the where clause. Try getting the field data using its default type.
	 */
	@Test
	public void test204Select()
	{
		_logger.debug("running test204Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection, insert data and then release the connection
		Date date = Date.valueOf("2019-10-02");
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, START_DATE, END_DATE) VALUES (?, ?, ?)")
											   		  .bind(1, 2003)
											   		  .bind(2, date)
											   		  .bindNull(3, Date.class)
													  .execute())
									.doAfterTerminate(() -> con.get().release());
		
		// Test insert
		StepVerifier.create(mono)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows inserted = 1, but received = "+rs.getNumRowsUpdated();
			        })
		            .expectComplete()
		            .verify();

		// Get connection, query the inserted data and then release the connection
		Flux<Row> flux = _pool.getConnection()
							  .doOnNext(c -> con.set(c))
							  .flatMap(c -> c.createStatement("SELECT ID, START_DATE FROM UNIT_TEST_"+_randomInt+" WHERE END_DATE is NULL")
									  			.execute())
							  .flatMapMany(result -> result.map((row, md) -> row))
							  .doAfterTerminate(() -> con.get().release());

		// Test the queried data
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert (Integer)row.get("ID") == 2003 : "Expected value is 2003, but received = "+row.get("ID");
					 	assert row.get("START_DATE", Date.class).equals(date) : "Expected value is 2019-10-02, but received = "+row.get("START_DATE");
					})
					.expectComplete()
					.verify();
	}
	
	
	/**
	 * Test querying for the data inserted using a mapped object to the result row.
	 */
	@Test
	public void test210Select()
	{
		_logger.debug("running test210Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection insert data and release the connection used
		Date date = Date.valueOf("2019-10-02");
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, START_DATE) VALUES (?, ?)")
										   		  .bind(1, 2010)
										   		  .bind(2, date)
												  .execute())
									.doAfterTerminate(() -> con.get().release());
		
		// Test insert
		StepVerifier.create(mono)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows inserted = 1, but received = "+rs.getNumRowsUpdated();
			        })
		            .expectComplete()
		            .verify();

		// Query the inserted data and map it to Employee object, then release the connection used
		Flux<Employee> flux = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("SELECT ID, START_DATE FROM UNIT_TEST_"+_randomInt+" WHERE ID = 2010")
													.execute())
								   .flatMapMany(result -> result.map((row, md) -> new Employee(row.get("ID", Integer.class), row.get("START_DATE", Date.class))))
								   .doAfterTerminate(() -> con.get().release());

		// Test queried data
		StepVerifier.create(flux)
					.assertNext(emp -> {
					 	assert emp != null : "Received a null object";
					 	assert emp instanceof Employee : "Did not receive an Employee object";
						assert emp.getId() == 2010 : "Expected value 100, but received "+emp.getId();
						assert emp.getStartDate().equals(date) : "Expected date value is 2019-10-02, but received "+emp.getStartDate();
					})
					.expectComplete()
					.verify();
	}
	
	/* UPDATE Tests */

	/**
	 * Test modifying an existing data using a prepared statement with bind value.
	 */
	@Test
	public void test301Update()
	{
		_logger.debug("running test301Update");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		Date date1 = Date.valueOf("2019-10-02");
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, START_DATE) VALUES (?, ?)")
										   		  .bind(1, 3001)
										   		  .bind(2, date1)
												  .execute());
		// Test
		StepVerifier.create(mono)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows inserted = 1, but received = "+rs.getNumRowsUpdated();
			        })
		            .expectComplete()
		            .verify();

		// Modify the inserted data and then release the connection
		Date date2 = Date.valueOf("2019-12-30");
		Mono<DB2Result> mono2 = Mono.just(con.get())
								   	.flatMap(c -> c.createStatement("UPDATE UNIT_TEST_"+_randomInt+" SET START_DATE = ? WHERE ID = 3001")
								   					.bind(1, date2)
												  	.execute());

		// Test
		StepVerifier.create(mono2)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows updated = 1, but received = "+rs.getNumRowsUpdated();
			        })
			        .expectComplete()
			        .verify();

		// Query the updated data and then release the connection
		Flux<Row> flux = Mono.just(con.get())
							   .flatMap(c -> c.createStatement("SELECT START_DATE FROM UNIT_TEST_"+_randomInt+" WHERE ID = 3001")
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("START_DATE", Date.class).equals(date2) : "Expected value is '2019-12-30, but received = "+row.get("START_DATE");
					})
					.expectComplete()
					.verify();
	}
	
	
	
	/**
	 * Test modifying an existing data using a prepared statement without any bind value.
	 */
	@Test
	public void test302Update()
	{
		_logger.debug("running test302Update");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		Date date1 = Date.valueOf("2019-10-02");
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, START_DATE) VALUES (?, ?)")
										   		  .bind(1, 3002)
										   		  .bind(2, date1)
												  .execute());
		// Test
		StepVerifier.create(mono)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows inserted = 1, but received = "+rs.getNumRowsUpdated();
			        })
		            .expectComplete()
		            .verify();

		// Modify the inserted data 
		Mono<DB2Result> mono2 = Mono.just(con.get())
								   	.flatMap(c -> c.createStatement("UPDATE UNIT_TEST_"+_randomInt+" SET START_DATE = '2019-12-30' WHERE ID = 3002")
												  	.execute());

		// Test
		StepVerifier.create(mono2)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows updated = 1, but received = "+rs.getNumRowsUpdated();
			        })
			        .expectComplete()
			        .verify();

		// Query the updated data and then release the connection
		Flux<Row> flux = Mono.just(con.get())
							   .flatMap(c -> c.createStatement("SELECT START_DATE FROM UNIT_TEST_"+_randomInt+" WHERE ID = 3002")
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		Date date2 = Date.valueOf("2019-12-30");
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("START_DATE", Date.class).equals(date2) : "Expected value is '2019-12-30', but received = "+row.get("START_DATE");
					})
					.expectComplete()
					.verify();
	}
	
	/**
	 * Test updating a row using execute immediate
	 */
	@Test
	public void test303Update()
	{	
		_logger.debug("running test303Update");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// get connection, insert data 
		Mono<Void> mono = _pool.getConnection()
							   .doOnNext(c -> con.set(c))
							   .flatMap(c -> c.executeUpdate("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, START_DATE) VALUES (3003, '2019-10-02')"));

		// Test
		StepVerifier.create(mono)
			        .expectComplete()
			        .verify();	
		
		// update data in a row 
		Mono<Void> mono2 = Mono.just(con.get())
							   .flatMap(c -> c.executeUpdate("UPDATE UNIT_TEST_"+_randomInt + " SET START_DATE = '2019-12-30' WHERE ID = 3003"));

		// Test
		StepVerifier.create(mono2)
			        .expectComplete()
			        .verify();	
		
		// Query the deleted data, we should get no rows and then release the connection
		Flux<Row> flux = Mono.just(con.get())
							   .flatMap(c -> c.createStatement("SELECT START_DATE FROM UNIT_TEST_"+_randomInt+" WHERE ID = 3003")
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());
		
		// Test
		Date date2 = Date.valueOf("2019-12-30");
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("START_DATE", Date.class).equals(date2) : "Expected value is 3004, but received = "+row.get("ID");
					})
					.expectComplete()
					.verify();
	}
	
	/**
	 * Test modifying an existing data using a prepared statement with bind value.
	 */
	@Test
	public void test304Update()
	{
		_logger.debug("running test304Update");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		Date date1 = Date.valueOf("2019-10-02");
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, START_DATE, END_DATE) VALUES (?, ?, ?)")
										   		  .bind(1, 3001)
										   		  .bind(2, date1)
										   		  .bindNull(3, Date.class)
												  .execute());
		// Test
		StepVerifier.create(mono)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows inserted = 1, but received = "+rs.getNumRowsUpdated();
			        })
		            .expectComplete()
		            .verify();

		// Modify the inserted data and then release the connection
		Date date2 = Date.valueOf("2019-12-30");
		Mono<DB2Result> mono2 = Mono.just(con.get())
								   	.flatMap(c -> c.createStatement("UPDATE UNIT_TEST_"+_randomInt+" SET END_DATE = ? WHERE ID = 3001")
								   					.bind(1, date2)
												  	.execute());

		// Test
		StepVerifier.create(mono2)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows updated = 1, but received = "+rs.getNumRowsUpdated();
			        })
			        .expectComplete()
			        .verify();

		// Query the updated data and then release the connection
		Flux<Row> flux = Mono.just(con.get())
							   .flatMap(c -> c.createStatement("SELECT END_DATE FROM UNIT_TEST_"+_randomInt+" WHERE ID = 3001")
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("END_DATE", Date.class).equals(date2) : "Expected value is '2019-12-30, but received = "+row.get("END_DATE");
					})
					.expectComplete()
					.verify();
	}
	
	/**
	 * Test modifying an existing data using a prepared statement with bind value.
	 */
	@Test
	public void test305Update()
	{
		_logger.debug("running test305Update");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		Date date1 = Date.valueOf("2019-10-02");
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, START_DATE, END_DATE) VALUES (?, ?, ?)")
										   		  .bind(1, 3001)
										   		  .bind(2, date1)
										   		  .bind(3, date1)
												  .execute());
		// Test
		StepVerifier.create(mono)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows inserted = 1, but received = "+rs.getNumRowsUpdated();
			        })
		            .expectComplete()
		            .verify();

		// Modify the inserted data and then release the connection
		Date date2 = Date.valueOf("2019-12-30");
		Mono<DB2Result> mono2 = Mono.just(con.get())
								   	.flatMap(c -> c.createStatement("UPDATE UNIT_TEST_"+_randomInt+" SET END_DATE = ? WHERE ID = 3001")
								   					.bindNull(1, Date.class)
												  	.execute());

		// Test
		StepVerifier.create(mono2)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows updated = 1, but received = "+rs.getNumRowsUpdated();
			        })
			        .expectComplete()
			        .verify();

		// Query the updated data and then release the connection
		Flux<Row> flux = Mono.just(con.get())
							   .flatMap(c -> c.createStatement("SELECT END_DATE FROM UNIT_TEST_"+_randomInt+" WHERE ID = 3001")
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("END_DATE", Date.class) == null : "Expected value is '2019-12-30, but received = "+row.get("END_DATE");
					})
					.expectComplete()
					.verify();
	}
	
	/* DELETE Tests */
	
	/**
	 * Test deleting an existing data using a prepared statement with bind value.
	 */
	@Test
	public void test401Delete()
	{
		_logger.debug("running test401Delete");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		Date date = Date.valueOf("2019-12-30");
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, START_DATE) VALUES (?, ?)")
										   		  .bind(1, 5001)
										   		  .bind(2, date)
												  .execute());
		// Test
		StepVerifier.create(mono)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows inserted = 1, but received = "+rs.getNumRowsUpdated();
			        })
		            .expectComplete()
		            .verify();

		// Delete the inserted data
		Mono<DB2Result> mono2 = Mono.just(con.get())
								   	.flatMap(c -> c.createStatement("DELETE FROM UNIT_TEST_"+_randomInt+" WHERE START_DATE = ?")
								   					.bind(1, date) 
												  	.execute());
		// Test
		StepVerifier.create(mono2)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows delete = 1, but received = "+rs.getNumRowsUpdated();
			        })
			        .expectComplete()
			        .verify();

		// Query the deleted data, we should get no rows and then release the connection
		Flux<Row> flux = Mono.just(con.get())
							   .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE START_DATE = ?")
									   		  .bind(1, date)
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.expectComplete()
					.verify();
	}
	
	/**
	 * Test deleting an existing data using a prepared statement without any bind value. 
	 */
	@Test
	public void test402Delete()
	{
		_logger.debug("running test402Delete");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		Date date = Date.valueOf("2019-12-30");
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, START_DATE) VALUES (?, ?)")
										   		  .bind(1, 5002)
										   		  .bind(2, date)
												  .execute());
		// Test
		StepVerifier.create(mono)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows inserted = 1, but received = "+rs.getNumRowsUpdated();
			        })
		            .expectComplete()
		            .verify();

		// Delete the inserted data
		Mono<DB2Result> mono2 = Mono.just(con.get())
								   	.flatMap(c -> c.createStatement("DELETE FROM UNIT_TEST_"+_randomInt+" WHERE START_DATE = '2019-12-30'")
												  	.execute());

		// Test
		StepVerifier.create(mono2)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows delete = 1, but received = "+rs.getNumRowsUpdated();
			        })
			        .expectComplete()
			        .verify();

		// Query the deleted data, we should get no rows and then release the connection
		Flux<Row> flux = Mono.just(con.get())
							   .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE START_DATE = '2019-12-30'")
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.expectComplete()
					.verify();
	}
	
	/**
	 * Test deleting a row using execute immediate
	 */
	@Test
	public void test403Delete()
	{	
		_logger.debug("running test403Delete");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// get connection, insert data 
		Mono<Void> mono = _pool.getConnection()
							   .doOnNext(c -> con.set(c))
							   .flatMap(c -> c.executeUpdate("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, START_DATE) VALUES (5003, '2019-12-30')"));

		// Test
		StepVerifier.create(mono)
			        .expectComplete()
			        .verify();	
		
		// delete data 
		Mono<Void> mono2 = Mono.just(con.get())
							   .flatMap(c -> c.executeUpdate("DELETE FROM UNIT_TEST_"+_randomInt + " WHERE START_DATE = '2019-12-30'"));

		// Test
		StepVerifier.create(mono2)
			        .expectComplete()
			        .verify();	
		
		// Query the deleted data, we should get no rows and then release the connection
		Flux<Row> flux = Mono.just(con.get())
							   .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE START_DATE = '2019-12-30'")
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.expectComplete()
					.verify();
	}
	
	/**
	 * Test deleting an existing data using a prepared statement with bind null value.
	 */
	@Test
	public void test404Delete()
	{
		_logger.debug("running test404Delete");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		Date date = Date.valueOf("2019-12-30");
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, START_DATE, END_DATE) VALUES (?, ?, ?)")
										   		  .bind(1, 5001)
										   		  .bind(2, date)
										   		  .bindNull(3, Date.class)
												  .execute());
		// Test
		StepVerifier.create(mono)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows inserted = 1, but received = "+rs.getNumRowsUpdated();
			        })
		            .expectComplete()
		            .verify();

		// Delete the inserted data
		Mono<DB2Result> mono2 = Mono.just(con.get())
								   	.flatMap(c -> c.createStatement("DELETE FROM UNIT_TEST_"+_randomInt+" WHERE START_DATE = ?")						
								   					.bind(1, date)
												  	.execute());
		// Test
		StepVerifier.create(mono2)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows delete = 1, but received = "+rs.getNumRowsUpdated();
			        })
			        .expectComplete()
			        .verify();

		// Query the deleted data, we should get no rows and then release the connection
		Flux<Row> flux = Mono.just(con.get())
							   .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE END_DATE = ?")
									   		  .bindNull(1, Date.class)
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.expectComplete()
					.verify();
	}
}
