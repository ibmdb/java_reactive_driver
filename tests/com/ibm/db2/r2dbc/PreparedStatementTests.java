package com.ibm.db2.r2dbc;

import static org.junit.Assert.fail;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.db2.r2dbc.DB2Connection;
import com.ibm.db2.r2dbc.DB2ConnectionConfiguration;
import com.ibm.db2.r2dbc.DB2ConnectionFactory;
import com.ibm.db2.r2dbc.DB2ConnectionPool;
import com.ibm.db2.r2dbc.DB2Result;

import io.r2dbc.spi.Row;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/*
 * We use fixed ordering of test execution, so note that the tests are named test001X, test002X etc.., in ascending order
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PreparedStatementTests extends BaseTestCase
{
	private static final Logger _logger =  LoggerFactory.getLogger(PreparedStatementTests.class.getName());
	
	@BeforeClass
	public static void createTable()
	{
		_logger.debug("createTable() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		Mono<Void> mono = getNewConnection()
								.doOnNext(c -> con.set(c))
								.flatMap(c -> c.executeUpdate("CREATE TABLE UNIT_TEST_"+_randomInt + " (ID INTEGER)"));
		
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
	public void test001Insert()
	{	
		_logger.debug("running test001Insert");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// get connection, insert data and then the release connection
		Mono<Void> mono = _pool.getConnection()
							   .doOnNext(c -> con.set(c))
							   .flatMap(c -> c.executeUpdate("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (100)"))
							   .doAfterTerminate(() -> con.get().release()); 

		StepVerifier.create(mono)
			        .expectComplete()	    
			        .verify();	
	}
	
	/**
	 * Test inserting a row using execute immediate using a new connection.
	 */
	@Test
	public void test002Insert()
	{
		_logger.debug("running test002Insert");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		Mono<Void> mono = getNewConnection()
								.doOnNext(c -> {con.set(c); _con = c;})
								.flatMap(c -> c.executeUpdate("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (200)"));

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
	public void test003Insert()
	{
		_logger.debug("running test003Insert");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		_logger.debug("inserting data - using prepared statement");
		Mono<DB2Result> mono = getNewConnection()
								   .doOnNext(c -> {con.set(c); _con = c;})
								   .flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (25)")
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
	public void test004Insert()
	{
		_logger.debug("running test004Insert");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// get connection, insert data and then release the connection
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (?)")
										   		  	.bind(1, 35)
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
		private String _name;
		
		public Employee(int id, String name)
		{
			_id = id;
			_name = name;
		}
		
		public int getId() {return _id;}
		public String getName() {return _name;}
		
		public String toString() 
		{
			return 	"Employee {\n" +
					"  id: " + _id + "\n" +
					"  name: " + _name + "\n" +
					"}\n";
		}
	}
	
	/**
	 * Test querying for the data inserted. Try getting the field data as integer.
	 */
	@Test
	public void test101Select()
	{
		_logger.debug("running test101Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (?)")
										   		  .bind(1, 2001)
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
							   .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 2001")
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 2001 : "Expected value is 2001, but received = "+row.get("ID");
					})
					.expectComplete()
					.verify();
	}
	
	
	/**
	 * Test querying for the data inserted. Try getting the field data as integer. Insert and Query using independent connections
	 */
	@Test
	public void test102Select()
	{
		_logger.debug("running test102Select");
		
		// Get connection and insert data
		AtomicReference<DB2Connection> con1 = new AtomicReference<DB2Connection>();
		Mono<DB2Result> mono = getNewConnection()
									.doOnNext(c -> con1.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (?)")
										   		  .bind(1, 2002)
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
							    .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 2002")
								 			  	.execute())
							    .flatMapMany(result -> result.map((row, md) -> row));
		
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 2002 : "Expected value is 2002, but received = "+row.get("ID");
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
	public void test103Select()
	{
		_logger.debug("running test103Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection, insert data and then release the connection
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (?)")
											   		  .bind(1, 2003)
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
							  .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 2003")
									  			.execute())
							  .flatMapMany(result -> result.map((row, md) -> row))
							  .doAfterTerminate(() -> con.get().release());

		// Test the queried data
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert (Integer)row.get("ID") == 2003 : "Expected value is 2003, but received = "+row.get("ID");
					})
					.expectComplete()
					.verify();
	}
	
	/**
	 * Test querying for the data inserted using a mapped object to the result row.
	 */
	@Test
	public void test110Select()
	{
		_logger.debug("running test110Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection insert data and release the connection used
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (?)")
										   		  .bind(1, 2010)
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
									.flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 2010")
													.execute())
								   .flatMapMany(result -> result.map((row, md) -> new Employee(row.get("ID", Integer.class), "")))
								   .doAfterTerminate(() -> con.get().release());

		// Test queried data
		StepVerifier.create(flux)
					.assertNext(emp -> {
					 	assert emp != null : "Received a null object";
					 	assert emp instanceof Employee : "Did not receive an Employee object";
						assert emp.getId() == 2010 : "Expected value 100, but received "+emp.getId();
					})
					.expectComplete()
					.verify();
	}
	
	
	/* UPDATE Tests */

	/**
	 * Test modifying an existing data using a prepared statement with bind value.
	 */
	@Test
	public void test201Update()
	{
		_logger.debug("running test201Update");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (?)")
										   		  .bind(1, 3001)
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
		Mono<DB2Result> mono2 = Mono.just(con.get())
								   	.flatMap(c -> c.createStatement("UPDATE UNIT_TEST_"+_randomInt+" SET ID = ? WHERE ID = 3001")
								   					.bind(1, 4001)
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
							   .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 4001")
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 4001 : "Expected value is 100, but received = "+row.get("ID");
					})
					.expectComplete()
					.verify();
	}
	
	/**
	 * Test modifying an existing data using a prepared statement without any bind value.
	 */
	@Test
	public void test202Update()
	{
		_logger.debug("running test202Update");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (?)")
										   		  .bind(1, 3002)
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
								   	.flatMap(c -> c.createStatement("UPDATE UNIT_TEST_"+_randomInt+" SET ID = 4002 WHERE ID = 3002")
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
							   .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 4002")
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 4002 : "Expected value is 4002, but received = "+row.get("ID");
					})
					.expectComplete()
					.verify();
	}
	
	/**
	 * Test updating a row using execute immediate
	 */
	@Test
	public void test203Update()
	{	
		_logger.debug("running test203Update");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// get connection, insert data 
		Mono<Void> mono = _pool.getConnection()
							   .doOnNext(c -> con.set(c))
							   .flatMap(c -> c.executeUpdate("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (3003)"));

		// Test
		StepVerifier.create(mono)
			        .expectComplete()
			        .verify();	
		
		// update data in a row 
		Mono<Void> mono2 = Mono.just(con.get())
							   .flatMap(c -> c.executeUpdate("UPDATE UNIT_TEST_"+_randomInt + " SET ID = 3004 WHERE ID = 3003"));

		// Test
		StepVerifier.create(mono2)
			        .expectComplete()
			        .verify();	
		
		// Query the deleted data, we should get no rows and then release the connection
		Flux<Row> flux = Mono.just(con.get())
							   .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 3004")
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());
		
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 3004 : "Expected value is 3004, but received = "+row.get("ID");
					})
					.expectComplete()
					.verify();
	}
	
	
	/* DELETE Tests */
	
	/**
	 * Test deleting an existing data using a prepared statement with bind value.
	 */
	@Test
	public void test301Delete()
	{
		_logger.debug("running test301Delete");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (?)")
										   		  .bind(1, 5001)
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
								   	.flatMap(c -> c.createStatement("DELETE FROM UNIT_TEST_"+_randomInt+" WHERE ID = ?")
								   					.bind(1, 5001) 
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
							   .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 5001")
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
	public void test302Delete()
	{
		_logger.debug("running test302Delete");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (?)")
										   		  .bind(1, 5002)
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
								   	.flatMap(c -> c.createStatement("DELETE FROM UNIT_TEST_"+_randomInt+" WHERE ID = 5002")
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
							   .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 5002")
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
	public void test303Delete()
	{	
		_logger.debug("running test303Delete");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// get connection, insert data 
		Mono<Void> mono = _pool.getConnection()
							   .doOnNext(c -> con.set(c))
							   .flatMap(c -> c.executeUpdate("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (5003)"));

		// Test
		StepVerifier.create(mono)
			        .expectComplete()
			        .verify();	
		
		// delete data 
		Mono<Void> mono2 = Mono.just(con.get())
							   .flatMap(c -> c.executeUpdate("DELETE FROM UNIT_TEST_"+_randomInt + " WHERE ID = 5003"));

		// Test
		StepVerifier.create(mono2)
			        .expectComplete()
			        .verify();	
		
		// Query the deleted data, we should get no rows and then release the connection
		Flux<Row> flux = Mono.just(con.get())
							   .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 5003")
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.expectComplete()
					.verify();
	}
	
	/**
	 * Test executing bad sqls (syntax error etc.,) - INSERT
	 */
	@Test
	public void test601Negative()
	{	
		_logger.debug("running test303Delete");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// get connection, insert data and release connection
		Flux<Row> flux = _pool.getConnection()
							   .doOnNext(c -> con.set(c))
							   .flatMap(c -> c.createStatement("INSERT UNIT_TEST_"+_randomInt +" (ID) VALUES (6001)")  // missing INTO 
										  			.execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
			        .expectError()
			        .verify();	
		
	}
	
	/**
	 * Test executing bad sqls (syntax error etc.,) - SELECT 
	 */
	@Test
	public void test602Negative()
	{	
		_logger.debug("running test303Delete");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// get connection, select data and release connection
		Flux<Row> flux = _pool.getConnection()
							   .doOnNext(c -> con.set(c))
							   .flatMap(c -> c.createStatement("SELECT * UNIT_TEST_"+_randomInt) // missing FROM 
										  			.execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
			        .expectError()
			        .verify();	
	}
	
	/**
	 * Test executing bad sqls (syntax error etc.,) - UPDATE 
	 */
	@Test
	public void test603Negative()
	{	
		_logger.debug("running test303Delete");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// get connection, select data and release connection
		Flux<Row> flux = _pool.getConnection()
							   .doOnNext(c -> con.set(c))
							   .flatMap(c -> c.createStatement("UPDATE UNIT_TEST_"+_randomInt + " SET something") // missing after SET
										  			.execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
			        .expectError()
			        .verify();	
	}
	
	/**
	 * Test executing bad sqls (syntax error etc.,) - DELETE
	 */
	@Test
	public void test604Negative()
	{	
		_logger.debug("running test303Delete");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// get connection, select data and release connection
		Flux<Row> flux = _pool.getConnection()
							   .doOnNext(c -> con.set(c))
							   .flatMap(c -> c.createStatement("DELETE UNIT_TEST_"+_randomInt + " WHERE something") // missing FROM
										  			.execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
			        .expectError()
			        .verify();		
	}
	
	/**
	 * Test executing bad sqls (syntax error etc.,) - execute immediate
	 */
	@Test
	public void test605Negative()
	{	
		_logger.debug("running test303Delete");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// get connection, insert data and release connection
		Mono<Void> mono = _pool.getConnection()
							   .doOnNext(c -> con.set(c))
							   .flatMap(c -> c.executeUpdate("DELETE UNIT_TEST_"+_randomInt + " WHERE something")) // missing FROM
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(mono)
			        .expectError()
			        .verify();	
		
	}
}
