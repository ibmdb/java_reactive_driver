package com.ibm.db2.r2dbc;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.db2.r2dbc.DB2Connection;
import com.ibm.db2.r2dbc.DB2Result;

import io.r2dbc.spi.Row;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/*
 * We use fixed ordering of test execution, so note that the tests are named test1X, test2X etc..,
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class VarCharAsciiTypeTests extends BaseTestCase
{
	private static final Logger _logger =  LoggerFactory.getLogger(VarCharAsciiTypeTests.class.getName());

	public VarCharAsciiTypeTests()
	{
		super();
		
		_logger.debug("constructor called");
	}
	
	/* Test String subtypes */
	
	/*
	 *  FOR < SBCS | MIXED | BIT > DATA 
	 *  
	 *  - or -
	 *  
	 *  CCSID 1208 -- for EBCDIC table
	 *    
	 *  Table level CCSID specification:
	 *  
	 *  CREATE TABLE <table-name> ( ... ) CCSID < ASCII | EBCDIC | UNICODE > 
	 */

	/*
	 *  ASCII Table 
	 *  
	 *  FOR < SBCS | MIXED | BIT > DATA 
	 *  
	 */
	
	@BeforeClass
	public static void createASCIITableA()
	{
		_logger.debug("createTable() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		String createTable;
		if (_isLUW) {
			// CCSID ASCII specification is not allowed in LUW
			createTable = "CREATE TABLE UNIT_TEST_A_A_"+_randomInt + " (NAME VARCHAR(25))";
		}
		else {
			createTable = "CREATE TABLE UNIT_TEST_A_A_"+_randomInt + " (NAME VARCHAR(25)) CCSID ASCII";
		}
		Mono<Void> mono = getNewConnection()
								.doOnNext(c -> con.set(c))
								.flatMap(c -> c.executeUpdate(createTable));
		
		StepVerifier.create(mono)
		            .expectComplete()
		            .verify();	
		
		con.get().close().block();
	}
	
	@BeforeClass
	public static void createASCIITableB()
	{
		_logger.debug("createTable() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		String createTable;
		if (_isLUW) {
			// FOR SBCS DATA and CCSID specification is not valid in LUW
			createTable= "CREATE TABLE UNIT_TEST_A_B_"+_randomInt + " (NAME VARCHAR(25))";
		}
		else {
			createTable = "CREATE TABLE UNIT_TEST_A_B_"+_randomInt + " (NAME VARCHAR(25) FOR SBCS DATA) CCSID ASCII";
		}
		Mono<Void> mono = getNewConnection()
								.doOnNext(c -> con.set(c))
								.flatMap(c -> c.executeUpdate(createTable));
		
		StepVerifier.create(mono)
		            .expectComplete()
		            .verify();	
		
		con.get().close().block();
	}
	
	@BeforeClass
	public static void createASCIITableC()
	{
		_logger.debug("createTable() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		String createTable;
		if (_isLUW) {
			// CCSID ASCII specificstion is not valid in LUW also BIT DATA has limitations
			createTable = "CREATE TABLE UNIT_TEST_A_C_"+_randomInt + " (NAME VARCHAR(25))";
		}
		else {
			createTable = "CREATE TABLE UNIT_TEST_A_C_"+_randomInt + " (NAME VARCHAR(25) FOR BIT DATA) CCSID ASCII";
		}
		Mono<Void> mono = getNewConnection()
								.doOnNext(c -> con.set(c))
								.flatMap(c -> c.executeUpdate(createTable));
		
		StepVerifier.create(mono)
		            .expectComplete()
		            .verify();	
		
		con.get().close().block();
	}
	
	/**
	 * Test querying for the existing data.
	 */
	@Test
	public void test601Select()
	{
		_logger.debug("running test601Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_A_A_"+_randomInt + " (NAME) VALUES ('Sachin Tendulkar')")
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
		String s = "Sachin Tendulkar";
		Flux<Row> flux = Mono.just(con.get())
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_A_A_"+_randomInt+" WHERE NAME = ?")
									   		  .bind(1, s)
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("NAME", String.class).equals(s) : "Expected value is 'Sachin Tendulkar', but received = "+row.get("NAME");
					})
					.expectComplete()
					.verify();
	}
	
	
	/**
	 * Test querying for the data inserted.
	 */
	@Test
	public void test602Select()
	{
		_logger.debug("running test602Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String s = "Sachin Tendulkar";  
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_A_A_"+_randomInt + " (NAME) VALUES (?)")
										   		   .bind(1, s)
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
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_A_A_"+_randomInt+" WHERE NAME = 'Sachin Tendulkar'")
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("NAME", String.class).equals(s) : "Expected value is 'Sachin Tendulkar', but received = "+row.get("NAME");
					})
					.expectComplete()
					.verify();
	}
	
	
	/**
	 * Test querying for the data inserted.
	 */
	@Test
	public void test603Select()
	{
		_logger.debug("running test603Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String s = "Sachin Tendulkar";  
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_A_A_"+_randomInt + " (NAME) VALUES (?)")
										   		   .bind(1, s)
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
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_A_A_"+_randomInt+" WHERE NAME = ?")
									   		  .bind(1, s)
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("NAME", String.class).equals(s) : "Expected value is 'Sachin Tendulkar', but received = "+row.get("NAME");
					})
					.expectComplete()
					.verify();
	}
	
	/**
	 * Test querying for the existing data.
	 */
	@Test
	public void test604Select()
	{
		_logger.debug("running test604Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_A_B_"+_randomInt + " (NAME) VALUES ('Sachin Tendulkar')")
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
		String s = "Sachin Tendulkar";
		Flux<Row> flux = Mono.just(con.get())
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_A_B_"+_randomInt+" WHERE NAME = ?")
									   		  .bind(1, s)
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("NAME", String.class).equals(s) : "Expected value is 'Sachin Tendulkar', but received = "+row.get("NAME");
					})
					.expectComplete()
					.verify();
	}
	
	
	/**
	 * Test querying for the data inserted.
	 */
	@Test
	public void test605Select()
	{
		_logger.debug("running test605Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String s = "Sachin Tendulkar";  
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_A_B_"+_randomInt + " (NAME) VALUES (?)")
										   		   .bind(1, s)
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
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_A_B_"+_randomInt+" WHERE NAME = 'Sachin Tendulkar'")
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("NAME", String.class).equals(s) : "Expected value is 'Sachin Tendulkar', but received = "+row.get("NAME");
					})
					.expectComplete()
					.verify();
	}
	
	
	/**
	 * Test querying for the data inserted.
	 */
	@Test
	public void test606Select()
	{
		_logger.debug("running test606Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String s = "Sachin Tendulkar";  
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_A_B_"+_randomInt + " (NAME) VALUES (?)")
										   		   .bind(1, s)
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
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_A_B_"+_randomInt+" WHERE NAME = ?")
									   		  .bind(1, s)
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("NAME", String.class).equals(s) : "Expected value is 'Sachin Tendulkar', but received = "+row.get("NAME");
					})
					.expectComplete()
					.verify();
	}
	
	
	/**
	 * Test querying for the data inserted. Using BIT substring type
	 */
	@Test
	public void test609Select()
	{
		// skip this test for LUW
		if (_isLUW) {
			return;
		}
		
		_logger.debug("running test609Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String s = "Sachin Tendulkar";  
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_A_C_"+_randomInt + " (NAME) VALUES (?)")
										   		   .bind(1, s.getBytes())
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
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_A_C_"+_randomInt+" WHERE NAME = ?")
									   		  .bind(1, s.getBytes())
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	String name = new String(row.get("NAME", byte[].class)).trim();
					 	assert name.equals(s) : "Expected value is 'Sachin Tendulkar', but received = "+name;
					})
					.expectComplete()
					.verify();
	}
}
