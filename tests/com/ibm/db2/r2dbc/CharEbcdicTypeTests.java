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
public class CharEbcdicTypeTests extends BaseTestCase
{
	private static final Logger _logger =  LoggerFactory.getLogger(CharEbcdicTypeTests.class.getName());

	public CharEbcdicTypeTests()
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
	 * EBCDIC Table
	 *  
	 */
	
	@BeforeClass
	public static void createEBCDICTableA()
	{
		_logger.debug("createTable() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		String createTable;
		if (_isLUW) {
			// CCSID EBCDIC is invalid in LUW
			createTable = "CREATE TABLE UNIT_TEST_E_A_"+_randomInt + " (NAME CHAR(25))";
		}
		else {
			createTable = "CREATE TABLE UNIT_TEST_E_A_"+_randomInt + " (NAME CHAR(25)) CCSID EBCDIC";
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
	public static void createEBCDICTableB()
	{
		_logger.debug("createTable() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		String createTable;
		if (_isLUW) {
			// FOR SBCS DATA and CCSID EBCDIC are invalid in LUW
			createTable = "CREATE TABLE UNIT_TEST_E_B_"+_randomInt + " (NAME CHAR(25))";
		}
		else {
			createTable = "CREATE TABLE UNIT_TEST_E_B_"+_randomInt + " (NAME CHAR(25) FOR SBCS DATA) CCSID EBCDIC";
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
	public static void createEBCDICTableC()
	{
		_logger.debug("createTable() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		String createTable;
		if (_isLUW) {
			// CCSID EBCDIC is invalid in LUW also BIT DATA has limitations
			createTable = "CREATE TABLE UNIT_TEST_E_C_"+_randomInt + " (NAME CHAR(25))";
		}
		else {
			createTable = "CREATE TABLE UNIT_TEST_E_C_"+_randomInt + " (NAME CHAR(25) FOR BIT DATA) CCSID EBCDIC";
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
	public static void createEBCDICTableD()
	{
		_logger.debug("createTable() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		String createTable;
		if (_isLUW) {
			// CCSID 1208 and CCSID EBCDIC are invalid in LUW
			createTable = "CREATE TABLE UNIT_TEST_E_D_"+_randomInt + " (NAME CHAR(25))";
		}
		else {
			createTable = "CREATE TABLE UNIT_TEST_E_D_"+_randomInt + " (NAME CHAR(25) CCSID 1208) CCSID EBCDIC";
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
	public void test501Select()
	{
		_logger.debug("running test501Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_E_A_"+_randomInt + " (NAME) VALUES ('Sachin Tendulkar')")
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
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_E_A_"+_randomInt+" WHERE NAME = ?")
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
	public void test502Select()
	{
		_logger.debug("running test502Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String s = "Sachin Tendulkar";  
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_E_A_"+_randomInt + " (NAME) VALUES (?)")
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
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_E_A_"+_randomInt+" WHERE NAME = 'Sachin Tendulkar'")
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
	 * Test querying for the data inserted using bind values.
	 */
	@Test
	public void test503Select()
	{
		_logger.debug("running test503Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String s = "Sachin Tendulkar";  
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_E_A_"+_randomInt + " (NAME) VALUES (?)")
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
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_E_A_"+_randomInt+" WHERE NAME = ?")
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
	public void test504Select()
	{
		_logger.debug("running test504Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String s = "Sachin Tendulkar";  
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_E_B_"+_randomInt + " (NAME) VALUES ('Sachin Tendulkar')")
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
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_E_B_"+_randomInt+" WHERE NAME = ?")
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
	 * Test inserting a row.
	 */
	@Test
	public void test505Select()
	{
		_logger.debug("running test505Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String s = "Sachin Tendulkar";  
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.doOnError(e -> System.exit(-1))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_E_B_"+_randomInt + " (NAME) VALUES (?)")
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
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_E_B_"+_randomInt+" WHERE NAME = 'Sachin Tendulkar'")
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
	public void test506Select()
	{
		_logger.debug("running test506Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String s = "Sachin Tendulkar";  
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_E_B_"+_randomInt + " (NAME) VALUES (?)")
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
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_E_B_"+_randomInt+" WHERE NAME = ?")
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
	public void test509Select()
	{
		// if LUW skip this test
		if (_isLUW) {
			return;
		}
		
		_logger.debug("running test509Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String s = "Sachin Tendulkar";  
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_E_C_"+_randomInt + " (NAME) VALUES (?)")
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
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_E_C_"+_randomInt+" WHERE NAME = ?")
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
	
	
	/**
	 * Test querying for the existing data.
	 */
	@Test
	public void test510Select()
	{
		_logger.debug("running test510Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String s = "Sachin Tendulkar";  
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_E_D_"+_randomInt + " (NAME) VALUES ('Sachin Tendulkar')")
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
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_E_D_"+_randomInt+" WHERE NAME = ?")
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
	 * Test inserting a row.
	 */
	@Test
	public void test511Select()
	{
		_logger.debug("running test511Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String s = "Sachin Tendulkar";  
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.doOnError(e -> System.exit(-1))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_E_D_"+_randomInt + " (NAME) VALUES (?)")
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
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_E_D_"+_randomInt+" WHERE NAME = 'Sachin Tendulkar'")
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
	public void test512Select()
	{
		_logger.debug("running test512Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String s = "Sachin Tendulkar";  
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_E_D_"+_randomInt + " (NAME) VALUES (?)")
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
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_E_D_"+_randomInt+" WHERE NAME = ?")
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
}
