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
public class CharUnicodeTypeTests extends BaseTestCase
{
	private static final Logger _logger =  LoggerFactory.getLogger(CharUnicodeTypeTests.class.getName());

	public CharUnicodeTypeTests()
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
	 *  UNICODE TABLE
	 * 
	 *  FOR < SBCS | MIXED | BIT > DATA 
	 *  
	 */
	
	@BeforeClass
	public static void createUNICODETableA()
	{
		_logger.debug("createTable() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		Mono<Void> mono = getNewConnection()
								.doOnNext(c -> con.set(c))
								.flatMap(c -> c.executeUpdate("CREATE TABLE UNIT_TEST_U_A_"+_randomInt + " (NAME CHAR(100)) CCSID UNICODE"));
		
		StepVerifier.create(mono)
		            .expectComplete()
		            .verify();	
		
		con.get().close().block();
	}
	
	@BeforeClass
	public static void createUNICODETableB()
	{
		_logger.debug("createTable() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		String createTable;
		if (_isLUW) {
			// FOR SBCS DATA is invalid in LUW
			createTable = "CREATE TABLE UNIT_TEST_U_B_"+_randomInt + " (NAME CHAR(100)) CCSID UNICODE";
		}
		else {
			createTable = "CREATE TABLE UNIT_TEST_U_B_"+_randomInt + " (NAME CHAR(100) FOR SBCS DATA) CCSID UNICODE";
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
	public static void createUNICODETableC()
	{
		_logger.debug("createTable() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		Mono<Void> mono = getNewConnection()
								.doOnNext(c -> con.set(c))
								.flatMap(c -> c.executeUpdate("CREATE TABLE UNIT_TEST_U_C_"+_randomInt + " (NAME CHAR(100) FOR MIXED DATA) CCSID UNICODE"));
		
		StepVerifier.create(mono)
		            .expectComplete()
		            .verify();	
		
		con.get().close().block();
	}
	
	@BeforeClass
	public static void createUNICODETableD()
	{
		_logger.debug("createTable() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		String createTable;
		if (_isLUW) {
			// In LUW, BIT DATA has limitations
			createTable = "CREATE TABLE UNIT_TEST_U_D_"+_randomInt + " (NAME CHAR(100)) CCSID UNICODE";
		}
		else {
			createTable = "CREATE TABLE UNIT_TEST_U_D_"+_randomInt + " (NAME CHAR(100) FOR BIT DATA) CCSID UNICODE";
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
	 * Test querying for the existing data with utf8 characters.
	 */
	@Test
	public void test701Select()
	{
		_logger.debug("running test701Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_U_A_"+_randomInt + " (NAME) VALUES ('₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ')")
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
		String s = "₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ";
		Flux<Row> flux = Mono.just(con.get())
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_U_A_"+_randomInt+" WHERE NAME = ?")
									   		  .bind(1, s)
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("NAME", String.class).equals(s) : "Expected value is '₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ', but received = "+row.get("NAME");
					})
					.expectComplete()
					.verify();
	}
	
	
	/**
	 * Test querying for the data inserted.
	 */
	@Test
	public void test702Select()
	{
		_logger.debug("running test702Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String s = "₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ";  
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_U_A_"+_randomInt + " (NAME) VALUES (?)")
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
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_U_A_"+_randomInt+" WHERE NAME = '₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ'")
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("NAME", String.class).equals(s) : "Expected value is '₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ', but received = "+row.get("NAME");
					})
					.expectComplete()
					.verify();
	}
	
	
	/**
	 * Test querying for the data inserted.
	 */
	@Test
	public void test703Select()
	{
		_logger.debug("running test703Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String s = "₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ";  
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_U_A_"+_randomInt + " (NAME) VALUES (?)")
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
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_U_A_"+_randomInt+" WHERE NAME = ?")
									   		  .bind(1, s)
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("NAME", String.class).equals(s) : "Expected value is '₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ', but received = "+row.get("NAME");
					})
					.expectComplete()
					.verify();
	}
	
	/**
	 * Test querying for the existing data. SBCS Unicode data.
	 */
	@Test
	public void test704Select()
	{
		_logger.debug("running test704Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_U_B_"+_randomInt + " (NAME) VALUES ('Sachin Tendulkar')")
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
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_U_B_"+_randomInt+" WHERE NAME = ?")
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
	public void test705Select()
	{
		_logger.debug("running test705Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String s = "Sachin Tendulkar";  
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_U_B_"+_randomInt + " (NAME) VALUES (?)")
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
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_U_B_"+_randomInt+" WHERE NAME = 'Sachin Tendulkar'")
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
	 * Test querying for the data inserted. SBCS unicode data.
	 */
	@Test
	public void test706Select()
	{
		_logger.debug("running test706Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String s = "Sachin Tendulkar";  
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_U_B_"+_randomInt + " (NAME) VALUES (?)")
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
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_U_B_"+_randomInt+" WHERE NAME = ?")
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
	public void test707Select()
	{
		_logger.debug("running test707Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_U_C_"+_randomInt + " (NAME) VALUES ('₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ')")
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
		String s = "₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ";
		Flux<Row> flux = Mono.just(con.get())
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_U_C_"+_randomInt+" WHERE NAME = ?")
									   		  .bind(1, s)
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("NAME", String.class).equals(s) : "Expected value is '₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ', but received = "+row.get("NAME");
					})
					.expectComplete()
					.verify();
	}
	
	
	/**
	 * Test querying for the data inserted.
	 */
	@Test
	public void test708Select()
	{
		_logger.debug("running test708Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String s = "₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ";  
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_U_C_"+_randomInt + " (NAME) VALUES (?)")
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
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_U_C_"+_randomInt+" WHERE NAME = '₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ'")
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("NAME", String.class).equals(s) : "Expected value is '₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ', but received = "+row.get("NAME");
					})
					.expectComplete()
					.verify();
	}
	
	/**
	 * Test querying for the data inserted.
	 */
	@Test
	public void test709Select()
	{
		_logger.debug("running test709Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String s = "₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ";  
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_U_C_"+_randomInt + " (NAME) VALUES (?)")
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
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_U_C_"+_randomInt+" WHERE NAME = ?")
									   		  .bind(1, s)
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("NAME", String.class).equals(s) : "Expected value is '₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ', but received = "+row.get("NAME");
					})
					.expectComplete()
					.verify();
	}
	
	/**
	 * Test querying for the existing data.
	 */
	//@Test
	public void test710Select()
	{
		_logger.debug("running test710Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_U_D_"+_randomInt + " (NAME) VALUES ('₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ')")
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
		String s = "₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ";
		Flux<Row> flux = Mono.just(con.get())
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_U_D_"+_randomInt+" WHERE NAME = ?")
									   		  .bind(1, s)
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("NAME", String.class).equals(s) : "Expected value is '₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ', but received = "+row.get("NAME");
					})
					.expectComplete()
					.verify();
	}
	
	
	/**
	 * Test querying for the data inserted.
	 */
	//@Test
	public void test711Select()
	{
		_logger.debug("running test711Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String s = "₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ";  
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_U_D_"+_randomInt + " (NAME) VALUES (?)")
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
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_U_D_"+_randomInt+" WHERE NAME = '₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ'")
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("NAME", String.class).equals(s) : "Expected value is '₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ', but received = "+row.get("NAME");
					})
					.expectComplete()
					.verify();
	}
	
	/**
	 * Test querying for the data inserted. In LUW, BIT DATA has limitations, hence this test is skipped for LUW.
	 */
	@Test
	public void test712Select()
	{
		// skip for LUW
		if (_isLUW) {
			return;
		}
		
		_logger.debug("running test712Select");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String s = "₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ";  
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_U_D_"+_randomInt + " (NAME) VALUES (?)")
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
							   .flatMap(c -> c.createStatement("SELECT NAME FROM UNIT_TEST_U_D_"+_randomInt+" WHERE NAME = ?")
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
					 	assert name.equals(s) : "Expected value is '₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ', but received = "+name;
					})
					.expectComplete()
					.verify();
	}
}
