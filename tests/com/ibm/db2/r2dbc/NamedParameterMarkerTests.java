package com.ibm.db2.r2dbc;

import java.math.BigDecimal;
import java.sql.Time;
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
public class NamedParameterMarkerTests extends BaseTestCase
{
	private static final Logger _logger =  LoggerFactory.getLogger(NamedParameterMarkerTests.class.getName());

	public NamedParameterMarkerTests()
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
								.flatMap(c -> c.executeUpdate("CREATE TABLE UNIT_TEST_"+_randomInt + " (ID INTEGER, NAME VARCHAR(100), HEIGHT DECFLOAT(34))"));
		
		StepVerifier.create(mono)
		            .expectComplete()
		            .verify();		
	
		con.get().release();
	}	
	
	/**
	 * Test if we could bind parameter values using named markers.
	 */
	@Test
	public void test1()
	{
		_logger.debug("running test1");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		BigDecimal height = new BigDecimal("165.5");
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" VALUES (:id, :name, :height)")
											       .bind("id", 1)
											       .bind("name",  "John")
											       .bind("height", height)
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
							 .flatMap(c -> c.createStatement("SELECT ID, NAME, HEIGHT FROM UNIT_TEST_"+_randomInt+" WHERE ID = :id AND NAME = :name AND HEIGHT = :height")
									 	     .bind("id", 1)
									         .bind("name", "John")
									         .bind("height", height)
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected value is 1, but received = "+row.get("ID");
					 	assert row.get("NAME", String.class).equals("John"): "Expected value is John, but received = "+row.get("NAME");
					 	assert row.get("HEIGHT", BigDecimal.class).equals(height): "Expected value is 165.5, but received = "+row.get("HEIGHT");
					})
					.expectComplete()
					.verify();

	}
	
	/**
	 * Test if we could bind parameter values using named markers for more than one position.
	 */
	@Test
	public void test2()
	{
		_logger.debug("running test2");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		BigDecimal height = new BigDecimal("165.5");
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" VALUES (:id, :name, :height)")
											       .bind("id", 1)
											       .bind("name",  "John")
											       .bind("height",  height)
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
							 .flatMap(c -> c.createStatement("SELECT ID, NAME, HEIGHT FROM UNIT_TEST_"+_randomInt+" WHERE ID = :id AND ID = :id AND NAME = :name AND HEIGHT = :height")
									 	     .bind("id", 1) // this id should be bound for two positions 
									         .bind("name", "John")
									         .bind("height", height)
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected value is 1, but received = "+row.get("ID");
					 	assert row.get("NAME", String.class).equals("John"): "Expected value is John, but received = "+row.get("NAME");
					 	assert row.get("HEIGHT", BigDecimal.class).equals(height): "Expected value is 165.5, but received = "+row.get("HEIGHT");
					})
					.expectComplete()
					.verify();

	}
	
	/**
	 * Test validate if we raise error when named markers and ? are mixed in a SQL.
	 */
	@Test
	public void test3()
	{
		_logger.debug("running test3");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		BigDecimal height = new BigDecimal("165.5");
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" VALUES (:id, ?, :height)")
											       .bind("id", 1)
											       .bind(2,  "John")
											       .bind("height",  height)
												   .execute());
		// Test
		StepVerifier.create(mono)
		            .expectError()
		            .verify();

	}
	
	/**
	 * Test validate if we raise error when unknown parameter names are used by bind.
	 */
	@Test
	public void test4()
	{
		_logger.debug("running test4");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		BigDecimal height = new BigDecimal("165.5");
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" VALUES (:_id, :name, :height)")
											       .bind("_id", 1) 
											       .bind("abcd",  "John") // unknown param name, should cause error
											       .bind("height",  height)
												   .execute());
		// Test
		StepVerifier.create(mono)
		            .expectError()
		            .verify();

	}
	
	/**
	 * Test validate if we raise error when named markers are ill-formed in a SQL.
	 */
	@Test
	public void test5()
	{
		_logger.debug("running test5");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		BigDecimal height = new BigDecimal("165.5");
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" VALUES (:1id, :name&co, :height)")
											       .bind("1id", 1) 
											       .bind("name&co",  "John") // ill-formed name '&' not allowed
											       .bind("height",  height)
												   .execute());
		// Test
		StepVerifier.create(mono)
		            .expectError()
		            .verify();

	}
}