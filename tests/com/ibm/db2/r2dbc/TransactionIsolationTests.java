package com.ibm.db2.r2dbc;

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

import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Row;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TransactionIsolationTests extends BaseTestCase
{
	private static final Logger _logger =  LoggerFactory.getLogger(TransactionIsolationTests.class.getName());
	
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
	
	/* Transaction Tests - Commit and Rollback */
	
	/**
	 * Test inserting a row (execute immdiate) and commit
	 */
	@Test
	public void test001Commit()
	{	
		_logger.debug("running test001Commit");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// get connection, insert data
		_logger.debug("inserting data - using execute direct");
		Mono<Void> mono = _pool.getConnection()
							   .doOnNext(c -> con.set(c))
							   .flatMap(c -> c.executeUpdate("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (100)"));

		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono)
			        .expectComplete()	    
			        .verify();	
		
		// commit the transaction
		_logger.debug("commit trasaction");
		Mono<Void> mono2 = Mono.just(con.get())
							   .flatMap(c -> c.commitTransaction());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono2)
			        .expectComplete()	    
			        .verify();	
		
		// release connection
		con.get().release();
		
		// Get connection, query the inserted data and then release the connection
		_logger.debug("select the inserted data");
		Flux<Row> flux = Mono.just(con.get())
							   .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 100")									   		  
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test and validate
		_logger.debug("validate..");
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 100 : "Expected value is 100, but received = "+row.get("ID");
					})
					.expectComplete()
					.verify();
	}
	
	/**
	 * Test inserting a row (prepared statement) and commit
	 */
	@Test
	public void test002Commit()
	{	
		_logger.debug("running test002Commit");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// get connection, insert data
		_logger.debug("inserting data - using prepared statement");
		Mono<DB2Result> mono = _pool.getConnection()
								    .doOnNext(c -> con.set(c))
								    .flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (200)")
								  				   .execute());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows inserted = 1, but received = "+rs.getNumRowsUpdated();
			        })
		            .expectComplete()
		            .verify();		
		
		// commit the transaction
		_logger.debug("commit trasaction");
		Mono<Void> mono2 = Mono.just(con.get())
							   .flatMap(c -> c.commitTransaction());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono2)
			        .expectComplete()	    
			        .verify();	
		
		// release connection
		_logger.debug("release connection");
		con.get().release();
		
		// Get connection, query the inserted data and then release the connection
		_logger.debug("select the inserted data");
		Flux<Row> flux = _pool.getConnection()
							  .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 200")									   		  
									  		 .execute())
							  .flatMapMany(result -> result.map((row, md) -> row))
							  .doAfterTerminate(() -> con.get().release());

		// Test and validate
		_logger.debug("validate..");
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 200 : "Expected value is 200, but received = "+row.get("ID");
					})
					.expectComplete()
					.verify();
	}
	
	/**
	 * Test inserting a row (execute immediate) and rollback
	 */
	@Test
	public void test021Rollback()
	{	
		_logger.debug("running test021Rollback");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// get connection, insert data
		_logger.debug("inserting data - using execute direct");
		Mono<Void> mono1 = _pool.getConnection()
						 	    .doOnNext(c -> {
						 	    	con.set(c); c.beginTransaction();
						 	    })
							    .flatMap(c -> c.executeUpdate("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (300)"));

		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono1)
			        .expectComplete()	    
			        .verify();	
		
		// rollback transaction
		_logger.debug("rollback trasaction");
		Mono<Void> mono2 = Mono.just(con.get())
							   .flatMap(c -> con.get().rollbackTransaction());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono2)
			        .expectComplete()	    
			        .verify();
		
		// release connection
		_logger.debug("close connection");
		con.get().release();
		
		// Get connection, query the inserted data and then release the connection
		_logger.debug("select the inserted data");
		Flux<Row> flux =  _pool.getConnection()
							   .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 300")									   		  
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test and validate
		_logger.debug("validate..");
		StepVerifier.create(flux)
					.expectComplete()  // we should have zero rows as result
					.verify();
	}
	
	
	/**
	 * Test inserting a row (using prepared statement) and rollback
	 */
	@Test
	public void test022Rollback()
	{	
		_logger.debug("running test002Rollback");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// get connection, insert data
		_logger.debug("inserting data - using prepared statement");
		Mono<DB2Result> mono = _pool.getConnection()
								    .doOnNext(c -> con.set(c))
								    .flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (200)")
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
		
		// rollback the transaction
		_logger.debug("rollback trasaction");
		Mono<Void> mono2 = Mono.just(con.get())
							   .flatMap(c -> c.rollbackTransaction());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono2)
			        .expectComplete()	    
			        .verify();	
		
		// release connection
		_logger.debug("release connection");
		con.get().release();
		
		// Get connection, query the inserted data and then release the connection
		_logger.debug("select the inserted data");
		Flux<Row> flux =  _pool.getConnection()
							   .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 300")									   		  
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row))
							   .doAfterTerminate(() -> con.get().release());

		// Test and validate
		_logger.debug("validate..");
		StepVerifier.create(flux)
					.expectComplete()  // we should have zero rows as result
					.verify();
	}
	
	/**
	 * Test Read Uncommited Isolation Level. Test we could read uncommitted row.
	 */
	@Test
	public void test101ReadUncommitted()
	{	
		_logger.debug("running test101ReadUncommitted");
		
		AtomicReference<DB2Connection> con1 = new AtomicReference<DB2Connection>();
		AtomicReference<DB2Connection> con2 = new AtomicReference<DB2Connection>();
		
		// get connection, set isolation level and insert data
		_logger.debug("inserting data - using execute direct");
		Mono<Void> mono = getNewConnection()
							   .doOnNext(c -> {con1.set(c); c.setTransactionIsolationLevel(IsolationLevel.READ_UNCOMMITTED); c.beginTransaction();})
							   .flatMap(c -> c.executeUpdate("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (1000)"));

		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono)
			        .expectComplete()	    
			        .verify();	
		
		// Data is not commited yet, try reading it from a different connection
		
		// Get a new connection, query set isolation and then read the inserted data
		_logger.debug("select the inserted data");
		Flux<Row> flux = getNewConnection()
							   .doOnNext(c -> {con2.set(c); c.setTransactionIsolationLevel(IsolationLevel.READ_UNCOMMITTED); c.beginTransaction();})
							   .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1000")									   		  
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row));

		// Test and validate
		_logger.debug("validate..");
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1000 : "Expected value is 1000, but received = "+row.get("ID");
					})
					.expectComplete()
					.verify();
		
		// rollback con1 transaction
		_logger.debug("rollback trasaction");
		Mono<Void> mono2 = Mono.just(con1.get())
							   .flatMap(c -> c.rollbackTransaction());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono2)
			        .expectComplete()	    
			        .verify();
		
		// close con1
		con1.get().close().block();
		
		// rollback con2 transaction
		_logger.debug("rollback trasaction");
		Mono<Void> mono3 = Mono.just(con2.get())
							   .flatMap(c -> c.rollbackTransaction());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono3)
			        .expectComplete()	    
			        .verify();
				
		// close con2
		con2.get().close().block();
	}
	
	/**
	 * Test inserting a row (prepared statement) and commit. Show we could read commited data.
	 */
	@Test
	public void test102ReadUncommitted()
	{	
		_logger.debug("running test102ReadUncommitted");
		
		AtomicReference<DB2Connection> con1 = new AtomicReference<DB2Connection>();
		AtomicReference<DB2Connection> con2 = new AtomicReference<DB2Connection>();
		
		// get connection, insert data
		_logger.debug("inserting data - using prepared statement");
		Mono<DB2Result> mono = _pool.getConnection()
								    .doOnNext(c -> {
								    	con1.set(c); c.beginTransaction();
								    	c.setTransactionIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
								    })
								    .flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (200)")
								  				   .execute());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows inserted = 1, but received = "+rs.getNumRowsUpdated();
			        })
		            .expectComplete()
		            .verify();		
		
		// Data not committed yet
		
		// Get a new connection, query the inserted data and then release the connection
		_logger.debug("select the inserted data");
		Flux<Row> flux = getNewConnection()
							  .doOnNext(c -> {
								  con2.set(c); 
								  c.setTransactionIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
							  })
							  .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 200")									   		  
									  		 .execute())
							  .flatMapMany(result -> result.map((row, md) -> row));

		// Test and validate
		_logger.debug("validate..");
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 200 : "Expected value is 200, but received = "+row.get("ID");
					})
					.expectComplete()
					.verify();
		
		// commit the transaction
		_logger.debug("commit trasaction");
		Mono<Void> mono2 = Mono.just(con1.get())
							   .flatMap(c -> c.commitTransaction());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono2)
			        .expectComplete()	    
			        .verify();	
		
		// release connection
		_logger.debug("release connection");
		con1.get().release();
		
		// close con2
		con2.get().close();
	}
	
	/**
	 * Test Read Committed Isolation Level. Make sure we don't have uncommited read in this isolation level.
	 */
	@Test
	public void test201ReadCommitted()
	{	
		_logger.debug("running test201ReadCommitted");
		
		AtomicReference<DB2Connection> con1 = new AtomicReference<DB2Connection>();
		AtomicReference<DB2Connection> con2 = new AtomicReference<DB2Connection>();
		
		// get connection, set isolation level and insert data
		_logger.debug("inserting data - using execute direct");
		Mono<Void> mono = getNewConnection()
							   .doOnNext(c -> {con1.set(c); c.setTransactionIsolationLevel(IsolationLevel.READ_COMMITTED); c.beginTransaction();})
							   .flatMap(c -> c.executeUpdate("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (1000)"));

		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono)
			        .expectComplete()	    
			        .verify();	
		
		// Data is not commited yet, try reading it from a different connection
		
		// Get a new connection, set isolation level and query the inserted data
		// This should block waiting for read lock, then timeout and fail
		_logger.debug("select the inserted data");
		Flux<Row> flux = getNewConnection()
							   .doOnNext(c -> {con2.set(c); c.setTransactionIsolationLevel(IsolationLevel.READ_COMMITTED); c.beginTransaction();})
							   .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1000")									   		  
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row));

		// Test and validate
		_logger.debug("validate..");
		if (_isLUW) {
			// In LUW, this just returns with no rows as this row is not yet committed
			StepVerifier.create(flux)
						.expectComplete()
						.verify();
		}
		else {
			// zOS behavior
			StepVerifier.create(flux)
						.expectError()
						.verify();
		}
		
		// rollback con1 transaction
		_logger.debug("rollback trasaction");
		Mono<Void> mono2 = Mono.just(con1.get())
							   .flatMap(c -> c.rollbackTransaction());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono2)
			        .expectComplete()	    
			        .verify();
		
		// close con1
		con1.get().close().block();
		
		// rollback con2 transaction
		_logger.debug("rollback trasaction");
		Mono<Void> mono3 = Mono.just(con2.get())
							   .flatMap(c -> c.rollbackTransaction());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono3)
			        .expectComplete()	    
			        .verify();
				
		// close con2
		con2.get().close().block();
	}

	
	/**
	 * Test Read Committed Transactions. Make sure we could read committed row of data.
	 */
	@Test
	public void test202ReadCommitted()
	{	
		_logger.debug("running test202ReadCommitted");
		
		AtomicReference<DB2Connection> con1 = new AtomicReference<DB2Connection>();
		AtomicReference<DB2Connection> con2 = new AtomicReference<DB2Connection>();
		
		// get connection, insert data
		_logger.debug("inserting data - using prepared statement");
		Mono<DB2Result> mono = _pool.getConnection()
								    .doOnNext(c -> {
								    	con1.set(c);
								    	c.setTransactionIsolationLevel(IsolationLevel.READ_COMMITTED);
								    })
								    .flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (200)")
								  				   .execute());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows inserted = 1, but received = "+rs.getNumRowsUpdated();
			        })
		            .expectComplete()
		            .verify();		
		
		// commit the transaction
		_logger.debug("commit trasaction");
		Mono<Void> mono2 = Mono.just(con1.get())
							   .flatMap(c -> c.commitTransaction());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono2)
			        .expectComplete()	    
			        .verify();	
		
		// release connection
		_logger.debug("release connection");
		con1.get().release();
		
		// Get connection, query the inserted data and then release the connection
		_logger.debug("select the inserted data");
		Flux<Row> flux = getNewConnection()
							  .doOnNext(c -> {
								  con2.set(c);
								  c.setTransactionIsolationLevel(IsolationLevel.READ_COMMITTED);
							  })
							  .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 200")									   		  
									  		 .execute())
							  .flatMapMany(result -> result.map((row, md) -> row));

		// Test and validate
		_logger.debug("validate..");
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 200 : "Expected value is 200, but received = "+row.get("ID");
					})
					.expectComplete()
					.verify();
		
		// close connection
		con2.get().close().block();
	}
	
	/**
	 * Test Read Committed Transactions. Test for Non-Repeatable Read.
	 */
	@Test
	public void test203ReadCommitted()
	{	
		_logger.debug("running test203ReadCommitted");
		
		AtomicReference<DB2Connection> con1 = new AtomicReference<DB2Connection>();
		AtomicReference<DB2Connection> con2 = new AtomicReference<DB2Connection>();
		
		// get connection, insert data
		_logger.debug("inserting data - using prepared statement");
		Mono<DB2Result> mono = _pool.getConnection()
								    .doOnNext(c -> {
								    	con1.set(c);
								    	c.setTransactionIsolationLevel(IsolationLevel.READ_COMMITTED);
								    })
								    .flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (200)")
								  				   .execute())
								    .doAfterTerminate(() -> con1.get().release());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows inserted = 1, but received = "+rs.getNumRowsUpdated();
			        })
		            .expectComplete()
		            .verify();		
		
		// Get connection, query the inserted data
		_logger.debug("select the inserted data");
		Flux<Row> flux = getNewConnection()
							  .doOnNext(c -> {
								  con2.set(c);
								  c.setTransactionIsolationLevel(IsolationLevel.READ_COMMITTED);
							  })
							  .delayUntil(c -> c.beginTransaction()) // get a transaction started
							  .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 200")									   		  
									  		 .execute())
							  .flatMapMany(result -> result.map((row, md) -> row));

		// Test and validate
		_logger.debug("validate..");
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 200 : "Expected value is 200, but received = "+row.get("ID");
					})
					.expectComplete()
					.verify();
		
		// Now update that row
		_logger.debug("updating the inserted data - using prepared statement");
		Mono<DB2Result> mono3 = _pool.getConnection()
								     .doOnNext(c -> {
								    	 con1.set(c);
								    	 c.setTransactionIsolationLevel(IsolationLevel.READ_COMMITTED);
								     })
								     .flatMap(c -> c.createStatement("UPDATE UNIT_TEST_"+_randomInt + " SET ID = 250 WHERE ID = 200")
								  				   .execute())
								     .doAfterTerminate(() -> con1.get().release());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono3)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows updated = 1, but received = "+rs.getNumRowsUpdated();
			        })
		            .expectComplete()
		            .verify();		
		
		// Try querying for the same data queried earlier
		_logger.debug("select the inserted data");
		Flux<Row> flux2 = Flux.just(con2.get())
							  .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 200")									   		  
									  		 .execute())
							  .flatMap(result -> result.map((row, md) -> row));

		// Test and validate
		_logger.debug("validate..");
		StepVerifier.create(flux2)      // this row will not be present, the same read is not repeatable
					.expectComplete()
					.verify();
		
		// close connection
		con2.get().close().block();
	}
	
	/**
	 * Test for Repeatable Read Transaction Isolation. Show we could repeat a read successfully.
	 */
	@Test
	public void test301RepeatableRead()
	{	
		_logger.debug("running test301RepeatableRead");
		
		AtomicReference<DB2Connection> con1 = new AtomicReference<DB2Connection>();
		AtomicReference<DB2Connection> con2 = new AtomicReference<DB2Connection>();
		
		// get connection, insert data
		_logger.debug("inserting data - using prepared statement");
		Mono<DB2Result> mono = _pool.getConnection()
								    .doOnNext(c -> {
								    	con1.set(c);
								    	c.setTransactionIsolationLevel(IsolationLevel.REPEATABLE_READ);
								    })
								    .flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (200)")
								  				   .execute())
								    .doAfterTerminate(() -> con1.get().release());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows inserted = 1, but received = "+rs.getNumRowsUpdated();
			        })
		            .expectComplete()
		            .verify();		
		
		// Get connection, query the inserted data
		_logger.debug("select the inserted data");
		AtomicReference<DB2Result> res = new AtomicReference<DB2Result>();
		Flux<Row> flux = getNewConnection()
							  .doOnNext(c -> {
								  con2.set(c);
								  c.setTransactionIsolationLevel(IsolationLevel.REPEATABLE_READ);
							  })
							  .delayUntil(c -> c.beginTransaction())  // get a transaction started and keep it open
							  .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 200")									   		  
									  		 .execute())
							  .doOnNext(result -> res.set(result))
							  .flatMapMany(result -> result.map((row, md) -> row));

		// Test and validate
		_logger.debug("validate..");
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 200 : "Expected value is 200, but received = "+row.get("ID");
					})
					.expectComplete()
					.verify();
		
		// close resultset
		Mono<Void> mres = Mono.just(res.get())
							  .flatMap(rs -> rs.close());
		
		// Test
		StepVerifier.create(mres)
			        .expectComplete()	    
			        .verify();	
		
		// Now try to update that row
		// this update should wait for the update lock, then timeout and fail
		_logger.debug("updating the inserted data - using prepared statement");
		Mono<DB2Result> mono3 = _pool.getConnection()
								     .doOnNext(c -> {
								    	 con1.set(c);
								    	 c.setTransactionIsolationLevel(IsolationLevel.REPEATABLE_READ);
								     })
								     .delayUntil(c -> {
								    	 if (_isLUW) {
								    		 return c.executeUpdate("SET LOCK TIMEOUT 10");
								    	 }
								    	 else {
								    		 // Not applicable for zOS
								    		 return Mono.empty();
								    	 }
								     })
								     .flatMap(c -> c.createStatement("UPDATE UNIT_TEST_"+_randomInt + " SET ID = 250 WHERE ID = 200")
								  				   .execute())
								     .doAfterTerminate(() -> con1.get().release());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono3)
		            .expectError() 
		            .verify();		
		
		
		// Try querying for the same data queried earlier, we should be able to repeat the read in this transaction
		_logger.debug("select the inserted data");
		Flux<Row> flux2 = Flux.just(con2.get())
							  .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 200")									   		  
									  		 .execute())
							  .flatMap(result -> result.map((row, md) -> row));

		// Test and validate
		_logger.debug("validate..");
		StepVerifier.create(flux2)      // this row should be present
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 200 : "Expected value is 200, but received = "+row.get("ID");
					})
					.expectComplete()
					.verify();
		
		// commit the transaction
		_logger.debug("commit trasaction");
		Mono<Void> mono5 = Mono.just(con2.get())
							   .flatMap(c -> c.commitTransaction());
				
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono5)
			        .expectComplete()	    
			        .verify();	
		
		// close connection
		con2.get().close().block();
	}
	
	/**
	 * Test for Repeatable Read Transaction Isolation. Insert a new row. Phantom read is possible.
	 */
	@Test
	public void test302RepeatableRead()
	{	
		_logger.debug("running test302RepeatableRead");
		
		AtomicReference<DB2Connection> con1 = new AtomicReference<DB2Connection>();
		AtomicReference<DB2Connection> con2 = new AtomicReference<DB2Connection>();
		
		// get connection, insert data
		_logger.debug("inserting data - using prepared statement");
		Mono<DB2Result> mono = _pool.getConnection()
								    .doOnNext(c -> {
								    	con1.set(c);
								    	c.setTransactionIsolationLevel(IsolationLevel.REPEATABLE_READ);
								    })
								    .flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (200)")
								  				   .execute())
								    .doAfterTerminate(() -> con1.get().release());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows inserted = 1, but received = "+rs.getNumRowsUpdated();
			        })
		            .expectComplete()
		            .verify();		
		
		// Get connection, query the inserted data
		_logger.debug("select the inserted data");
		AtomicReference<DB2Result> res = new AtomicReference<DB2Result>();
		Flux<Row> flux = getNewConnection()
							  .doOnNext(c -> {
								  con2.set(c);
								  c.setTransactionIsolationLevel(IsolationLevel.REPEATABLE_READ);
							  })
							  .delayUntil(c -> c.beginTransaction())  // get a transaction started and keep it open
							  .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID > 100")									   		  
									  		 .execute())
							  .doOnNext(result -> res.set(result))
							  .flatMapMany(result -> result.map((row, md) -> row));

		// Test and validate
		_logger.debug("validate..");
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 200 : "Expected value is 200, but received = "+row.get("ID");
					})
					.expectComplete()
					.verify();
		
		// close resultset
		Mono<Void> mres = Mono.just(res.get())
							  .flatMap(rs -> rs.close());
		
		// Test
		StepVerifier.create(mres)
			        .expectComplete()	    
			        .verify();	
		
		// Now try inserting a new row, should go through fine!
		_logger.debug("updating the inserted data - using prepared statement");
		Mono<DB2Result> mono3 = _pool.getConnection()
								     .doOnNext(c -> {
								    	 con1.set(c);
								    	 c.setTransactionIsolationLevel(IsolationLevel.REPEATABLE_READ);
								     })
								     .flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (500)")
								  				   .execute())
								     .doAfterTerminate(() -> con1.get().release());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono3)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows inserted = 1, but received = "+rs.getNumRowsUpdated();
			        })
					.expectComplete()
		            .verify();		
		
		
		// Try querying for the same data queried earlier, notice phantom read is possible
		_logger.debug("select the inserted data");
		Flux<Row> flux2 = Flux.just(con2.get())
							  .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID > 100")									   		  
									  		 .execute())
							  .flatMap(result -> result.map((row, md) -> row));

		// Test and validate
		_logger.debug("validate..");
		StepVerifier.create(flux2)      // this row should be present
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 200 : "Expected value is 200, but received = "+row.get("ID");
					})
					.assertNext(row -> {  // phantom row
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 500 : "Expected value is 500, but received = "+row.get("ID");
					})
					.expectComplete()
					.verify();
		
		// commit the transaction
		_logger.debug("commit trasaction");
		Mono<Void> mono5 = Mono.just(con2.get())
							   .flatMap(c -> c.commitTransaction());
				
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono5)
			        .expectComplete()	    
			        .verify();	
		
		// close connection
		con2.get().close().block();
	}
	
	/**
	 * Test Read Repeatable Read Isolation Level. Make sure we don't have uncommited read in this isolation level.
	 */
	@Test
	public void test303RepeatableRead()
	{	
		_logger.debug("running test303RepeatableRead");
		
		AtomicReference<DB2Connection> con1 = new AtomicReference<DB2Connection>();
		AtomicReference<DB2Connection> con2 = new AtomicReference<DB2Connection>();
		
		// get connection, set isolation level and insert data
		_logger.debug("inserting data - using execute direct");
		Mono<Void> mono = getNewConnection()
							   .doOnNext(c -> {
								   con1.set(c); c.setTransactionIsolationLevel(IsolationLevel.REPEATABLE_READ); 
								   c.beginTransaction();
								})
							   .flatMap(c -> c.executeUpdate("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (1000)"));

		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono)
			        .expectComplete()	    
			        .verify();	
		
		// Data is not commited yet, try reading it from a different connection
		
		// Get a new connection, set isolation level and query the inserted data
		// This should block waiting for read lock, then timeout and fail
		_logger.debug("select the inserted data");
		Flux<Row> flux = getNewConnection()
							   .doOnNext(c -> {
								   con2.set(c); c.setTransactionIsolationLevel(IsolationLevel.REPEATABLE_READ); 
								   c.beginTransaction();
								})
							   .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1000")									   		  
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row));

		// Test and validate
		_logger.debug("validate..");
		if (_isLUW) {
			// LUW behavior
			StepVerifier.create(flux)
						.expectComplete()
						.verify();
		}
		else {
			// zOS behavior
			StepVerifier.create(flux)
						.expectError()
						.verify();
		}
		
		// rollback con1 transaction
		_logger.debug("rollback trasaction");
		Mono<Void> mono2 = Mono.just(con1.get())
							   .flatMap(c -> c.rollbackTransaction());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono2)
			        .expectComplete()	    
			        .verify();
		
		// close con1
		con1.get().close().block();
		
		// rollback con2 transaction
		_logger.debug("rollback trasaction");
		Mono<Void> mono3 = Mono.just(con2.get())
							   .flatMap(c -> c.rollbackTransaction());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono3)
			        .expectComplete()	    
			        .verify();
				
		// close con2
		con2.get().close().block();
	}
	
	/**
	 * Test for Serializable Transaction Isolation. Insert a new row. No Phantom read is possible.
	 */
	@Test
	public void test401Serializable()
	{	
		_logger.debug("running test401Serializable");
		
		AtomicReference<DB2Connection> con1 = new AtomicReference<DB2Connection>();
		AtomicReference<DB2Connection> con2 = new AtomicReference<DB2Connection>();
		
		// get connection, insert data
		_logger.debug("inserting data - using prepared statement");
		Mono<DB2Result> mono = _pool.getConnection()
								    .doOnNext(c -> {
								    	con1.set(c);
								    	c.setTransactionIsolationLevel(IsolationLevel.SERIALIZABLE);
								    })
								    .flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (200)")
								  				   .execute())
								    .doAfterTerminate(() -> con1.get().release());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows inserted = 1, but received = "+rs.getNumRowsUpdated();
			        })
		            .expectComplete()
		            .verify();		
		
		// Get connection, query the inserted data
		_logger.debug("select the inserted data");
		AtomicReference<DB2Result> res = new AtomicReference<DB2Result>();
		Flux<Row> flux = getNewConnection()
							  .doOnNext(c -> {
								  con2.set(c);
								  c.setTransactionIsolationLevel(IsolationLevel.SERIALIZABLE);
							  })
							  .delayUntil(c -> c.beginTransaction())  // get a transaction started and keep it open
							  .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID > 100")									   		  
									  		 .execute())
							  .doOnNext(result -> res.set(result))
							  .flatMapMany(result -> result.map((row, md) -> row));

		// Test and validate
		_logger.debug("validate..");
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 200 : "Expected value is 200, but received = "+row.get("ID");
					})
					.expectComplete()
					.verify();
		
		// close resultset
		Mono<Void> mres = Mono.just(res.get())
							  .flatMap(rs -> rs.close());
		
		// Test
		StepVerifier.create(mres)
			        .expectComplete()	    
			        .verify();	
		
		// Now try inserting a new row
		// This attempt to insert a phantom row should block asking for update lock, then timeout and fail 
		_logger.debug("updating the inserted data - using prepared statement");
		Mono<DB2Result> mono3 = _pool.getConnection()
								     .doOnNext(c -> {
								    	 con1.set(c);
								    	 c.setTransactionIsolationLevel(IsolationLevel.SERIALIZABLE);
								     })
								     .delayUntil(c -> {
								    	 if (_isLUW) {
								    		 return c.executeUpdate("SET LOCK TIMEOUT 10");
								    	 }
								    	 else {
								    		 // Not applicable for zOS
								    		 return Mono.empty();
								    	 }
								     })
								     .flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (500)")
								  				   .execute())
								     .doAfterTerminate(() -> con1.get().release());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono3)
					.expectError()
		            .verify();		
		
		
		// Try querying for the same data queried earlier, we should not have any phantom read
		_logger.debug("select the inserted data");
		Flux<Row> flux2 = Flux.just(con2.get())
							  .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID > 100")									   		  
									  		 .execute())
							  .flatMap(result -> result.map((row, md) -> row));

		// Test and validate
		_logger.debug("validate..");
		StepVerifier.create(flux2)      // this row should be present and no other rows
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 200 : "Expected value is 200, but received = "+row.get("ID");
					})
					.expectComplete()
					.verify();
		
		// commit the transaction
		_logger.debug("commit trasaction");
		Mono<Void> mono5 = Mono.just(con2.get())
							   .flatMap(c -> c.commitTransaction());
				
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono5)
			        .expectComplete()	    
			        .verify();	
		
		// close connection
		con2.get().close().block();
	}
	
	/**
	 * Test Serializable Isolation Level. Make sure we don't have uncommited read in this isolation level.
	 */
	@Test
	public void test402Serializable()
	{	
		_logger.debug("running test402Serializable");
		
		AtomicReference<DB2Connection> con1 = new AtomicReference<DB2Connection>();
		AtomicReference<DB2Connection> con2 = new AtomicReference<DB2Connection>();
		
		// get connection, set isolation level and insert data
		_logger.debug("inserting data - using execute direct");
		Mono<Void> mono = getNewConnection()
							   .doOnNext(c -> {con1.set(c); c.setTransactionIsolationLevel(IsolationLevel.SERIALIZABLE); c.beginTransaction();})
							   .flatMap(c -> c.executeUpdate("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (1000)"));

		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono)
			        .expectComplete()	    
			        .verify();	
		
		// Data is not commited yet, try reading it from a different connection
		
		// Get a new connection, set isolation level and query the inserted data
		// This should block waiting for read lock, then timeout and fail
		_logger.debug("select the inserted data");
		Flux<Row> flux = getNewConnection()
							   .doOnNext(c -> {
								   con2.set(c); c.setTransactionIsolationLevel(IsolationLevel.SERIALIZABLE); 
								   c.beginTransaction();
								})
							   .delayUntil(c -> {
							    	 if (_isLUW) {
							    		 return c.executeUpdate("SET LOCK TIMEOUT 10");
							    	 }
							    	 else {
							    		 // Not applicable for zOS
							    		 return Mono.empty();
							    	 }
							     })
							   .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1000")									   		  
											  .execute())
							   .flatMapMany(result -> result.map((row, md) -> row));

		// Test and validate
		_logger.debug("validate..");
		StepVerifier.create(flux)
					.expectError()
					.verify();
		
		// rollback con1 transaction
		_logger.debug("rollback trasaction");
		Mono<Void> mono2 = Mono.just(con1.get())
							   .flatMap(c -> c.rollbackTransaction());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono2)
			        .expectComplete()	    
			        .verify();
		
		// close con1
		con1.get().close().block();
		
		// rollback con2 transaction
		_logger.debug("rollback trasaction");
		Mono<Void> mono3 = Mono.just(con2.get())
							   .flatMap(c -> c.rollbackTransaction());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono3)
			        .expectComplete()	    
			        .verify();
				
		// close con2
		con2.get().close().block();
	}
	
	/**
	 * Test for Serializable Transaction Isolation. Show we could repeat a read successfully.
	 */
	@Test
	public void test403Serializable()
	{	
		_logger.debug("running test403Serializable");
		
		AtomicReference<DB2Connection> con1 = new AtomicReference<DB2Connection>();
		AtomicReference<DB2Connection> con2 = new AtomicReference<DB2Connection>();
		
		// get connection, insert data
		_logger.debug("inserting data - using prepared statement");
		Mono<DB2Result> mono = _pool.getConnection()
								    .doOnNext(c -> {
								    	con1.set(c);
								    	c.setTransactionIsolationLevel(IsolationLevel.SERIALIZABLE);
								    })
								    .flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID) VALUES (200)")
								  				   .execute())
								    .doAfterTerminate(() -> con1.get().release());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono)
					.assertNext(rs -> {
			        	assert rs != null : "Received a null object";
			        	assert rs instanceof DB2Result : "Did not receive a DB2Result object";
			        	assert rs.getNumRowsUpdated() == 1 : "Expected number of rows inserted = 1, but received = "+rs.getNumRowsUpdated();
			        })
		            .expectComplete()
		            .verify();		
		
		// Get connection, query the inserted data
		_logger.debug("select the inserted data");
		AtomicReference<DB2Result> res = new AtomicReference<DB2Result>();
		Flux<Row> flux = getNewConnection()
							  .doOnNext(c -> {
								  con2.set(c);
								  c.setTransactionIsolationLevel(IsolationLevel.SERIALIZABLE);
							  })
							  .delayUntil(c -> c.beginTransaction())  // get a transaction started and keep it open
							  .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 200")									   		  
									  		 .execute())
							  .doOnNext(result -> res.set(result))
							  .flatMapMany(result -> result.map((row, md) -> row));

		// Test and validate
		_logger.debug("validate..");
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 200 : "Expected value is 200, but received = "+row.get("ID");
					})
					.expectComplete()
					.verify();
		
		// close resultset
		Mono<Void> mres = Mono.just(res.get())
							  .flatMap(rs -> rs.close());
		
		// Test
		StepVerifier.create(mres)
			        .expectComplete()	    
			        .verify();	
		
		// Now try to update that row
		// this update should wait for the update lock, then timeout and fail
		_logger.debug("updating the inserted data - using prepared statement");
		Mono<DB2Result> mono3 = _pool.getConnection()
								     .doOnNext(c -> {
								    	 con1.set(c);
								    	 c.setTransactionIsolationLevel(IsolationLevel.SERIALIZABLE);
								     })
								     .delayUntil(c -> {
								    	 if (_isLUW) {
								    		 return c.executeUpdate("SET LOCK TIMEOUT 10");
								    	 }
								    	 else {
								    		 // Not applicable for zOS
								    		 return Mono.empty();
								    	 }
								     })
								     .flatMap(c -> c.createStatement("UPDATE UNIT_TEST_"+_randomInt + " SET ID = 250 WHERE ID = 200")
								  				   .execute())
								     .doAfterTerminate(() -> con1.get().release());
		
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono3)
		            .expectError() 
		            .verify();		
		
		
		// Try querying for the same data queried earlier, we should be able to repeat the read in this transaction
		_logger.debug("select the inserted data");
		Flux<Row> flux2 = Flux.just(con2.get())
							  .flatMap(c -> c.createStatement("SELECT ID FROM UNIT_TEST_"+_randomInt+" WHERE ID = 200")									   		  
									  		 .execute())
							  .flatMap(result -> result.map((row, md) -> row));

		// Test and validate
		_logger.debug("validate..");
		StepVerifier.create(flux2)      // this row should be present
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 200 : "Expected value is 200, but received = "+row.get("ID");
					})
					.expectComplete()
					.verify();
		
		// commit the transaction
		_logger.debug("commit trasaction");
		Mono<Void> mono5 = Mono.just(con2.get())
							   .flatMap(c -> c.commitTransaction());
				
		// Test
		_logger.debug("verifying..");
		StepVerifier.create(mono5)
			        .expectComplete()	    
			        .verify();	
		
		// close connection
		con2.get().close().block();
	}
}
