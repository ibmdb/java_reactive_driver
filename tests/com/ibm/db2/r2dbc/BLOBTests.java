package com.ibm.db2.r2dbc;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.r2dbc.spi.Row;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/*
 * We use fixed ordering of test execution, so note that the tests are named test001X, test002X etc.., in ascending order
 */

/**
 * Tests for BLOB.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BLOBTests extends BaseTestCase
{
	private static final Logger _logger =  LoggerFactory.getLogger(BLOBTests.class.getName());
	
	@BeforeClass
	public static void createTable()
	{
		_logger.debug("createTable() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		Mono<Void> mono = getNewConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.executeUpdate("CREATE TABLE UNIT_TEST_"+_randomInt + " (ID INTEGER UNIQUE NOT NULL, DATA1 BLOB, DATA2 BLOB(10M))"));

		StepVerifier.create(mono)
            .expectComplete()
            .verify();	
		
		con.get().close().block();
	}
	
	
	/* INSERT tests */
	
	/**
	 * Test inserting a row using a prepared statement with bind values
	 */
	@Test
	public void test001Insert()
	{
		_logger.debug("running test001Insert");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		AtomicReference<DB2Blob> blob1 = new AtomicReference<DB2Blob>();
		AtomicReference<DB2Blob> blob2 = new AtomicReference<DB2Blob>();
		
		// data for DATA1 column
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 100; i++) {
			sb.append(i+". "+"Hello World\n");
		}
		
		// data for DATA2 column
		Flux<ByteBuffer> flux = Flux.create(sink -> {
			for (int i = 0; i < 1024; i++) {
				String s = i+". "+"Best Wishes\n";
				sink.next(ByteBuffer.wrap(s.getBytes()));
			}
			sink.complete();
		});
				
		// get connection, insert data and then release the connection
		Mono<DB2Result> mono = _pool.getConnection()
			.doOnNext(c -> {
				con.set(c);
				blob1.set(c.createDB2Blob(sb.toString().getBytes()));
				blob2.set(c.createDB2Blob(flux));
			})
			.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, DATA1, DATA2) VALUES (?, ?, ?)")
	   		  	.bind(1, 1)
	   		  	.bind(2, blob1.get())
	   		  	.bind(3, blob2.get())
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
	 * Test inserting a row using a prepared statement with bind values. Set null for BLOB data.
	 */
	@Test
	public void test002Insert()
	{
		_logger.debug("running test002Insert");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// get connection, insert data and then release the connection
		Mono<DB2Result> mono = _pool.getConnection()
			.doOnNext(c -> {
				con.set(c);
			})
			.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, DATA1, DATA2) VALUES (?, ?, ?)")
	   		  	.bind(1, 2)
	   		  	.bindNull(2, DB2Blob.class)
	   		  	.bindNull(3, DB2Blob.class)
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
	 * Test inserting a row using a prepared statement with bind values. Set one null and one non-null value for BLOB data.
	 */
	@Test
	public void test003Insert()
	{
		_logger.debug("running test003Insert");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		AtomicReference<DB2Blob> blob1 = new AtomicReference<DB2Blob>();
		
		// data for DATA1 column
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 100; i++) {
			sb.append(i+". "+"Hello World\n");
		}
				
		// get connection, insert data and then release the connection
		Mono<DB2Result> mono = _pool.getConnection()
			.doOnNext(c -> {
				con.set(c);
				blob1.set(c.createDB2Blob(sb.toString().getBytes()));
			})
			.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, DATA1, DATA2) VALUES (?, ?, ?)")
	   		  	.bind(1, 3)
	   		  	.bind(2, blob1.get())
	   		  	.bindNull(3, DB2Blob.class)
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
	
	/**
	 * Test querying for the data inserted.
	 */
	@Test
	public void test101Select()
	{
		_logger.debug("running test101Select");
		
		// Get connection and insert data
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		AtomicReference<DB2Blob> blob1 = new AtomicReference<DB2Blob>();
		AtomicReference<DB2Blob> blob2 = new AtomicReference<DB2Blob>();
				
		// data for DATA1 column
		StringBuilder isb1 = new StringBuilder();
		for (int i = 0; i < 2/*100*/; i++) {
			isb1.append(i+". "+"Hello World\n");
		}
		
		// data for DATA2 column
		StringBuilder isb2 = new StringBuilder();
		Flux<ByteBuffer> iflux = Flux.create(sink -> {
			for (int i = 0; i < 2/*1024*/; i++) {
				String s = i+". "+"Best Wishes\n";
				isb2.append(s);
				sink.next(ByteBuffer.wrap(s.getBytes()));
			}
			sink.complete();
		});
				
		// get connection, insert data and then release the connection
		Mono<DB2Result> mono = _pool.getConnection()
			.doOnNext(c -> {
				con.set(c);
				blob1.set(c.createDB2Blob(isb1.toString().getBytes()));
				blob2.set(c.createDB2Blob(iflux));
			})
			.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, DATA1, DATA2) VALUES (?, ?, ?)")
	   		  	.bind(1, 101)
	   		  	.bind(2, blob1.get())
	   		  	.bind(3, blob2.get())
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

		// Query the inserted data and then release the connection
		StringBuffer osb1 = new StringBuffer();
		StringBuffer osb2 = new StringBuffer();
		Flux<Row> oflux = _pool.getConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.createStatement("SELECT ID, DATA1, DATA2 FROM UNIT_TEST_"+_randomInt +" WHERE ID = ?")
				.bind(1, 101)
				.execute())
			.flatMapMany(result -> result.map((row, md) -> row))
			.doAfterTerminate(() -> {
				con.get().release();
			});

		// Test
		StepVerifier.create(oflux)
			.assertNext(row -> {
			 	assert row != null : "Received a null object";
			 	assert row instanceof Row : "Did not receive a Row object";
			 	assert row.get("ID", Integer.class) == 101 : "Expected value is 101, but received = "+row.get("ID");
			 	
			 	DB2Blob oblob1 = (DB2Blob) row.get("DATA1");
				oblob1.stream()
					.subscribe(b -> {	
						osb1.append(new String(b.array()));
					});
				
				DB2Blob oblob2 = (DB2Blob) row.get("DATA2");
				oblob2.stream()
					.subscribe(b -> {	
						osb2.append(new String(b.array()));
					});
			})
			.expectComplete()
			.verify();
		
		// validate the received blob data
		assert osb1.toString().equals(isb1.toString()) : "Retrived BLOB data did not match the inserted BLOB data";
		assert osb2.toString().equals(isb2.toString()) : "Retrived BLOB data did not match the inserted BLOB data";
	}
	
	/**
	 * Test querying for the data inserted. Test for null BLOB values.
	 */
	@Test
	public void test102Select()
	{
		_logger.debug("running test102Select");
		
		// Get connection and insert data
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();

		// get connection, insert data and then release the connection
		Mono<DB2Result> mono = _pool.getConnection()
			.doOnNext(c -> {
				con.set(c);
			})
			.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, DATA1, DATA2) VALUES (?, ?, ?)")
	   		  	.bind(1, 102)
	   		  	.bindNull(2, DB2Blob.class)
	   		  	.bindNull(3, DB2Blob.class)
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

		// Query the inserted data and then release the connection
		Flux<Row> oflux = _pool.getConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.createStatement("SELECT ID, DATA1, DATA2 FROM UNIT_TEST_"+_randomInt +" WHERE ID = ?")
				.bind(1, 102)
				.execute())
			.flatMapMany(result -> result.map((row, md) -> row))
			.doAfterTerminate(() -> {
				con.get().release();
			});

		// Test
		StepVerifier.create(oflux)
			.assertNext(row -> {
			 	assert row != null : "Received a null object";
			 	assert row instanceof Row : "Did not receive a Row object";
			 	assert row.get("ID", Integer.class) == 102 : "Expected value is 102, but received = "+row.get("ID");
			 	assert row.get("DATA1", DB2Blob.class) == null : "Expected value is null, but a non null value is received";
			 	assert row.get("DATA2", DB2Blob.class) == null : "Expected value is null, but a non null value is received";
			})
			.expectComplete()
			.verify();
	}
	
	/**
	 * Test querying for the data inserted. Test for one null and one non-null blob value.
	 */
	@Test
	public void test103Select()
	{
		_logger.debug("running test103Select");
		
		// Get connection and insert data
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		AtomicReference<DB2Blob> blob1 = new AtomicReference<DB2Blob>();

		// data for DATA1 column
		StringBuilder isb1 = new StringBuilder();
		for (int i = 0; i < 100; i++) {
			isb1.append(i+". "+"Hello World\n");
		}
				
		// get connection, insert data and then release the connection
		Mono<DB2Result> mono = _pool.getConnection()
			.doOnNext(c -> {
				con.set(c);
				blob1.set(c.createDB2Blob(isb1.toString().getBytes()));
			})
			.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, DATA1, DATA2) VALUES (?, ?, ?)")
	   		  	.bind(1, 103)
	   		  	.bind(2, blob1.get())
	   		  	.bindNull(3, DB2Blob.class)
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

		// Query the inserted data and then release the connection
		StringBuilder osb1 = new StringBuilder();
		Flux<Row> oflux = _pool.getConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.createStatement("SELECT ID, DATA1, DATA2 FROM UNIT_TEST_"+_randomInt +" WHERE ID = ?")
				.bind(1, 103)
				.execute())
			.flatMapMany(result -> result.map((row, md) -> row))
			.doAfterTerminate(() -> {
				con.get().release();
			});

		// Test
		StepVerifier.create(oflux)
			.assertNext(row -> {
			 	assert row != null : "Received a null object";
			 	assert row instanceof Row : "Did not receive a Row object";
			 	assert row.get("ID", Integer.class) == 103 : "Expected value is 103, but received = "+row.get("ID");
			 	
			 	DB2Blob oblob1 = (DB2Blob) row.get("DATA1");
				oblob1.stream()
					.subscribe(s -> {	
						osb1.append(new String(s.array()));
					});
				
			 	assert row.get("DATA2", DB2Blob.class) == null : "Expected value is null, but a non null value is received";
			})
			.expectComplete()
			.verify();
		
		// validate blob data received
		assert osb1.toString().equals(isb1.toString()) : "Retrived BLOB data did not match the inserted BLOB data";
	}
	
	/**
	 * Test querying for the data inserted. Insert two rows with blob and retrieve them.
	 */
	@Test
	public void test104Select()
	{
		_logger.debug("running test104Select");
		
		// Get connection and insert data
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		AtomicReference<DB2Blob> blob1 = new AtomicReference<DB2Blob>();
		AtomicReference<DB2Blob> blob2 = new AtomicReference<DB2Blob>();
		
		// data for DATA1 column
		StringBuilder isb1 = new StringBuilder();
		for (int i = 0; i < 100; i++) {
			isb1.append(i+". "+"Hello World\n");
		}
		
		// data for DATA2 column
		StringBuilder isb2 = new StringBuilder();
		Flux<ByteBuffer> iflux1 = Flux.create(sink -> {
			for (int i = 0; i < 1024; i++) {
				String s = i+". "+"Best Wishes\n";
				isb2.append(s);
				sink.next(ByteBuffer.wrap(s.getBytes()));
			}
			sink.complete();
		});			
		
		// get connection, insert data and then release the connection
		Mono<DB2Result> mono = _pool.getConnection()
			.doOnNext(c -> {
				con.set(c);
				blob1.set(c.createDB2Blob(isb1.toString().getBytes()));
				blob2.set(c.createDB2Blob(iflux1));
			})
			.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, DATA1, DATA2) VALUES (?, ?, ?)")
	   		  	.bind(1, 1041)
	   		  	.bind(2, blob1.get())
	   		  	.bind(3, blob2.get())
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
		
		// insert second row with blob
		// data for DATA1 column
		StringBuilder isb3 = new StringBuilder();
		for (int i = 0; i < 100; i++) {
			isb3.append(i+". "+"Second Hello World\n");
		}
		
		// data for DATA2 column
		StringBuilder isb4 = new StringBuilder();
		Flux<ByteBuffer> iflux2 = Flux.create(sink -> {
			for (int i = 0; i < 1024; i++) {
				String s = i+". "+"Second Best Wishes\n";
				isb4.append(s);
				sink.next(ByteBuffer.wrap(s.getBytes()));
			}
			sink.complete();
		});		
		
		mono = _pool.getConnection()
				.doOnNext(c -> {
					con.set(c);
					blob1.set(c.createDB2Blob(isb3.toString().getBytes()));
					blob2.set(c.createDB2Blob(iflux2));
				})
				.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, DATA1, DATA2) VALUES (?, ?, ?)")
		   		  	.bind(1, 1042)
		   		  	.bind(2, blob1.get())
		   		  	.bind(3, blob2.get())
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

		// Query the inserted data and then release the connection
		StringBuilder osb1 = new StringBuilder();
		StringBuilder osb2 = new StringBuilder();
		StringBuilder osb3 = new StringBuilder();
		StringBuilder osb4 = new StringBuilder();
		Flux<Row> oflux = _pool.getConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.createStatement("SELECT ID, DATA1, DATA2 FROM UNIT_TEST_"+_randomInt +" WHERE ID in (?, ?) ORDER BY ID")
				.bind(1, 1041)
				.bind(2, 1042)
				.execute())
			.flatMapMany(result -> result.map((row, md) -> row))
			.doAfterTerminate(() -> {
				con.get().release();
			});

		// Test
		StepVerifier.create(oflux)
			.assertNext(row -> {
			 	assert row != null : "Received a null object";
			 	assert row instanceof Row : "Did not receive a Row object";
			 	assert row.get("ID", Integer.class) == 1041 : "Expected value is 1041, but received = "+row.get("ID");
			 	
			 	DB2Blob oblob1 = (DB2Blob) row.get("DATA1");
				oblob1.stream()
					.doOnNext(s -> {
						osb1.append(new String(s.array()));
					})
					.subscribe();
				
				DB2Blob oblob2 = (DB2Blob) row.get("DATA2");
				oblob2.stream()
					.doOnNext(s -> {
						osb2.append(new String(s.array()));
					})
					.subscribe(); 
			})
			.assertNext(row -> {
			 	assert row != null : "Received a null object";
			 	assert row instanceof Row : "Did not receive a Row object";
			 	assert row.get("ID", Integer.class) == 1042 : "Expected value is 1041, but received = "+row.get("ID");
			 	
			 	DB2Blob oblob1 = (DB2Blob) row.get("DATA1");
				oblob1.stream()
					.doOnNext(s -> {
						osb3.append(new String(s.array()));
					})
					.subscribe();
				
				DB2Blob oblob2 = (DB2Blob) row.get("DATA2");
				oblob2.stream()
					.doOnNext(s -> {
						osb4.append(new String(s.array()));
					})
					.subscribe(); 
				
			})
			.expectComplete()
			.verify();
		
		// validate blobs
		assert osb1.toString().equals(isb1.toString()) : "Retrived BLOB data did not match the inserted BLOB data";
		assert osb2.toString().equals(isb2.toString()) : "Retrived BLOB data did not match the inserted BLOB data";
		assert osb3.toString().equals(isb3.toString()) : "Retrived BLOB data did not match the inserted BLOB data";
		assert osb4.toString().equals(isb4.toString()) : "Retrived BLOB data did not match the inserted BLOB data";
	}

	/**
	 * Test querying for the data inserted. Test querying a non-existing row with blob column.
	 */
	@Test
	public void test105Select()
	{
		_logger.debug("running test105Select");
		
		// Get connection and insert data
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		AtomicReference<DB2Blob> blob1 = new AtomicReference<DB2Blob>();
		AtomicReference<DB2Blob> blob2 = new AtomicReference<DB2Blob>();
		
		// data for DATA1 column
		StringBuilder isb1 = new StringBuilder();
		for (int i = 0; i < 100; i++) {
			isb1.append(i+". "+"Hello World\n");
		}
		
		// data for DATA2 column
		StringBuilder isb2 = new StringBuilder();
		Flux<ByteBuffer> iflux = Flux.create(sink -> {
			for (int i = 0; i < 1024; i++) {
				String s = i+". "+"Best Wishes\n";
				isb2.append(s);
				sink.next(ByteBuffer.wrap(s.getBytes()));
			}
			sink.complete();
		});		
		
		// get connection, insert data and then release the connection
		Mono<DB2Result> mono = _pool.getConnection()
			.doOnNext(c -> {
				con.set(c);
				blob1.set(c.createDB2Blob(isb1.toString().getBytes()));
				blob2.set(c.createDB2Blob(iflux));
			})
			.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, DATA1, DATA2) VALUES (?, ?, ?)")
	   		  	.bind(1, 105)
	   		  	.bind(2, blob1.get())
	   		  	.bind(3, blob2.get())
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

		// Query the inserted data and then release the connection
		Flux<Row> oflux = _pool.getConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.createStatement("SELECT ID, DATA1, DATA2 FROM UNIT_TEST_"+_randomInt +" WHERE ID = ?")
				.bind(1, 0)  // non existing row id
				.execute())
			.flatMapMany(result -> result.map((row, md) -> row))
			.doAfterTerminate(() -> {
				con.get().release();
			});

		// Test
		StepVerifier.create(oflux)
			.expectComplete()
			.verify();
	}
	
	
	/* UPDATE tests */
	
	/**
	 * Test querying for the updated data.
	 */
	@Test
	public void test301Upate()
	{
		_logger.debug("running test301Update");
		
		// Get connection and insert data
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		AtomicReference<DB2Blob> blob1 = new AtomicReference<DB2Blob>();
		AtomicReference<DB2Blob> blob2 = new AtomicReference<DB2Blob>();
		
		// data for DATA1 column
		StringBuilder isb1 = new StringBuilder();
		for (int i = 0; i < 100; i++) {
			isb1.append(i+". "+"Hello World\n");
		}
		
		// data for DATA2 column
		StringBuilder isb2 = new StringBuilder();
		Flux<ByteBuffer> iflux = Flux.create(sink -> {
			for (int i = 0; i < 1024; i++) {
				String s = i+". "+"Best Wishes\n";
				isb2.append(s);
				sink.next(ByteBuffer.wrap(s.getBytes()));
			}
			sink.complete();
		});			
		
		// get connection, insert data and then release the connection
		Mono<DB2Result> mono = _pool.getConnection()
			.doOnNext(c -> {
				con.set(c);
				blob1.set(c.createDB2Blob(isb1.toString().getBytes()));
				blob2.set(c.createDB2Blob(iflux));
			})
			.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, DATA1, DATA2) VALUES (?, ?, ?)")
	   		  	.bind(1, 301)
	   		  	.bind(2, blob1.get())
	   		  	.bind(3, blob2.get())
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
	
		// update the inserted blob data
		mono = _pool.getConnection()
				.doOnNext(c -> {
					con.set(c);
					blob1.set(c.createDB2Blob(isb1.toString().getBytes()));
				})
				.flatMap(c -> c.createStatement("UPDATE UNIT_TEST_"+_randomInt + " SET DATA2 = ? WHERE ID = ?")
		   		  	.bind(1, blob1.get())
		   		  	.bind(2, 301)
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
		
		// Query the inserted and updated data and then release the connection
		StringBuilder osb1 = new StringBuilder();
		StringBuilder osb2 = new StringBuilder();
		Flux<Row> oflux = _pool.getConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.createStatement("SELECT ID, DATA1, DATA2 FROM UNIT_TEST_"+_randomInt +" WHERE ID = ?")
				.bind(1, 301)
				.execute())
			.flatMapMany(result -> result.map((row, md) -> row))
			.doAfterTerminate(() -> {
				con.get().release();
			});
	
		// Test
		StepVerifier.create(oflux)
			.assertNext(row -> {
			 	assert row != null : "Received a null object";
			 	assert row instanceof Row : "Did not receive a Row object";
			 	assert row.get("ID", Integer.class) == 301 : "Expected value is 301, but received = "+row.get("ID");
			 	
			 	DB2Blob oblob1 = (DB2Blob) row.get("DATA1");
				oblob1.stream()
					.subscribe(s -> {	
						osb1.append(new String(s.array()));
					});
				
				DB2Blob oblob2 = (DB2Blob) row.get("DATA2");
				oblob2.stream()
					.subscribe(s -> {	
						osb2.append(new String(s.array()));
					});
			})
			.expectComplete()
			.verify();
		
		// validate the received blob data
		assert osb1.toString().equals(isb1.toString()) : "Retrived BLOB data did not match the inserted BLOB data";
		assert osb2.toString().equals(isb1.toString()) : "Retrived BLOB data did not match the inserted BLOB data";
	}
	
	/**
	 * Test querying for the updated data. Set one blob value to null.
	 */
	@Test
	public void test302Upate()
	{
		_logger.debug("running test302Update");
		
		// Get connection and insert data
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		AtomicReference<DB2Blob> blob1 = new AtomicReference<DB2Blob>();
		AtomicReference<DB2Blob> blob2 = new AtomicReference<DB2Blob>();
		
		// data for DATA1 column
		StringBuilder isb1 = new StringBuilder();
		for (int i = 0; i < 100; i++) {
			isb1.append(i+". "+"Hello World\n");
		}
		
		// data for DATA2 column
		StringBuilder isb2 = new StringBuilder();
		Flux<ByteBuffer> iflux = Flux.create(sink -> {
			for (int i = 0; i < 1024; i++) {
				String s = i+". "+"Best Wishes\n";
				isb2.append(s);
				sink.next(ByteBuffer.wrap(s.getBytes()));
			}
			sink.complete();
		});				
		
		// get connection, insert data and then release the connection
		Mono<DB2Result> mono = _pool.getConnection()
			.doOnNext(c -> {
				con.set(c);
				blob1.set(c.createDB2Blob(isb1.toString().getBytes()));
				blob2.set(c.createDB2Blob(iflux));
			})
			.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, DATA1, DATA2) VALUES (?, ?, ?)")
	   		  	.bind(1, 302)
	   		  	.bind(2, blob1.get())
	   		  	.bind(3, blob2.get())
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
	
		// update the inserted blob data
		mono = _pool.getConnection()
				.doOnNext(c -> {
					con.set(c);
					blob1.set(c.createDB2Blob(isb1.toString().getBytes()));
				})
				.flatMap(c -> c.createStatement("UPDATE UNIT_TEST_"+_randomInt + " SET DATA2 = ? WHERE ID = ?")
		   		  	.bindNull(1, DB2Blob.class)
		   		  	.bind(2, 302)
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
		
		// Query the inserted and updated data and then release the connection
		StringBuilder osb1 = new StringBuilder();
		Flux<Row> oflux = _pool.getConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.createStatement("SELECT ID, DATA1, DATA2 FROM UNIT_TEST_"+_randomInt +" WHERE ID = ?")
				.bind(1, 302)
				.execute())
			.flatMapMany(result -> result.map((row, md) -> row))
			.doAfterTerminate(() -> {
				con.get().release();
			});
	
		// Test
		StepVerifier.create(oflux)
			.assertNext(row -> {
			 	assert row != null : "Received a null object";
			 	assert row instanceof Row : "Did not receive a Row object";
			 	assert row.get("ID", Integer.class) == 302 : "Expected value is 302, but received = "+row.get("ID");
			 	
			 	DB2Blob oblob1 = (DB2Blob) row.get("DATA1");
				oblob1.stream()
					.subscribe(s -> {	
						osb1.append(new String(s.array()));
					});
				
				DB2Blob oblob2 = (DB2Blob) row.get("DATA2");
				assert oblob2 == null : "Expecting a null value, but received a non-null value";
			})
			.expectComplete()
			.verify();
		
		// validate the received blob data
		assert osb1.toString().equals(isb1.toString()) : "Retrived BLOB data did not match the inserted BLOB data";
	}
	
	/**
	 * Test querying for the updated data. Set both blob value to null.
	 */
	@Test
	public void test303Upate()
	{
		_logger.debug("running test303Update");
		
		// Get connection and insert data
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		AtomicReference<DB2Blob> blob1 = new AtomicReference<DB2Blob>();
		AtomicReference<DB2Blob> blob2 = new AtomicReference<DB2Blob>();
		
		// data for DATA1 column
		StringBuilder isb1 = new StringBuilder();
		for (int i = 0; i < 100; i++) {
			isb1.append(i+". "+"Hello World\n");
		}
		
		// data for DATA2 column
		StringBuilder isb2 = new StringBuilder();
		Flux<ByteBuffer> iflux = Flux.create(sink -> {
			for (int i = 0; i < 1024; i++) {
				String s = i+". "+"Best Wishes\n";
				isb2.append(s);
				sink.next(ByteBuffer.wrap(s.getBytes()));
			}
			sink.complete();
		});				
		
		// get connection, insert data and then release the connection
		Mono<DB2Result> mono = _pool.getConnection()
			.doOnNext(c -> {
				con.set(c);
				blob1.set(c.createDB2Blob(isb1.toString().getBytes()));
				blob2.set(c.createDB2Blob(iflux));
			})
			.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, DATA1, DATA2) VALUES (?, ?, ?)")
	   		  	.bind(1, 303)
	   		  	.bind(2, blob1.get())
	   		  	.bind(3, blob2.get())
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
	
		// update the inserted blob data
		mono = _pool.getConnection()
				.doOnNext(c -> {
					con.set(c);
					blob1.set(c.createDB2Blob(isb1.toString().getBytes()));
				})
				.flatMap(c -> c.createStatement("UPDATE UNIT_TEST_"+_randomInt + " SET DATA1 = ?, DATA2 = ? WHERE ID = ?")
		   		  	.bindNull(1, DB2Blob.class)
		   		  	.bindNull(2, DB2Blob.class)
		   		  	.bind(3, 303)
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
		
		// Query the inserted and updated data and then release the connection
		Flux<Row> oflux = _pool.getConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.createStatement("SELECT ID, DATA1, DATA2 FROM UNIT_TEST_"+_randomInt +" WHERE ID = ?")
				.bind(1, 303)
				.execute())
			.flatMapMany(result -> result.map((row, md) -> row))
			.doAfterTerminate(() -> {
				con.get().release();
			});
	
		// Test
		StepVerifier.create(oflux)
			.assertNext(row -> {
			 	assert row != null : "Received a null object";
			 	assert row instanceof Row : "Did not receive a Row object";
			 	assert row.get("ID", Integer.class) == 303 : "Expected value is 303, but received = "+row.get("ID");
			 	
			 	DB2Blob oblob1 = (DB2Blob) row.get("DATA1");
			 	assert oblob1 == null : "Expecting a null value, but received a non-null value";
				
				DB2Blob oblob2 = (DB2Blob) row.get("DATA2");
				assert oblob2 == null : "Expecting a null value, but received a non-null value";
			})
			.expectComplete()
			.verify();
	}
	
	/* DELETE tests */
	
	/**
	 * Test querying for a deleted row with blob column
	 */
	@Test
	public void test401Delete()
	{
		_logger.debug("running test401Delete");
		
		// Get connection and insert data
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		AtomicReference<DB2Blob> blob1 = new AtomicReference<DB2Blob>();
		AtomicReference<DB2Blob> blob2 = new AtomicReference<DB2Blob>();
		
		// data for DATA1 column
		StringBuilder isb1 = new StringBuilder();
		for (int i = 0; i < 100; i++) {
			isb1.append(i+". "+"Hello World\n");
		}
		
		// data for DATA2 column
		StringBuilder isb2 = new StringBuilder();
		Flux<ByteBuffer> iflux = Flux.create(sink -> {
			for (int i = 0; i < 1024; i++) {
				String s = i+". "+"Best Wishes\n";
				isb2.append(s);
				sink.next(ByteBuffer.wrap(s.getBytes()));
			}
			sink.complete();
		});				
		
		// get connection, insert data and then release the connection
		Mono<DB2Result> mono = _pool.getConnection()
			.doOnNext(c -> {
				con.set(c);
				blob1.set(c.createDB2Blob(isb1.toString().getBytes()));
				blob2.set(c.createDB2Blob(iflux));
			})
			.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt + " (ID, DATA1, DATA2) VALUES (?, ?, ?)")
	   		  	.bind(1, 401)
	   		  	.bind(2, blob1.get())
	   		  	.bind(3, blob2.get())
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
	
		// delete the row with blob data
		mono = _pool.getConnection()
				.doOnNext(c -> {
					con.set(c);
					blob1.set(c.createDB2Blob(isb1.toString().getBytes()));
				})
				.flatMap(c -> c.createStatement("DELETE UNIT_TEST_"+_randomInt + " WHERE ID = ?")
		   		  	.bind(1, 401)
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
		
		// Query the inserted and updated data and then release the connection
		Flux<Row> oflux = _pool.getConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.createStatement("SELECT ID, DATA1, DATA2 FROM UNIT_TEST_"+_randomInt +" WHERE ID = ?")
				.bind(1, 401)
				.execute())
			.flatMapMany(result -> result.map((row, md) -> row))
			.doAfterTerminate(() -> {
				con.get().release();
			});
	
		// Test
		StepVerifier.create(oflux)
			.expectComplete()
			.verify();
	}
}
