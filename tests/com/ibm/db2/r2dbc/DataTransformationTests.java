package com.ibm.db2.r2dbc;

import java.math.BigDecimal;
import java.math.BigInteger;
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
 * We use fixed ordering of test execution, so note that the tests are named test1X, test2X etc..,
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DataTransformationTests extends BaseTestCase
{
	private static final Logger _logger =  LoggerFactory.getLogger(DataTransformationTests.class.getName());

	public DataTransformationTests()
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
								.flatMap(c -> c.executeUpdate(
									"CREATE TABLE UNIT_TEST_"+_randomInt + " ("+
										"ID             INTEGER NOT NULL UNIQUE, "+
										"SMALLINT_COL   SMALLINT, "+
										"INTEGER_COL    INTEGER, "+
										"BIGINT_COL     BIGINT, "+
										"DECIMAL_COL    DECIMAL(5,3), "+
										"NUMERIC_COL    NUMERIC(5,3), "+
										"DECFLOAT_COL   DECFLOAT, "+
										"REAL_COL       REAL, "+
										"DOUBLE_COL     DOUBLE, "+
										"CHAR_COL       CHAR, "+
										"VARCHAR2_COL   VARCHAR(2), "+
										"VARCHAR5_COL   VARCHAR(5), "+
										"VARCHAR10_COL  VARCHAR(10), "+
										"VARCHAR50_COL  VARCHAR(50), "+
										"DATE_COL       DATE, "+
										"TIME_COL       TIME, "+
										"TIMESTAMP_COL  TIMESTAMP"+
									")"
								));
		
		StepVerifier.create(mono)
		            .expectComplete()
		            .verify();	
	
		con.get().close().block();
	}	
	
	/**
	 * Test parameter data transform from byte to short.
	 */
	@Test
	public void testParam001ByteToShort()
	{
		_logger.debug("running testParam001ByteToShort");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		byte value = (byte)0x0F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, SMALLINT_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, SMALLINT_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	assert row.get("SMALLINT_COL", Short.class) == (short)15: "Expected value is 15, but received = "+row.get("SHORT_COL");
					})
					.expectComplete()
					.verify();

	}
	
	/**
	 * Test parameter data transform from byte to integer.
	 */
	@Test
	public void testParam002ByteToInteger()
	{
		_logger.debug("running testParam002ByteToInteger");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		byte value = (byte)0x0F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, INTEGER_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, INTEGER_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());

		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	assert row.get("INTEGER_COL", Integer.class) == 15: "Expected value is 15, but received = "+row.get("INTEGER_COL");
					})
					.expectComplete()
					.verify();

	}


	/**
	 * Test parameter data transform from byte to long.
	 */
	@Test
	public void testParam003ByteToLong()
	{
		_logger.debug("running testParam003ByteToLong");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		byte value = (byte)0x7F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, BIGINT_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, BIGINT_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	assert row.get("BIGINT_COL", Long.class) == 127: "Expected value is 127, but received = "+row.get("BIGINT_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from byte to float.
	 */
	@Test
	public void testParam004ByteToFloat()
	{
		_logger.debug("running testParam004ByteToFloat");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		byte value = (byte)0x7F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, REAL_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, REAL_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	assert row.get("REAL_COL", Float.class) == 127.0: "Expected value is 127.0, but received = "+row.get("REAL_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from byte to double.
	 */
	@Test
	public void testParam005ByteToDouble()
	{
		_logger.debug("running testParam005ByteToDouble");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		byte value = (byte)0x7F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, DOUBLE_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, DOUBLE_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	assert row.get("DOUBLE_COL", Double.class) == 127.0: "Expected value is 127.0, but received = "+row.get("DOUBLE_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from byte to string.
	 */
	@Test
	public void testParam006ByteToString()
	{
		_logger.debug("running testParam006ByteToString");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		byte value = (byte)0x7F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, VARCHAR10_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, VARCHAR10_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	assert row.get("VARCHAR10_COL", String.class).equals("127"): "Expected value is 127.0, but received = "+row.get("VARCHAR10_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from byte to string. Negative case, byte to char should fail.
	 */
	@Test
	public void testParam007ByteToString()
	{
		_logger.debug("running testParam007ByteToString");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		byte value = (byte)0x7F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, CHAR_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
		// Test
		StepVerifier.create(mono)
		            .expectError()
		            .verify();
	}


	/**
	 * Test parameter data transform from byte to string. Negative case, byte with value 127 to varchar(2) should fail.
	 */
	@Test
	public void testParam008ByteToString()
	{
		_logger.debug("running testParam008ByteToString");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		byte value = (byte)0x7F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, VARCHAR2_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
		// Test
		StepVerifier.create(mono)
		            .expectError()
		            .verify();
	}


	/**
	 * Test parameter data transform from short to integer.
	 */
	@Test
	public void testParam101ShortToInt()
	{
		_logger.debug("running testParam101ShortToInt");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		short value = (short)0x0F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, INTEGER_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, INTEGER_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	assert row.get("INTEGER_COL", Integer.class) == 15: "Expected value is 15, but received = "+row.get("INTEGER_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from short to long.
	 */
	@Test
	public void testParam102ShortToLong()
	{
		_logger.debug("running testParam102ShortToLong");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		short value = (short)0x0F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, BIGINT_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, BIGINT_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	assert row.get("BIGINT_COL", Long.class) == (long)15: "Expected value is 15, but received = "+row.get("BIGINT_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from short to float.
	 */
	@Test
	public void testParam103ShortToFloat()
	{
		_logger.debug("running testParam103ShortToFloat");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		short value = (short)0x7F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, REAL_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, REAL_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	assert row.get("REAL_COL", Float.class) == 127.0: "Expected value is 127.0, but received = "+row.get("REAL_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from short to double.
	 */
	@Test
	public void testParam104ShortToDouble()
	{
		_logger.debug("running testParam104ShortToDouble");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		short value = (short)0x7F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, DOUBLE_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, DOUBLE_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	assert row.get("DOUBLE_COL", Double.class) == 127.0: "Expected value is 127.0, but received = "+row.get("DOUBLE_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from short to string.
	 */
	@Test
	public void testParam105ShortToString()
	{
		_logger.debug("running testParam105ShortToString");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		short value = (short)0x7F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, VARCHAR10_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, VARCHAR10_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	assert row.get("VARCHAR10_COL", String.class).equals("127"): "Expected value is 127.0, but received = "+row.get("VARCHAR10_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from short to string. Negative case, a 3 digit short value to char should fail.
	 */
	@Test
	public void testParam106ShortToString()
	{
		_logger.debug("running testParam106ShortToString");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		short value = (short)0x7F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, CHAR_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
		// Test
		StepVerifier.create(mono)
		            .expectError()
		            .verify();
	}


	/**
	 * Test parameter data transform from short to string. Negative case, a 3 digit short value to varchar(2) should fail.
	 */
	@Test
	public void testParam107ShortToString()
	{
		_logger.debug("running testParam107ShortToString");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		short value = (short)0x7F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, VARCHAR2_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
		// Test
		StepVerifier.create(mono)
		            .expectError()
		            .verify();
	}


	/**
	 * Test parameter data transform from int to long.
	 */
	@Test
	public void testParam201IntToLong()
	{
		_logger.debug("running testParam201IntToLong");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		int value = 15;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, BIGINT_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, BIGINT_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	assert row.get("BIGINT_COL", Long.class) == (long)15: "Expected value is 15, but received = "+row.get("BIGINT_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from int to float.
	 */
	@Test
	public void testParam202IntToFloat()
	{
		_logger.debug("running testParam202IntToFloat");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		int value = 0x7F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, REAL_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, REAL_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	assert row.get("REAL_COL", Float.class) == 127.0: "Expected value is 127.0, but received = "+row.get("REAL_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from int to double.
	 */
	@Test
	public void testParam203IntToDouble()
	{
		_logger.debug("running testParam203IntToDouble");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		int value = 0x7F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, DOUBLE_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, DOUBLE_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	assert row.get("DOUBLE_COL", Double.class) == 127.0: "Expected value is 127.0, but received = "+row.get("DOUBLE_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from int to string.
	 */
	@Test
	public void testParam204IntToString()
	{
		_logger.debug("running testParam204IntToString");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		int value = 0x7F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, VARCHAR10_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, VARCHAR10_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	assert row.get("VARCHAR10_COL", String.class).equals("127"): "Expected value is 127.0, but received = "+row.get("VARCHAR10_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from int to string. Negative case, a 3 digit int value to char should fail.
	 */
	@Test
	public void testParam205IntToString()
	{
		_logger.debug("running testParam205IntToString");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		int value = 0x7F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, CHAR_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
		// Test
		StepVerifier.create(mono)
		            .expectError()
		            .verify();
	}


	/**
	 * Test parameter data transform from int to string. Negative case, a 3 digit int value to varchar(2) should fail.
	 */
	@Test
	public void testParam206IntToString()
	{
		_logger.debug("running testParam206IntToString");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		int value = 0x7F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, VARCHAR2_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
		// Test
		StepVerifier.create(mono)
		            .expectError()
		            .verify();
	}


	/**
	 * Test parameter data transform from long to float.
	 */
	@Test
	public void testParam301LongToFloat()
	{
		_logger.debug("running testParam301LongToFloat");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		long value = (long)0x7F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, REAL_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, REAL_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	assert row.get("REAL_COL", Float.class) == 127.0: "Expected value is 127.0, but received = "+row.get("REAL_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from long to double.
	 */
	@Test
	public void testParam302LongToDouble()
	{
		_logger.debug("running testParam302LongToDouble");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		long value = (long)0x7F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, DOUBLE_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, DOUBLE_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	assert row.get("DOUBLE_COL", Double.class) == 127.0: "Expected value is 127.0, but received = "+row.get("DOUBLE_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from long to string.
	 */
	@Test
	public void testParam303LongToString()
	{
		_logger.debug("running testParam303LongToString");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		long value = (long)0x7F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, VARCHAR10_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, VARCHAR10_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	assert row.get("VARCHAR10_COL", String.class).equals("127"): "Expected value is 127.0, but received = "+row.get("VARCHAR10_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from long to string. Negative case, a 3 digit long value to char should fail.
	 */
	@Test
	public void testParam304LongToString()
	{
		_logger.debug("running testParam304LongToString");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		long value = (long)0x7F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, CHAR_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
		// Test
		StepVerifier.create(mono)
		            .expectError()
		            .verify();
	}


	/**
	 * Test parameter data transform from long to string. Negative case, a 3 digit long value to varchar(2) should fail.
	 */
	@Test
	public void testParam305LongToString()
	{
		_logger.debug("running testParam305LongToString");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		long value = (long)0x7F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, VARCHAR2_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
		// Test
		StepVerifier.create(mono)
		            .expectError()
		            .verify();
	}


	/**
	 * Test parameter data transform from util date to sql date.
	 */
	@Test
	public void testParam401UtilDateToSqlDate()
	{
		_logger.debug("running testParam401UtilDateToSqlDate");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		java.util.Date date = new java.util.Date();
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, DATE_COL) VALUES (1, :date)")
											       .bind("date", date)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, DATE_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	String sqlDate = new java.sql.Date(date.getTime()).toString();
					 	assert row.get("DATE_COL").toString().equals(sqlDate) : "Expected date is "+sqlDate+", but received = "+row.get("DATE_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from util date to sql time.
	 */
	@Test
	public void testParam402UtilDateToSqlTime()
	{
		_logger.debug("running testParam402UtilDateToSqlTime");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		java.util.Date date = new java.util.Date();
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, TIME_COL) VALUES (1, :date)")
											       .bind("date", date)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, TIME_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	String sqlTime = new java.sql.Time(date.getTime()).toString();
					 	assert row.get("TIME_COL").toString().equals(sqlTime) : "Expected date is "+sqlTime+", but received = "+row.get("TIME_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from util date to sql timestamp.
	 */
	@Test
	public void testParam403UtilDateToSqlTimestamp()
	{
		_logger.debug("running testParam403UtilDateToSqlTimestamp");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		java.util.Date date = new java.util.Date();
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, TIMESTAMP_COL) VALUES (1, :date)")
											       .bind("date", date)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, TIMESTAMP_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	String sqlTimestamp = new java.sql.Timestamp(date.getTime()).toString();
					 	assert row.get("TIMESTAMP_COL").toString().equals(sqlTimestamp) : "Expected date is "+sqlTimestamp+", but received = "+row.get("TIMESTAMP_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from local date to sql date.
	 */
	@Test
	public void testParam501LocalDateToSqlDate()
	{
		_logger.debug("running testParam501LocalDateToSqlDate");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		java.time.LocalDate date = java.time.LocalDate.now();
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, DATE_COL) VALUES (1, :date)")
											       .bind("date", date)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, DATE_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	String sqlDate = java.sql.Date.valueOf(date).toString();
					 	assert row.get("DATE_COL").toString().equals(sqlDate) : "Expected date is "+sqlDate+", but received = "+row.get("DATE_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from local time to sql time.
	 */
	@Test
	public void testParam502LocalDateToSqlTime()
	{
		_logger.debug("running testParam502LocalDateToSqlTime");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		java.time.LocalTime time = java.time.LocalTime.now();
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, TIME_COL) VALUES (1, :time)")
											       .bind("time", time)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, TIME_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	String sqlTime = java.sql.Time.valueOf(time).toString();
					 	assert row.get("TIME_COL").toString().equals(sqlTime) : "Expected date is "+sqlTime+", but received = "+row.get("TIME_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from local date time to sql timestamp.
	 */
	@Test
	public void testParam503LocalDateTimeToSqlTimestamp()
	{
		_logger.debug("running testParam503LocalDateTimeToSqlTimestamp");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		java.time.LocalDateTime dateTime = java.time.LocalDateTime.now();
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, TIMESTAMP_COL) VALUES (1, :datetime)")
											       .bind("datetime", dateTime)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, TIMESTAMP_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	java.sql.Timestamp ts = java.sql.Timestamp.valueOf(dateTime);
					 	assert ts.toLocalDateTime().compareTo(dateTime) == 0 : "Expected timestamp is "+dateTime+", but received "+ts;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from string to short.
	 */
	@Test
	public void testParam601StringToShort()
	{
		_logger.debug("running testParam601StringToShort");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String value = "15";
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, SMALLINT_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, SMALLINT_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	assert row.get("SMALLINT_COL", Short.class) == (short)15: "Expected value is 15, but received = "+row.get("SHORT_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from string to integer.
	 */
	@Test
	public void testParam602StringToInteger()
	{
		_logger.debug("running testParam602StringToInteger");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String value = "15";
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, INTEGER_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, INTEGER_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	assert row.get("INTEGER_COL", Integer.class) == 15: "Expected value is 15, but received = "+row.get("INTEGER_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from string to long.
	 */
	@Test
	public void testParam603StringToLong()
	{
		_logger.debug("running testParam603StringToLong");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String value = "127";
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, BIGINT_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, BIGINT_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	assert row.get("BIGINT_COL", Long.class) == 127: "Expected value is 127, but received = "+row.get("BIGINT_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from string to float.
	 */
	@Test
	public void testParam604StringToFloat()
	{
		_logger.debug("running testParam604StringToFloat");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String value = "127.9";
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, REAL_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, REAL_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	String result = String.format("%.2f", row.get("REAL_COL", Float.class));
					 	assert result.equals("127.90"): "Expected value is 127.90, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from string to double.
	 */
	@Test
	public void testParam605StringToDouble()
	{
		_logger.debug("running testParam605StringToDouble");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String value = "127.999";
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, DOUBLE_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, DOUBLE_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	String result = String.format("%.3f", row.get("DOUBLE_COL", Double.class));
					 	assert result.equals("127.999"): "Expected value is 127.999, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from string to sql date.
	 */
	@Test
	public void testParam606StringToSqlDate()
	{
		_logger.debug("running testParam606StringToSqlDate");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String date = "2022-01-26";
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, DATE_COL) VALUES (1, :date)")
											       .bind("date", date)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, DATE_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	java.sql.Date expected = java.sql.Date.valueOf(date);
					 	assert row.get("DATE_COL").equals(expected) : "Expected date is "+expected+", but received = "+row.get("DATE_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from string to sql time.
	 */
	@Test
	public void testParam607StringToSqlTime()
	{
		_logger.debug("running testParam607StringToSqlTime");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String time = "13:30:59";
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, TIME_COL) VALUES (1, :time)")
											       .bind("time", time)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, TIME_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	java.sql.Time expectedTime = java.sql.Time.valueOf(time);
					 	assert row.get("TIME_COL").equals(expectedTime) : "Expected date is "+expectedTime+", but received = "+row.get("TIME_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test parameter data transform from string to sql timestamp.
	 */
	@Test
	public void testParam608StringToSqlTimestamp()
	{
		_logger.debug("running testParam608StringToSqlTimestamp");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String timestamp = "2022-01-26 13:30:59.123456";
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, TIMESTAMP_COL) VALUES (1, :timestamp)")
											       .bind("timestamp", timestamp)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, TIMESTAMP_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	java.sql.Timestamp expectedValue = java.sql.Timestamp.valueOf(timestamp);
					 	assert row.get("TIMESTAMP_COL").equals(expectedValue) : "Expected date is "+expectedValue+", but received = "+row.get("TIMESTAMP_COL");
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from short to integer.
	 */
	@Test
	public void testResult701ShortToInt()
	{
		_logger.debug("running testResult701ShortToInt");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		short value = (short)0x0F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, SMALLINT_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, SMALLINT_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	int result = row.get("SMALLINT_COL", Integer.class); // result should transform from short to integer
					 	assert result == 15: "Expected value is 15, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from short to long.
	 */
	@Test
	public void testResult702ShortToLong()
	{
		_logger.debug("running testResult702ShortToLong");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		short value = (short)0x0F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, SMALLINT_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, SMALLINT_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	long result = row.get("SMALLINT_COL", Long.class); // result should transform from short to long
					 	assert result == (long)15: "Expected value is 15, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from short to float.
	 */
	@Test
	public void testResult703ShortToFloat()
	{
		_logger.debug("running testResult703ShortToFloat");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		short value = (short)0x0F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, SMALLINT_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, SMALLINT_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	float result = row.get("SMALLINT_COL", Float.class); // result should transform from short to float
					 	assert result == (float)15.0: "Expected value is 15.0, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from short to double.
	 */
	@Test
	public void testResult704ShortToDouble()
	{
		_logger.debug("running testResult704ShortToDouble");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		short value = (short)0x0F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, SMALLINT_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, SMALLINT_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	double result = row.get("SMALLINT_COL", Double.class); // result should transform from short to double
					 	assert result == 15.0: "Expected value is 15.0, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from short to string.
	 */
	@Test
	public void testResult705ShortToString()
	{
		_logger.debug("running testResult705ShortToString");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		short value = (short)0x0F;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, SMALLINT_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, SMALLINT_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	String result = row.get("SMALLINT_COL", String.class); // result should transform from short to string
					 	assert result.equals("15"): "Expected value is 15, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from int to long.
	 */
	@Test
	public void testResult801IntToLong()
	{
		_logger.debug("running testResult801IntToLong");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		int value = 125;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, INTEGER_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, INTEGER_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	long result = row.get("INTEGER_COL", Long.class); // result should transform from integer to long
					 	assert result == (long)125: "Expected value is 125, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from int to float.
	 */
	@Test
	public void testResult802IntToFloat()
	{
		_logger.debug("running testResult802IntToFloat");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		int value = 125;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, INTEGER_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, INTEGER_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	float result = row.get("INTEGER_COL", Float.class); // result should transform from integer to float
					 	assert result == (float)125.0: "Expected value is 125.0, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from int to double.
	 */
	@Test
	public void testResult803IntToDouble()
	{
		_logger.debug("running testResult803IntToDouble");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		int value = 125;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, INTEGER_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, INTEGER_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	double result = row.get("INTEGER_COL", Double.class); // result should transform from integer to double
					 	assert result == (double)125.0: "Expected value is 125.0, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from int to string.
	 */
	@Test
	public void testResult804IntToString()
	{
		_logger.debug("running testResult804IntToString");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		int value = 125;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, INTEGER_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, INTEGER_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	String result = row.get("INTEGER_COL", String.class); // result should transform from integer to string
					 	assert result.equals("125"): "Expected value is 125.0, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from long to float.
	 */
	@Test
	public void testResult901LongToFloat()
	{
		_logger.debug("running testResult901LongToFloat");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		long value = 125;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, BIGINT_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, BIGINT_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	float result = row.get("BIGINT_COL", Float.class); // result should transform from long to float
					 	assert result == (float)125.0: "Expected value is 125.0, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from long to double.
	 */
	@Test
	public void testResult902LongToDouble()
	{
		_logger.debug("running testResult902LongToDouble");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		long value = 125;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, BIGINT_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, BIGINT_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	double result = row.get("BIGINT_COL", Double.class); // result should transform from long to double
					 	assert result == (double)125.0: "Expected value is 125.0, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from long to string.
	 */
	@Test
	public void testResult903LongToString()
	{
		_logger.debug("running testResult903LongToString");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		long value = 125;
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, BIGINT_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, BIGINT_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	String result = row.get("BIGINT_COL", String.class); // result should transform from long to double
					 	assert result.equals("125"): "Expected value is 125.0, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from sql date to local date.
	 */
	@Test
	public void testResult1001SqlDateTolocalDate()
	{
		_logger.debug("running testResult1001SqlDateToLocalDate");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		java.sql.Date date = java.sql.Date.valueOf("2022-01-26"); 
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, DATE_COL) VALUES (1, :date)")
											       .bind("date", date)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, DATE_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	java.time.LocalDate result = row.get("DATE_COL", java.time.LocalDate.class); // result should transform from sql date to local date
					 	java.time.LocalDate expected = date.toLocalDate();
					 	assert result.equals(expected): "Expected value is "+expected+", but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from sql time to local time.
	 */
	@Test
	public void testResult1002SqlTimeToLocalTime()
	{
		_logger.debug("running testResult1002SqlTimeToLocalTime");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		java.sql.Time time = java.sql.Time.valueOf("13:30:59"); 
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, TIME_COL) VALUES (1, :time)")
											       .bind("time", time)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, TIME_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	java.time.LocalTime result = row.get("TIME_COL", java.time.LocalTime.class); // result should transform from sql time to local time
					 	java.time.LocalTime expected = time.toLocalTime();
					 	assert result.equals(expected): "Expected value is "+expected+", but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from sql timestamp to local date time.
	 */
	@Test
	public void testResult1003SqlTimestampToLocalDateTime()
	{
		_logger.debug("running testResult1003SqlTimestampToLocalDateTime");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf("2022-01-26 13:30:59.123456"); 
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, TIMESTAMP_COL) VALUES (1, :timestamp)")
											       .bind("timestamp", timestamp)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, TIMESTAMP_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	java.time.LocalDateTime result = row.get("TIMESTAMP_COL", java.time.LocalDateTime.class); // result should transform from sql timestamp to local date time
					 	java.time.LocalDateTime expected = timestamp.toLocalDateTime();
					 	assert result.equals(expected): "Expected value is "+expected+", but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from sql date to string.
	 */
	@Test
	public void testResult1004SqlDateToString()
	{
		_logger.debug("running testResult1004SqlDateToString");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		java.sql.Date date = java.sql.Date.valueOf("2022-01-26"); 
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, DATE_COL) VALUES (1, :date)")
											       .bind("date", date)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, DATE_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	String result = row.get("DATE_COL", String.class); // result should transform from sql date to string.
					 	assert result.equals("2022-01-26"): "Expected value is 2022-01-26, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from sql time to string.
	 */
	@Test
	public void testResult1005SqlTimeToString()
	{
		_logger.debug("running testResult1005SqlTimeToString");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		java.sql.Time time = java.sql.Time.valueOf("13:30:59"); 
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, TIME_COL) VALUES (1, :time)")
											       .bind("time", time)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, TIME_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	String result = row.get("TIME_COL", String.class); // result should transform from sql time to string
					 	assert result.equals("13:30:59"): "Expected value is 13:30:59, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from sql timestamp to string.
	 */
	@Test
	public void testResult1006SqlTimestampToString()
	{
		_logger.debug("running testResult1006SqlTimestampToString");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf("2022-01-26 13:30:59.123456"); 
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, TIMESTAMP_COL) VALUES (1, :timestamp)")
											       .bind("timestamp", timestamp)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, TIMESTAMP_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	String result = row.get("TIMESTAMP_COL", String.class); // result should transform from sql timestamp to string
					 	assert result.equals("2022-01-26 13:30:59.123456"): "Expected value is 2022-01-26 13:30:59.123456, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from string to short.
	 */
	@Test
	public void testResult1101StringToShort()
	{
		_logger.debug("running testResult1101StringToShort");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String value = "127";
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, VARCHAR10_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, VARCHAR10_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	short result = row.get("VARCHAR10_COL", Short.class); // transform the result in string to a short value
					 	assert result == 127: "Expected value is 127, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from string to short. Negative case. Should get NumberFormatException.
	 */
	@Test
	public void testResult1102StringToShort()
	{
		_logger.debug("running testResult1102StringToShort");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String value = "bad-value";
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, VARCHAR10_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, VARCHAR10_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	short result = -1;
					 	try {
					 		result = row.get("VARCHAR10_COL", Short.class); // should fail with exception
					 		// failed
						 	assert 1 == 0: "Parsed the string 'bad-value' to a short value " + result;
						} catch (Exception e) {
							// pass
						}	 	
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from string to int.
	 */
	@Test
	public void testResult1103StringToInt()
	{
		_logger.debug("running testResult1103StringToInt");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String value = "127";
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, VARCHAR10_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, VARCHAR10_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	int result = row.get("VARCHAR10_COL", Integer.class); // transform the result in string to a int value
					 	assert result == 127: "Expected value is 127, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from string to long.
	 */
	@Test
	public void testResult1104StringToLong()
	{
		_logger.debug("running testResult1104StringToLong");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String value = "127";
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, VARCHAR10_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, VARCHAR10_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	float result = row.get("VARCHAR10_COL", Long.class); // transform the result in string to a long value
					 	assert result == (long)127: "Expected value is 127, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from string to float.
	 */
	@Test
	public void testResult1105StringToFloat()
	{
		_logger.debug("running testResult1105StringToFloat");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String value = "127.99";
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, VARCHAR10_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, VARCHAR10_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	float result = row.get("VARCHAR10_COL", Float.class); // transform the result in string to a float value
					 	assert result == (float)127.99: "Expected value is 127.99, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from string to double.
	 */
	@Test
	public void testResult1106StringToDouble()
	{
		_logger.debug("running testResult1106StringToDouble");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String value = "127.999";
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, VARCHAR10_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, VARCHAR10_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	double result = row.get("VARCHAR10_COL", Double.class); // transform the result in string to a double value
					 	assert result == 127.999: "Expected value is 127.999, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from string to sql date.
	 */
	@Test
	public void testResult1107StringToSqlDate()
	{
		_logger.debug("running testResult1107StringToSqlDate");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String value = "2022-08-15";
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, VARCHAR10_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, VARCHAR10_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	java.sql.Date result = row.get("VARCHAR10_COL", java.sql.Date.class); // transform the result string to sql date 
					 	java.sql.Date expected = java.sql.Date.valueOf(value);
					 	assert result.equals(expected): "Expected value is 2022-08-15, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from string to local date.
	 */
	@Test
	public void testResult1108StringToLocalDate()
	{
		_logger.debug("running testResult1108StringToLocalDate");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String value = "2022-08-15";
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, VARCHAR10_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, VARCHAR10_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	java.time.LocalDate result = row.get("VARCHAR10_COL", java.time.LocalDate.class); // transform the result string to local date 
					 	java.sql.Date sqlDate = java.sql.Date.valueOf(value);
					 	java.time.LocalDate expected = sqlDate.toLocalDate();
					 	assert result.equals(expected): "Expected value is 2022-08-15, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from string to sql time.
	 */
	@Test
	public void testResult1109StringToSqlTime()
	{
		_logger.debug("running testResult1109StringToSqlTime");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String value = "13:30:59";
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, VARCHAR10_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, VARCHAR10_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	java.sql.Time result = row.get("VARCHAR10_COL", java.sql.Time.class); // transform the result string to sql time 
					 	java.sql.Time expected = java.sql.Time.valueOf(value);
					 	assert result.equals(expected): "Expected value is 13:30:59, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from string to local time.
	 */
	@Test
	public void testResult1110StringToLocalTime()
	{
		_logger.debug("running testResult1110StringToLocalTime");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String value = "13:30:59";
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, VARCHAR10_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, VARCHAR10_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	java.time.LocalTime result = row.get("VARCHAR10_COL", java.time.LocalTime.class); // transform the result string to local time 
					 	java.sql.Time sqlTime = java.sql.Time.valueOf(value);
					 	java.time.LocalTime expected = sqlTime.toLocalTime();
					 	assert result.equals(expected): "Expected value is 13:30:59, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from string to sql timestamp.
	 */
	@Test
	public void testResult1111StringToSqlTimestamp()
	{
		_logger.debug("running testResult1111StringToSqlTimestamp");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String value = "2022-01-26 13:30:59.123456";
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, VARCHAR50_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, VARCHAR50_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	java.sql.Timestamp result = row.get("VARCHAR50_COL", java.sql.Timestamp.class); // transform the result string to sql timestamp 
					 	java.sql.Timestamp expected = java.sql.Timestamp.valueOf(value);
					 	assert result.equals(expected): "Expected value is 13:30:59, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}


	/**
	 * Test result set data transform from string to local date time.
	 */
	@Test
	public void testResult1112StringToLocalDateTime()
	{
		_logger.debug("running testResult1112StringToLocalDateTime");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Get connection and insert data
		String value = "2022-01-26 13:30:59.123456";
		Mono<DB2Result> mono = _pool.getConnection()
									.doOnNext(c -> con.set(c))
									.flatMap(c -> c.createStatement("INSERT INTO UNIT_TEST_"+_randomInt+" (ID, VARCHAR50_COL) VALUES (1, :value)")
											       .bind("value", value)
												   .execute())
									.doAfterTerminate(() -> con.get().release());
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
							 .flatMap(c -> c.createStatement("SELECT ID, VARCHAR50_COL FROM UNIT_TEST_"+_randomInt+" WHERE ID = 1")
											 .execute())
							 .flatMapMany(result -> result.map((row, md) -> row))
							 .doAfterTerminate(() -> con.get().release());
	
		// Test
		StepVerifier.create(flux)
					.assertNext(row -> {
					 	assert row != null : "Received a null object";
					 	assert row instanceof Row : "Did not receive a Row object";
					 	assert row.get("ID", Integer.class) == 1 : "Expected ID value is 1, but received = "+row.get("ID");
					 	java.time.LocalDateTime result = row.get("VARCHAR50_COL", java.time.LocalDateTime.class); // transform the result string to local date time 
					 	java.sql.Timestamp sqlTimestamp = java.sql.Timestamp.valueOf(value);
					 	java.time.LocalDateTime expected = sqlTimestamp.toLocalDateTime();
					 	assert result.equals(expected): "Expected value is 2022-01-26 13:30:59.123456, but received = "+result;
					})
					.expectComplete()
					.verify();
	
	}
}