package com.ibm.db2.r2dbc;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.ibm.db2.r2dbc.DB2Connection;
import com.ibm.db2.r2dbc.DB2ConnectionConfiguration;
import com.ibm.db2.r2dbc.DB2ConnectionFactory;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/*
 * We use fixed ordering of test execution, so note that the tests are named test1X, test2X etc..,
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DDLTests extends BaseTestCase
{	
	@Test  
	public void test1CreateTable1()
	{
		// autocommit ON
		Mono<Void> mono = getNewConnection()
								.flatMap(c -> c.executeUpdate("CREATE TABLE UNIT_TEST_1_"+_randomInt+" (id integer)"));
		
		StepVerifier.create(mono)
		            .expectComplete()
		            .verify();	
	}
	
	@Test
	public void test2CreateTable2()
	{
		// autocommit OFF
		AtomicReference<DB2Connection> conn = new AtomicReference<DB2Connection>(null);
		Mono<Void> mono1 = getNewConnection()
								.doOnNext(c -> {conn.set(c); c.beginTransaction();})
								.flatMap(c -> c.executeUpdate("CREATE TABLE UNIT_TEST_2_"+_randomInt+" (id integer)"));
		
		StepVerifier.create(mono1)
		            .expectComplete()
		            .verify();
		
		Mono<Void> mono2 = Mono.just(conn.get())
								.flatMap(c -> c.commitTransaction());
		
		StepVerifier.create(mono2)
					.expectComplete()
					.verify();							
	}
	
	@Test  
	public void test3DropTable1()
	{
		// autocommit ON
		Mono<Void> mono = getNewConnection()
								.flatMap(c -> c.executeUpdate("DROP TABLE UNIT_TEST_1_"+_randomInt));
		
		StepVerifier.create(mono)
		            .expectComplete()
		            .verify();	
	}
	
	@Test
	public void test4DropTable2()
	{
		// autocommit OFF
		AtomicReference<DB2Connection> conn = new AtomicReference<DB2Connection>(null);
		Mono<Void> mono1 = getNewConnection()
								.doOnNext(c -> {conn.set(c); c.beginTransaction();})
								.flatMap(c -> c.executeUpdate("DROP TABLE UNIT_TEST_2_"+_randomInt));
		
		StepVerifier.create(mono1)
		            .expectComplete()
		            .verify();
		
		Mono<Void> mono2 = Mono.just(conn.get())
								.flatMap(c -> c.commitTransaction());
		
		StepVerifier.create(mono2)
					.expectComplete()
					.verify();							
	}
}
