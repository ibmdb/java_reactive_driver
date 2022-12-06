package com.ibm.db2.r2dbc;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import com.ibm.db2.r2dbc.DB2Connection;
import com.ibm.db2.r2dbc.DB2ConnectionConfiguration;
import com.ibm.db2.r2dbc.DB2ConnectionFactory;
import com.ibm.db2.r2dbc.DB2ConnectionPool;
import com.ibm.db2.r2dbc.DB2PreparedStatement;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;


public class ConnectionTests extends BaseTestCase
{
	/**
	 * Test if we can get a connection to a database server.
	 */
	@Test
	public void testGetConnection()
	{
		DB2ConnectionConfiguration config = DB2ConnectionConfiguration.builder()
				.database(_database)
				.host(_host)
				.port(_port)
				.username(_userid)
				.password(_password)
				//.licenseFilePath(_licenseFilePath)
				.securityMechanism(_securityMechanism)
				.kerberosServerPrincipal(_kerberosServerPrincipal)
				.enableSSL(_enableSSL)
				.trustStorePath(_trustStorePath)
				.trustStorePassword(_trustStorePassword)
				.build();
		
		DB2ConnectionFactory factory = new DB2ConnectionFactory(config);

		StepVerifier.create(factory.create())
		            .assertNext(c -> {
		            	assert c != null : "Received a null object";
		            	assert c instanceof DB2Connection : "Did not receive a DB2Connection object";
		            })
		            .expectComplete()
		            .verify();
	}
	
	/**
	 * Test if we can get a connection from the connection pool.
	 */
	@Test
	public void testGetPooledConnection1()
	{
		DB2ConnectionConfiguration config = DB2ConnectionConfiguration.builder()
				.database(_database)
				.host(_host)
				.port(_port)
				.username(_userid)
				.password(_password)
				.conPoolSize(1)
				.stmtCacheSize(10)
				//.licenseFilePath(_licenseFilePath)
				.securityMechanism(_securityMechanism)
				.kerberosServerPrincipal(_kerberosServerPrincipal)
				.enableSSL(_enableSSL)
				.trustStorePath(_trustStorePath)
				.trustStorePassword(_trustStorePassword)
				.build();
		
		DB2ConnectionFactory factory = new DB2ConnectionFactory(config);
		DB2ConnectionPool pool = new DB2ConnectionPool(factory);

		StepVerifier.create(pool.getConnection())
		            .assertNext(c -> {
		            	assert c != null : "Received a null object";
		            	assert c instanceof DB2Connection : "Did not receive a DB2Connection object";
		                c.release();
		            })
		            .expectComplete()
		            .verify();
		
		// close the pool
		pool.closeAll().block();
	}
	
	/**
	 * Test if we can reuse a connection released to the pool.
	 */
	@Test
	public void testGetPooledConnection2()
	{
		DB2ConnectionConfiguration config = DB2ConnectionConfiguration.builder()
				.database(_database)
				.host(_host)
				.port(_port)
				.username(_userid)
				.password(_password)
				.conPoolSize(1)         // max connections
				.stmtCacheSize(10)
				//.licenseFilePath(_licenseFilePath)
				.securityMechanism(_securityMechanism)
				.kerberosServerPrincipal(_kerberosServerPrincipal)
				.enableSSL(_enableSSL)
				.trustStorePath(_trustStorePath)
				.trustStorePassword(_trustStorePassword)
				.build();
		
		DB2ConnectionFactory factory = new DB2ConnectionFactory(config);
		DB2ConnectionPool pool = new DB2ConnectionPool(factory);
		
		// get a pooled connection and release
		StepVerifier.create(pool.getConnection())
		            .assertNext(c -> {
		            	assert c != null : "Received a null object";
		            	assert c instanceof DB2Connection : "Did not receive a DB2Connection object";
		            	c.release();
		            })
		            .expectComplete()
		            .verify();
		
		// attempt to reuse the released connection
		StepVerifier.create(pool.getConnection())
					.assertNext(c -> {
			        	assert c != null : "Received a null object";
			        	assert c instanceof DB2Connection : "Did not receive a DB2Connection object";
			        	c.release();
			        })
			        .expectComplete()
			        .verify();
		
		// close the pool
		pool.closeAll().block();
	}
	
	/**
	 * Test if we can get a 2nd connection from the pool while the 1st one we got is still in use.
	 */
	@Test
	public void testGetPooledConnection3()
	{
		DB2ConnectionConfiguration config = DB2ConnectionConfiguration.builder()
				.database(_database)
				.host(_host)
				.port(_port)
				.username(_userid)
				.password(_password)
				.conPoolSize(2)         // max connections
				.stmtCacheSize(10)
				//.licenseFilePath(_licenseFilePath)
				.securityMechanism(_securityMechanism)
				.kerberosServerPrincipal(_kerberosServerPrincipal)
				.enableSSL(_enableSSL)
				.trustStorePath(_trustStorePath)
				.trustStorePassword(_trustStorePassword)
				.build();
		
		DB2ConnectionFactory factory = new DB2ConnectionFactory(config);
		DB2ConnectionPool pool = new DB2ConnectionPool(factory);

		// get a pooled connection
		StepVerifier.create(pool.getConnection())
		            .assertNext(c -> {
		            	assert c != null : "Received a null object";
		            	assert c instanceof DB2Connection : "Did not receive a DB2Connection object";
		            	// do not release the connection
		            })
		            .expectComplete()
		            .verify();
		
		// get a second connection from pool
		StepVerifier.create(pool.getConnection())
					.assertNext(c -> {
			        	assert c != null : "Received a null object";
			        	assert c instanceof DB2Connection : "Did not receive a DB2Connection object";
			        	c.release();
			        })
			        .expectComplete()
			        .verify();
		
		// close the pool
		pool.closeAll().block();
	}
	
	/**
	 * Test if a request for get connection waits when no connection is available in the pool and when a connection
	 * is released, it gets assigned to the waiting request.
	 */
	@Test
	public void testGetPooledConnection4()
	{
		DB2ConnectionConfiguration config = DB2ConnectionConfiguration.builder()
				.database(_database)
				.host(_host)
				.port(_port)
				.username(_userid)
				.password(_password)
				.conPoolSize(1)         // max connections
				.stmtCacheSize(10)
				//.licenseFilePath(_licenseFilePath)
				.securityMechanism(_securityMechanism)
				.kerberosServerPrincipal(_kerberosServerPrincipal)
				.enableSSL(_enableSSL)
				.trustStorePath(_trustStorePath)
				.trustStorePassword(_trustStorePassword)
				.build();
		
		DB2ConnectionFactory factory = new DB2ConnectionFactory(config);
		DB2ConnectionPool pool = new DB2ConnectionPool(factory);
		
		AtomicReference<Integer> id = new AtomicReference<Integer>(0);

		// get a pooled connection and release after 3 seconnds
		pool.getConnection()
			.doOnNext(c -> {
				id.set(c.getConnectionId());
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				c.release();
			})
			.subscribe();
		
		// try getting a connection, it should wait and then receive the released connection
		StepVerifier.create(pool.getConnection())
		            .assertNext(c -> {
		            	assert c != null : "Received a null object";
		            	assert c instanceof DB2Connection : "Did not receive a DB2Connection object";
		            	assert c.getConnectionId() == id.get() : "Did no receive the released DB2Connection object";
		            	c.release();
		            })
		            .expectComplete()
		            .verify();
		
		// close the pool
		pool.closeAll().block();
	}
	
	/**
	 * Test if we can customize the connection factory to execute set statements.
	 */
	@Test
	public void testGetPooledConnection5()
	{
		DB2ConnectionConfiguration config = DB2ConnectionConfiguration.builder()
				.database(_database)
				.host(_host)
				.port(_port)
				.username(_userid)
				.password(_password)
				.conPoolSize(1)         // max connections
				.stmtCacheSize(10)
				//.licenseFilePath(_licenseFilePath)
				.securityMechanism(_securityMechanism)
				.kerberosServerPrincipal(_kerberosServerPrincipal)
				.enableSSL(_enableSSL)
				.trustStorePath(_trustStorePath)
				.trustStorePassword(_trustStorePassword)
				.build();
		
		// create a custom factory 
		DB2ConnectionFactory factory = new DB2ConnectionFactory(config) {
			// in each connection created by this factory, invoke set current schema
			public Mono<Void> init(DB2Connection con)
			{
				return con.executeUpdate("SET CURRENT SCHEMA "+_userid);
			}
		};
		
		DB2ConnectionPool pool = new DB2ConnectionPool(factory);	

		StepVerifier.create(pool.getConnection())
			        .assertNext(c -> {
			        	assert c != null : "Received a null object";
			        	assert c instanceof DB2Connection : "Did not receive a DB2Connection object";
			            c.release();
			        })
			        .expectComplete()
			        .verify();
		
		// close the pool
		pool.closeAll().block();
	} 
	
	/**
	 * Test if connections are recreated after a pooled connection is closed.
	 */
	@Test
	public void testGetPooledConnection6()
	{
		DB2ConnectionConfiguration config = DB2ConnectionConfiguration.builder()
				.database(_database)
				.host(_host)
				.port(_port)
				.username(_userid)
				.password(_password)
				.conPoolSize(1)         // max connections
				.stmtCacheSize(10)
				//.licenseFilePath(_licenseFilePath)
				.securityMechanism(_securityMechanism)
				.kerberosServerPrincipal(_kerberosServerPrincipal)
				.enableSSL(_enableSSL)
				.trustStorePath(_trustStorePath)
				.trustStorePassword(_trustStorePassword)
				.build();
		
		DB2ConnectionFactory factory = new DB2ConnectionFactory(config);	
		DB2ConnectionPool pool = new DB2ConnectionPool(factory);	

		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// get a connection from pool
		StepVerifier.create(pool.getConnection())
			        .assertNext(c -> {
			        	assert c != null : "Received a null object";
			        	assert c instanceof DB2Connection : "Did not receive a DB2Connection object";
			            con.set(c);
			        })
			        .expectComplete()
			        .verify();
		
		// close the connection
		con.get().close().block();
		
		// as max pool is 1, now no connections exist in the pool
		// see if we can get a new connection
		StepVerifier.create(pool.getConnection())
		        .assertNext(c -> {
		        	assert c != null : "Received a null object";
		        	assert c instanceof DB2Connection : "Did not receive a DB2Connection object";
		            c.release();
		        })
		        .expectComplete()
		        .verify();
		
		// close the pool
		pool.closeAll().block();
	} 
	
	/**
	 * Test if connections are recreated after a pooled connection is closed.
	 */
	@Test
	public void testGetPooledConnection7()
	{
		DB2ConnectionConfiguration config = DB2ConnectionConfiguration.builder()
				.database(_database)
				.host(_host)
				.port(_port)
				.username(_userid)
				.password(_password)
				.conPoolSize(2)         // max connections
				.stmtCacheSize(10)
				//.licenseFilePath(_licenseFilePath)
				.securityMechanism(_securityMechanism)
				.kerberosServerPrincipal(_kerberosServerPrincipal)
				.enableSSL(_enableSSL)
				.trustStorePath(_trustStorePath)
				.trustStorePassword(_trustStorePassword)
				.build();
		
		DB2ConnectionFactory factory = new DB2ConnectionFactory(config);	
		DB2ConnectionPool pool = new DB2ConnectionPool(factory);	
		
		DB2Connection con1 = pool.getConnection().block();
		DB2Connection con2 = pool.getConnection().block();
		con2.close().block();
		
		// as max pool is 2, now no free connections exist in the pool
		// see if we can get a new connection
		StepVerifier.create(pool.getConnection())
			        .assertNext(c -> {
			        	assert c != null : "Received a null object";
			        	assert c instanceof DB2Connection : "Did not receive a DB2Connection object";
			            c.release();
			        })
			        .expectComplete()
			        .verify();
		
		// close the pool
		pool.closeAll().block();
	} 
	
	/**
	 * Test if the recreated connection is usable.
	 */
	@Test
	public void testGetPooledConnection8()
	{
		DB2ConnectionConfiguration config = DB2ConnectionConfiguration.builder()
				.database(_database)
				.host(_host)
				.port(_port)
				.username(_userid)
				.password(_password)
				.conPoolSize(2)         // max connections
				.stmtCacheSize(10)
				//.licenseFilePath(_licenseFilePath)
				.securityMechanism(_securityMechanism)
				.kerberosServerPrincipal(_kerberosServerPrincipal)
				.enableSSL(_enableSSL)
				.trustStorePath(_trustStorePath)
				.trustStorePassword(_trustStorePassword)
				.build();
		
		DB2ConnectionFactory factory = new DB2ConnectionFactory(config);	
		DB2ConnectionPool pool = new DB2ConnectionPool(factory);	
		
		DB2Connection con1 = pool.getConnection().block();
		DB2Connection con2 = pool.getConnection().block();
		con2.close().block();
		
		// as max pool is 2, now no free connections exist in the pool
		// see if we can get a new connection and is usable
		Mono<Void> mono = pool.getConnection()
							.flatMap(c -> c.executeUpdate("SET CURRENT SCHEMA "+_userid));
		
		StepVerifier.create(mono)
			        .expectComplete()
			        .verify();
		
		// close the pool
		pool.closeAll().block();
	} 
	
	/**
	 * Test if connection creation errors are correctly propagated to the called user.
	 */
	@Test
	public void testGetPooledConnection9()
	{
		//this test is applicable only for userid passowrd security mechanism
		if (_securityMechanism != DB2ConnectionConfiguration.USERID_PASSWORD) {
			return;
		}
		
		DB2ConnectionConfiguration config = DB2ConnectionConfiguration.builder()
				.database(_database)
				.host(_host)
				.port(_port)
				.username(_userid)
				.password("wrong-passord")
				.conPoolSize(2)         // max connections
				.stmtCacheSize(10)
				//.licenseFilePath(_licenseFilePath)
				.securityMechanism(_securityMechanism)
				.kerberosServerPrincipal(_kerberosServerPrincipal)
				.enableSSL(_enableSSL)
				.trustStorePath(_trustStorePath)
				.trustStorePassword(_trustStorePassword)
				.build();
		
		DB2ConnectionFactory factory = new DB2ConnectionFactory(config);	
		DB2ConnectionPool pool = new DB2ConnectionPool(factory);	
		
		Mono<DB2Connection> mono = pool.getConnection();
		
		StepVerifier.create(mono)
        			.expectError()
        			.verify();
		
		// close the pool
		pool.closeAll().block();
	} 
	
	/**
	 * Test if connection creation errors are correctly propagated to the called user.
	 */
	@Test
	public void testGetPooledConnection10()
	{
		//this test is applicable only for userid passowrd security mechanism
		if (_securityMechanism != DB2ConnectionConfiguration.USERID_PASSWORD) {
			return;
		}
		
		DB2ConnectionConfiguration config = DB2ConnectionConfiguration.builder()
				.database(_database)
				.host(_host)
				.port(_port)
				.username(_userid)
				.password("wrong-passord")
				.conPoolSize(2)         // max connections
				.stmtCacheSize(10)
				//.licenseFilePath(_licenseFilePath)
				.securityMechanism(_securityMechanism)
				.kerberosServerPrincipal(_kerberosServerPrincipal)
				.enableSSL(_enableSSL)
				.trustStorePath(_trustStorePath)
				.trustStorePassword(_trustStorePassword)
				.build();
		
		DB2ConnectionFactory factory = new DB2ConnectionFactory(config);	
		DB2ConnectionPool pool = new DB2ConnectionPool(factory);	
		
		// 1st attempt
		Mono<DB2Connection> mono = pool.getConnection();
		StepVerifier.create(mono)
        			.expectError()
        			.verify();
		
		// 2nd attempt
		mono = pool.getConnection();
		StepVerifier.create(mono)
        			.expectError()
        			.verify();
		
		// max pool size is 2, still any furter attemps should get the correct error
		mono = pool.getConnection();
		StepVerifier.create(mono)
        			.expectError()
        			.verify();
		
		// max pool size 2, still any furter attemps should get the correct error
		mono = pool.getConnection();
		StepVerifier.create(mono)
        			.expectError()
        			.verify();
		
		// close the pool
		pool.closeAll().block();
	} 
}
