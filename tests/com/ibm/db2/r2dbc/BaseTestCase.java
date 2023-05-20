package com.ibm.db2.r2dbc;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.db2.r2dbc.DB2Connection;
import com.ibm.db2.r2dbc.DB2ConnectionConfiguration;
import com.ibm.db2.r2dbc.DB2ConnectionFactory;
import com.ibm.db2.r2dbc.DB2ConnectionPool;

import io.r2dbc.spi.Row;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public abstract class BaseTestCase 
{
	private static final Logger _baselogger =  LoggerFactory.getLogger(BaseTestCase.class.getName());
	
	protected static Properties _config = new Properties();
	
	protected static String _database;
	protected static String _host;
	protected static int _port;
	protected static String _userid;
	protected static String _password;
	protected static int _conPoolSize;
	protected static int _stmtCacheSize;
	protected static boolean _keepDynamic;
	protected static int _securityMechanism;
	protected static String _kerberosServerPrincipal;
	protected static boolean _enableSSL;
	protected static String _trustStorePath;
	protected static String _trustStorePassword;
	protected static String _licenseFilePath;
	protected static boolean _isLUW;
	
	protected static int _randomInt;
	
	protected static DB2ConnectionPool _pool;
	
	// used by failing tests
	protected DB2Connection _con = null;
	
	@BeforeClass
	public static void setup() throws FileNotFoundException, IOException
	{
		_baselogger.debug("setup() called");
		
		// load db connection credentials
		String configPath = Thread.currentThread().getContextClassLoader().getResource("config.properties").getPath();
		_config.load(new FileInputStream(configPath));	
		
		_database = _config.getProperty("database").trim();
		_host = _config.getProperty("host").trim();
		_port = Integer.parseInt(_config.getProperty("port").trim());
		_userid = _config.getProperty("userid").trim();
		_password = _config.getProperty("password").trim();
	
		String value = _config.getProperty("keepDynamic");
		if (value != null && value.trim().equalsIgnoreCase("true")) {
			_keepDynamic = true;
		} else {
			_keepDynamic = false;
		}
		
		value = _config.getProperty("conPoolSize");
		if (value == null) {
			_conPoolSize = 10;
		} else {
			_conPoolSize = Integer.parseInt(value.trim());
		}
		
		value = _config.getProperty("stmtCacheSize");
		if (value == null) {
			_stmtCacheSize = 10;
		} else {
			_stmtCacheSize = Integer.parseInt(value.trim());
		}
		
		value = _config.getProperty("securityMechanism");
		if ((value != null) && (value.trim().equalsIgnoreCase("Kerberos"))) {
			_securityMechanism = DB2ConnectionConfiguration.KERBEROS_SECURITY;
		} else {
			// defaults to DB2ConnectionConfiguration.USERID_PASSWORD
			_securityMechanism = DB2ConnectionConfiguration.USERID_PASSWORD;	
		}
		_kerberosServerPrincipal = _config.getProperty("kerberosServerPrincipal");
		
		// SSL
		value = _config.getProperty("enableSSL");
		if (value != null && value.trim().equalsIgnoreCase("true")) {
			_enableSSL = true;
		} else {
			_enableSSL = false;
		}
		value = _config.getProperty("trustStorePath");
		if (value != null) {
			_trustStorePath = value.trim();
		}
		value = _config.getProperty("trustStorePassword");
		if (value != null) {
			_trustStorePassword = value.trim();
		}
				
		value = _config.getProperty("licenseFile");
		if (value != null) {
			_licenseFilePath = value.trim();
		}
		
		value = _config.getProperty("type");
		if (value != null && value.trim().equalsIgnoreCase("LUW")) {
			_isLUW = true;
		} else {
			_isLUW = false;
		}
		
		// create a rondom number for use in the tests
		Random random = new Random();
		_randomInt = random.nextInt(1000);
		
		setupConnectionPool();
	}
	
	@AfterClass
	public static void cleanup()
	{
		_baselogger.debug("cleanup() called");
		
		// close the connections in the pool
		_pool.closeAll().block();
		
		// drop all the tables we created
		dropTable();
	}
	
	// Use the pooled connection in the tests as much as possible
	// These connections can be cleaned at the end in the cleanup method
	// And the drop table statements in cleanup() method will not fail
	public static void setupConnectionPool()
	{
		_baselogger.debug("setupConnectionPool() called");
		
		DB2ConnectionConfiguration config = DB2ConnectionConfiguration.builder()
				.database(_database)
				.host(_host)
				.port(_port)
				.username(_userid)
				.password(_password)
				.conPoolSize(_conPoolSize)
				.stmtCacheSize(_stmtCacheSize)
				.keepDynamic(_keepDynamic)
				//.licenseFilePath(_licenseFilePath)
				.securityMechanism(_securityMechanism)
				.kerberosServerPrincipal(_kerberosServerPrincipal)
				.enableSSL(_enableSSL)
				.trustStorePath(_trustStorePath)
				.trustStorePassword(_trustStorePassword)
				.build();
		
		DB2ConnectionFactory factory = new DB2ConnectionFactory(config);	
		_pool = new DB2ConnectionPool(factory);
	}
	
	// get a new connection everytime
	// remember to close this connection everytime!
	// otherwise cleaup() method will fail!
	public static Mono<DB2Connection> getNewConnection()
	{
		_baselogger.debug("getNewConnection() called");
		
		DB2ConnectionConfiguration config = DB2ConnectionConfiguration.builder()
				.database(_database)
				.host(_host)
				.port(_port)
				.username(_userid)
				.password(_password)
				.conPoolSize(0)
				.stmtCacheSize(0)
				.keepDynamic(_keepDynamic)
				//.licenseFilePath(_licenseFilePath)
				.securityMechanism(_securityMechanism)
				.kerberosServerPrincipal(_kerberosServerPrincipal)
				.enableSSL(_enableSSL)
				.trustStorePath(_trustStorePath)
				.trustStorePassword(_trustStorePassword)
				.build();
		
		DB2ConnectionFactory factory = new DB2ConnectionFactory(config);	
		return factory.create();
	}
	
	public static void dropTable()
	{
		_baselogger.debug("dropTable() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// get list of all tables with prefix "UNIT_TEST_"
		ArrayList<String> tables = new ArrayList<String>();
		getNewConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.createStatement("SELECT NAME FROM SYSIBM.SYSTABLES WHERE CREATOR = ? AND NAME LIKE 'UNIT_TEST_%'")
						   .bind(1, _userid.toUpperCase())
						   .execute())
			.flatMapMany(result -> result.map((row, md) -> row))		
			.doOnNext(row -> tables.add(row.get("NAME").toString()))
			.doOnError(e -> _baselogger.error("Error getting table names to drop : "+e))
			.then().block();

		// drop the tables in the list
		Mono.<Void>create(sink -> {
			for (String t : tables) {
				con.get().executeUpdate("DROP TABLE "+t)
				         .doOnError(e -> sink.error(e))
				         .then().block();
			}
			sink.success();
		})
		.doOnTerminate(() -> {
			// close the connection
			con.get().close().block();
		})
		.then().block();
	}
	
	@Before
	public void perTestSetup()
	{
		_baselogger.debug("perTestSetup() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// get list of all tables with prefix "UNIT_TEST_"
		ArrayList<String> tables = new ArrayList<String>();
		_pool.getConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.createStatement("SELECT NAME FROM SYSIBM.SYSTABLES WHERE CREATOR = ? AND NAME LIKE 'UNIT_TEST_%'")
						   .bind(1, _userid.toUpperCase())
						   .execute())
			.flatMapMany(result -> result.map((row, md) -> row))		
			.doOnNext(row -> tables.add(row.get("NAME").toString()))
			.doOnError(e -> _baselogger.error("Error getting table names to clear : "+e))
			.then().block();
				
		// delete all the rows in each of these tables
		Mono.<Void>create(sink -> {
			for (String t : tables) {
				con.get().executeUpdate("DELETE FROM "+t+" WHERE 1 = 1")
						 //.execute()
				         .doOnError(e -> sink.error(e))
				         .then().block();
			}
			sink.success();
		})
		.doOnTerminate(() -> {
			// release the connection we used
			con.get().release();
		})
		.then().block();
		
		// init the _con reference
		_con = null;
	}
	
	@After
	public void perTestCleanup()
	{
		_baselogger.debug("perTestCleanup() called");
		
		// failing tests may leave the connection open, close them
		if (_con != null) {
			_con.close().block();
		}
	}
}
