
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import com.ibm.db2.r2dbc.DB2Connection;
import com.ibm.db2.r2dbc.DB2ConnectionConfiguration;
import com.ibm.db2.r2dbc.DB2ConnectionFactory;
import com.ibm.db2.r2dbc.DB2ConnectionPool;
import com.ibm.db2.r2dbc.DB2Result;
import io.r2dbc.spi.Row;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;


public class SampleApp 
{
	// points to a config file 
	protected static Properties _config = new Properties();
	
	// properties read from a config file
	protected static String _database;
	protected static String _host;
	protected static int _port;
	protected static String _userid;
	protected static String _password;
	protected static boolean _keepDynamic;
	protected static int _conPoolSize;
	protected static int _stmtCacheSize;
	protected static int _securityMechanism;
	protected static String _kerberosServerPrincipal;
	protected static boolean _enableSSL;
	protected static String _trustStorePath;
	protected static String _trustStorePassword;
	
	// reference to a connection pool
	protected static DB2ConnectionPool _pool;
	
	
	/**
	 * Reads the configuration file for connection properties.
	 * 
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void readConfig() throws FileNotFoundException, IOException
	{
		System.out.println("readConfig() called");
		
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
		
		System.out.println("database: "+_database+", host: "+_host+", port: "+_port+", user: "+_userid+", keep_dynamic: "+_keepDynamic);
	}
	
	/**
	 * Sets up a connection pool.
	 */
	public static void setupConnectionPool()
	{
		System.out.println("setupConnectionPool() called");
		
		DB2ConnectionConfiguration config = DB2ConnectionConfiguration.builder()
				.database(_database)
				.host(_host)
				.port(_port)
				.username(_userid)
				.password(_password)
				.conPoolSize(_conPoolSize)
				.stmtCacheSize(_stmtCacheSize)
				.keepDynamic(_keepDynamic)
				.securityMechanism(_securityMechanism)
				.kerberosServerPrincipal(_kerberosServerPrincipal)
				.enableSSL(_enableSSL)
				.trustStorePath(_trustStorePath)
				.trustStorePassword(_trustStorePassword)
				.build();
		
		DB2ConnectionFactory factory = new DB2ConnectionFactory(config);	
		_pool = new DB2ConnectionPool(factory);
	}
	
	
	/**
	 * Creates the DEMO table.
	 * 
	 */
	public Mono<Void> createTable()
	{
		System.out.println("createTable() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		Mono<Void> mono = _pool.getConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.executeUpdate(
					"CREATE TABLE DEMO (" +
						"ID INTEGER UNIQUE NOT NULL, " +
						"ACC_BAL DECIMAL(10,2) NOT NULL, " +
						"AS_OF TIMESTAMP NOT NULL" +
					")"
			))
			.doOnSuccess(s -> System.out.println("Table created."))
			.doOnError(e -> System.err.println("ERROR: Failed to create table.\n"+e))
			.doOnTerminate(() -> {if (con.get() != null) con.get().release();});
		
		return mono;
	}
	

	/**
	 * Drop the DEMO table.
	 * 
	 */
	public Mono<Void> dropTable()
	{
		System.out.println("dropTable() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		Mono<Void> mono = _pool.getConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.executeUpdate("DROP TABLE DEMO"))
			.doOnSuccess(s -> System.out.println("Table dropped."))
			.doOnError(e -> System.err.println("ERROR: Failed to drop table.\n"+e))
			.doOnTerminate(() -> con.get().release());
		
		return mono;
	}
	
	
	/**
	 * Inserts a row of data in DEMO table.
	 * 
	 * @param id - account id, integer
	 * @param amount - initial ammount, BigDecimal
	 * @return Mono<Result> - insert return value
	 */
	public Mono<DB2Result> createAccount(int id, BigDecimal amount)
	{
		System.out.println("createAccount() called, id: "+id+", amount: "+amount);
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		Mono<DB2Result> mono = _pool.getConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.createStatement("INSERT INTO DEMO (ID, ACC_BAL, AS_OF) VALUES (?, ?, ?)")
				   		  	.bind(1, id)
				   		  	.bind(2, amount)
				   		  	.bind(3, Timestamp.from(Instant.now()))
				   		  	.execute())
			.doOnSuccess(s -> System.out.println("Account created."))
			.doOnError(e -> System.err.println("ERROR: Failed to create account.\n"+e))
			.doAfterTerminate(() -> con.get().release());
		
		return mono;	
	}
	
	
	/**
	 * Updates two rows in DEMO table as a transaction.
	 * 
	 * @param src - account id
	 * @param amount - tranfer ammount, BigDecimal
	 * @param dest - account id
	 * @return Mono<Result> - insert return value
	 */
	public Mono<Void> transferFunds(int src, BigDecimal amount, int dest)
	{
		System.out.println("transferFunds() called, src: "+src+", amount: "+amount+", dest: "+dest);
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		return Mono.<Void>create(sink -> {		
			_pool.getConnection()
				.doOnNext(c -> {
					con.set(c); 
					c.beginTransaction();
				})
				.flatMap(c -> c.createStatement("UPDATE DEMO SET ACC_BAL = ACC_BAL - ?, AS_OF = ? WHERE ID = ?")
					   		  	.bind(1, amount)
					   		  	.bind(2, Timestamp.from(Instant.now()))
					   		  	.bind(3, src)
					   		  	.execute())
				.flatMap(r -> r.getNumRowsUpdated()!= 1? Mono.error(new Exception("Debit Failed")) : Mono.just(r))
				.flatMap(r -> con.get().createStatement("UPDATE DEMO SET ACC_BAL = ACC_BAL + ?, AS_OF = ? WHERE ID = ?")
					   		  	.bind(1, amount)
					   		  	.bind(2, Timestamp.from(Instant.now()))
					   		  	.bind(3, dest)
					   		  	.execute())
				.flatMap(r -> r.getNumRowsUpdated()!= 1? Mono.error(new Exception("Credit Failed")) : Mono.just(r))
				.doOnSuccess(r -> {
					con.get()
					   .commitTransaction()
					   .doOnSuccess(s -> {
						   con.get().release();
						   System.out.println("Updated records.");
						   sink.success();
					   })
					   .doOnError(e -> {
						   con.get().release();
						   System.out.println("ERROR: Commit failed.\n"+e);
						   sink.error(e);
					   })
					   .subscribe();
				})
				.doOnError(e -> {
					System.err.println("ERROR: Failed to update records.\n"+e);
					con.get()
					   .rollbackTransaction()
					   .doOnSuccess(s -> {
						   con.get().release();				   
						   sink.error(e);
					   })
					   .doOnError(e2 -> {
						   con.get().release();
						   System.err.println("ERROR: Rollback Failed.\n"+e2);
						   sink.error(e2);
					   })
					   .subscribe();
				})
				.subscribe();
		});
	}
	
	
	/**
	 * Selects two rows from the table DEMO and print their column values.
	 * 
	 * @param src - account id, integer
	 * @param dest - account id, integer
	 */
	public Mono<Void> queryAccounts(int src, int dest)
	{
		System.out.println("queryAccounts() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Query the inserted data and then release the connection
		Flux<Row> flux = _pool.getConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.createStatement("SELECT ID, ACC_BAL, AS_OF FROM DEMO WHERE ID IN (?, ?)")
							.bind(1, src)
							.bind(2, dest)
							.execute())
			.flatMapMany(result -> result.map((row, md) -> row))
			.doOnNext(row -> System.out.println("ID: "+row.get("ID")+", ACC_BAL: "+row.get("ACC_BAL")+", AS_OF: "+row.get("AS_OF")))
			.doOnError(e -> System.err.println("ERROR: Failed to querying account details.\n"+e))
			.doAfterTerminate(() -> con.get().release());
		
		return flux.then();
	}
	
	
	/**
	 * Delete all rows in DEMO table.
	 * 
	 * @param id - account id, integer
	 * @param bdec - initial ammount, BigDecimal
	 * @return Mono<Result> - insert return value
	 */
	public Mono<Void> deleteAccounts()
	{
		System.out.println("deleteAccounts() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		Mono<Void> mono = _pool.getConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.executeUpdate("DELETE FROM DEMO VALUES"))
			.doOnSuccess(s -> System.out.println("Deleted accounts."))
			.doOnError(e -> System.err.println("ERROR: Failed to delete accounts.\n"+e))
			.doAfterTerminate(() -> con.get().release());
		
		return mono;	
	}
	
	/**
	 * Runs the SampleApp.
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String args[]) throws Exception
	{
		// debug flags
		// System.setProperty("sun.security.krb5.debug","true");
	    System.setProperty("javax.net.debug", "ssl,handshake");
		
		readConfig();			
		setupConnectionPool();  
		
		Mono<Void> mono = Mono.just(new SampleApp())
			.delayUntil(d -> d.createTable())
			.delayUntil(d -> d.createAccount(1, new BigDecimal("1000.00")))
			.delayUntil(d -> d.createAccount(2, new BigDecimal("499.99")))
			.delayUntil(d -> d.transferFunds(1, new BigDecimal("100.01"), 2))
			.delayUntil(d -> d.queryAccounts(1, 2))
			.delayUntil(d -> d.deleteAccounts())
			.delayUntil(d -> d.dropTable())
			.doOnError(e -> System.err.println(e.toString()))
			.then();
		
		System.out.println("Sample Application is starting...");
		
		mono.block();   // subscribe and wait till this mono completes	
	}
}
