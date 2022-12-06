
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import com.ibm.db2.r2dbc.DB2Blob;
import com.ibm.db2.r2dbc.DB2Connection;
import com.ibm.db2.r2dbc.DB2ConnectionConfiguration;
import com.ibm.db2.r2dbc.DB2ConnectionFactory;
import com.ibm.db2.r2dbc.DB2ConnectionPool;
import com.ibm.db2.r2dbc.DB2Result;
import io.r2dbc.spi.Row;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Sample application that demonstrates BLOB support for reactive applications.
 *
 */
public class BlobSample 
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
		
		_database = _config.getProperty("database");
		_host = _config.getProperty("host");
		_port = Integer.parseInt(_config.getProperty("port"));
		_userid = _config.getProperty("userid");
		_password = _config.getProperty("password");
	
		String value = _config.getProperty("keepDynamic");
		if (value != null && value.equalsIgnoreCase("true")) {
			_keepDynamic = true;
		} else {
			_keepDynamic = false;
		}
		
		value = _config.getProperty("conPoolSize");
		if (value == null) {
			_conPoolSize = 10;
		} else {
			_conPoolSize = Integer.parseInt(value);
		}
		
		value = _config.getProperty("stmtCacheSize");
		if (value == null) {
			_stmtCacheSize = 10;
		} else {
			_stmtCacheSize = Integer.parseInt(value);
		}
		
		value = _config.getProperty("securityMechanism");
		if ((value != null) && (value.equalsIgnoreCase("Kerberos"))) {
			_securityMechanism = DB2ConnectionConfiguration.KERBEROS_SECURITY;
		} else {
			// defaults to DB2ConnectionConfiguration.USERID_PASSWORD
			_securityMechanism = DB2ConnectionConfiguration.USERID_PASSWORD;	
		}	
		_kerberosServerPrincipal = _config.getProperty("kerberosServerPrincipal");
		
		// SSL
		value = _config.getProperty("enableSSL");
		if (value != null && value.equalsIgnoreCase("true")) {
			_enableSSL = true;
		} else {
			_enableSSL = false;
		}
		value = _config.getProperty("trustStorePath");
		if (value != null) {
			_trustStorePath = value;
		}
		value = _config.getProperty("trustStorePassword");
		if (value != null) {
			_trustStorePassword = value;
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
	 * Creates the BLOB_SAMPLE table.
	 * 
	 */
	public Mono<Void> createTable()
	{
		System.out.println("createTable() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		Mono<Void> mono = _pool.getConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.executeUpdate(
					"CREATE TABLE BLOB_SAMPLE (" +
						"ID INTEGER UNIQUE NOT NULL, " +
						"DATA1 BLOB NOT NULL," +
						"DATA2 BLOB(10M) NOT NULL" +
					")"
			))
			.doOnSuccess(s -> System.out.println("Table created."))
			.doOnError(e -> System.err.println("ERROR: Failed to create table.\n"+e))
			.doOnTerminate(() -> {if (con.get() != null) con.get().release();});
		
		return mono;
	}
	

	/**
	 * Drop the BLOB_SAMPLE table.
	 * 
	 */
	public Mono<Void> dropTable()
	{
		System.out.println("dropTable() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		Mono<Void> mono = _pool.getConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.executeUpdate("DROP TABLE BLOB_SAMPLE"))
			.doOnSuccess(s -> System.out.println("Table dropped."))
			.doOnError(e -> System.err.println("ERROR: Failed to drop table.\n"+e))
			.doOnTerminate(() -> con.get().release());
		
		return mono;
	}
	
	
	/**
	 * Inserts a row of data in the table with BLOB column.
	 * 
	 * @param id - unique id
	 * @return Mono<Result> - insert return value
	 */
	public Mono<DB2Result> insertRow(int id)
	{
		System.out.println("insertRow() called, id: "+id);
		
		// data for DATA1 column 
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 1024; i++) {
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
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		AtomicReference<DB2Blob> blob1 = new AtomicReference<DB2Blob>();
		AtomicReference<DB2Blob> blob2 = new AtomicReference<DB2Blob>();
		
		Mono<DB2Result> mono = _pool.getConnection()
			.doOnNext(c -> {
				con.set(c);
				blob1.set(c.createDB2Blob(sb.toString().getBytes()));
				blob2.set(c.createDB2Blob(flux)); 
			})
			.flatMap(c -> c.createStatement("INSERT INTO BLOB_SAMPLE (ID, DATA1, DATA2) VALUES (?, ?, ?)")
				   		  	.bind(1, id)
				   		  	.bind(2, blob1.get())
				   		  	.bind(3, blob2.get())
				   		  	.execute())
			.doOnSuccess(s -> System.out.println("Row inserted."))
			.doOnError(e -> System.err.println("ERROR: Failed to insert a row.\n"+e))
			.doAfterTerminate(() -> con.get().release());
		
		return mono;	
	}
	

	/**
	 * Query the BLOB data given the corresponding id.
	 * 
	 * @param id - identifies the specific row.
	 */
	public Mono<Void> queryRow(int id)
	{
		System.out.println("queryRow() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		// Query the inserted data and then release the connection
		Flux<Row> flux = _pool.getConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.createStatement("SELECT ID, DATA1, DATA2 FROM BLOB_SAMPLE WHERE ID = ?")
							.bind(1, id)
							.execute())
			.flatMapMany(result -> result.map((row, md) -> row))
			.doOnNext(row -> {
				System.out.println("ID: "+row.get("ID"));
				DB2Blob blob1 = (DB2Blob) row.get("DATA1");
				blob1.stream().subscribe(b -> {
					System.out.println("DATA1: "+new String(b.array())); 
				});
				DB2Blob blob2 = (DB2Blob) row.get("DATA2");
				blob2.stream().subscribe(b -> {
					System.out.println("DATA2: "+new String(b.array()));
				});
			})
			.doOnComplete(() -> System.out.println("Query completed."))
			.doOnError(e -> System.err.println("ERROR: Failed to query blob data.\n"+e))
			.doAfterTerminate(() -> {
				con.get().release();
			});
		
		return flux.then();
	}
	
	
	/**
	 * Delete all rows in BLOB_SAMPLE table.
	 * 
	 */
	public Mono<Void> deleteRows()
	{
		System.out.println("deleteRows() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		Mono<Void> mono = _pool.getConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.executeUpdate("DELETE FROM BLOB_SAMPLE VALUES"))
			.doOnSuccess(s -> System.out.println("Deleted rows."))
			.doOnError(e -> System.err.println("ERROR: Failed to delete rows.\n"+e))
			.doAfterTerminate(() -> con.get().release());
		
		return mono;	
	}
	
	/**
	 * Runs the BlobSample.
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String args[]) throws Exception
	{
		// debug flags
		// System.setProperty("sun.security.krb5.debug","true");
	    // System.setProperty("javax.net.debug", "ssl,handshake");
		
		readConfig();			
		setupConnectionPool();  
		
		Mono<Void> mono = Mono.just(new BlobSample())
			.delayUntil(d -> d.createTable())
			.delayUntil(d -> d.insertRow(1))
			.delayUntil(d -> d.queryRow(1))
			.delayUntil(d -> d.deleteRows())
			.delayUntil(d -> d.dropTable())
			.doOnError(e -> System.err.println(e.toString()))
			.then();
		
		System.out.println("BLOB Sample Application is starting...");
		
		mono.block();   // subscribe and wait till this mono completes	
	}
}
