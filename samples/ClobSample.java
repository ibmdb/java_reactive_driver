
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import com.ibm.db2.r2dbc.DB2Clob;
import com.ibm.db2.r2dbc.DB2Connection;
import com.ibm.db2.r2dbc.DB2ConnectionConfiguration;
import com.ibm.db2.r2dbc.DB2ConnectionFactory;
import com.ibm.db2.r2dbc.DB2ConnectionPool;
import com.ibm.db2.r2dbc.DB2Result;
import io.r2dbc.spi.Row;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;


public class ClobSample 
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
	 * Creates the CLOB_SAMPLE table.
	 * 
	 */
	public Mono<Void> createTable()
	{
		System.out.println("createTable() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		Mono<Void> mono = _pool.getConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.executeUpdate(
					"CREATE TABLE CLOB_SAMPLE (" +
						"ID INTEGER UNIQUE NOT NULL, " +
						"DATA1 CLOB(10M) NOT NULL, " +
						"DATA2 CLOB(10M) NOT NULL" +
					") CCSID UNICODE"
			))
			.doOnSuccess(s -> System.out.println("Table created."))
			.doOnError(e -> System.err.println("ERROR: Failed to create table.\n"+e))
			.doOnTerminate(() -> {if (con.get() != null) con.get().release();});
		
		return mono;
	}
	

	/**
	 * Drop the CLOB_SAMPLE table.
	 * 
	 */
	public Mono<Void> dropTable()
	{
		System.out.println("dropTable() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		Mono<Void> mono = _pool.getConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.executeUpdate("DROP TABLE CLOB_SAMPLE"))
			.doOnSuccess(s -> System.out.println("Table dropped."))
			.doOnError(e -> System.err.println("ERROR: Failed to drop table.\n"+e))
			.doOnTerminate(() -> con.get().release());
		
		return mono;
	}
	
	
	/**
	 * Inserts a row of data in  table.
	 * 
	 * @param id - unique id
	 * @param path - path to the file containing clob data
	 * @return Mono<Result> - insert return value
	 */
	public Mono<DB2Result> insertRow(int id, String file)
	{
		System.out.println("insertRow() called, id: "+id);
		
		String path = Thread.currentThread().getContextClassLoader().getResource(file).getPath();
		String filepath = path.replace("/C:/", "/"); 
		
		// data for DATA1 column
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 10; i++) {
			sb.append(i+". "+"Best Wishes | வாழ்த்துகள் | ಶುಭಾಷಯಗಳು | శుభాకాంక్షలు | शुभकामनाएं | ਸ਼ੁਭ ਕਾਮਨਾਵਾਂ  | 幸運をお祈りしています | 最好的祝愿 | 최고의 소원 | С наилучшими пожеланиями | أهلا بك | \n");
			sb.append(i+". "+"₹ € £ $ ¥ ∫ ∂ δ α Β Ɣ \n");
		}
		
		// data for DATA2 column
		Flux<String> flux = Flux.using(
				() -> Files.lines(Paths.get(filepath)), 
				Flux::fromStream, 
				Stream::close)
				.map(s -> s+"\n")     // add the stripped line seperator
				.doOnError(e -> System.err.println(e));   // print any file open errors
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		AtomicReference<DB2Clob> clob1 = new AtomicReference<DB2Clob>();
		AtomicReference<DB2Clob> clob2 = new AtomicReference<DB2Clob>();
		
		Mono<DB2Result> mono = _pool.getConnection()
			.doOnNext(c -> {
				con.set(c);
				clob1.set(c.createDB2Clob(sb.toString()));
				clob2.set(c.createDB2Clob(flux));
			})
			.flatMap(c -> c.createStatement("INSERT INTO CLOB_SAMPLE (ID, DATA1, DATA2) VALUES (:id, :clob1, :clob2)")
				   		  	.bind("id", id)
				   		  	.bind("clob1", clob1.get())
				   		  	.bind("clob2", clob2.get())
				   		  	.execute())
			.doOnSuccess(s -> System.out.println("Row inserted."))
			.doOnError(e -> System.err.println("ERROR: Failed to insert a row.\n"+e))
			.doAfterTerminate(() -> con.get().release());
		
		return mono;	
	}
	

	/**
	 * Query the CLOB data given the corresponding id. Write the CLOB data to the
	 * specified path.
	 * 
	 * @param id - identifies the specific row.
	 * @param path - path to a file to write the CLOB data to.
	 */
	public Mono<Void> queryRow(int id, String path) 
	{
		System.out.println("queryRow() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		AtomicReference<BufferedWriter> bw = new AtomicReference<BufferedWriter>();
		
		// setup buffered writer for the output file
		try {
			Path outputFile = Paths.get(path);
			BufferedWriter w = Files.newBufferedWriter(outputFile, Charset.forName("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
			bw.set(w);
		} catch (Exception e) {
			return Mono.error(e);
		}
		
		// Query the inserted data and then release the connection
		Flux<Row> flux = _pool.getConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.createStatement("SELECT ID, DATA1, DATA2 FROM CLOB_SAMPLE WHERE ID = :id")
							.bind("id", id)
							.execute())
			.flatMapMany(result -> result.map((row, md) -> row))
			.doOnNext(row -> {
				System.out.println("ID: "+row.get("ID"));
				DB2Clob clob1 = (DB2Clob) row.get("DATA1");
				clob1.stream().subscribe(s -> {
					System.out.println("DATA1: "+s);
				}); 
				DB2Clob clob2 = (DB2Clob) row.get("DATA2");
				clob2.stream().subscribe(s -> {
					//System.out.println("DATA2: "+s);
					try {
						// write to the output file
						bw.get().write(s.toString());
					}
					catch (Exception e) {
						System.err.println(e);
					}
				});
			})
			.doOnComplete(() -> {
				try {
					bw.get().flush();
					bw.get().close();
				} catch(Exception e) {
					System.err.println(e);
				}
				System.out.println("Query completed.");
			})
			.doOnError(e -> System.err.println("ERROR: Failed to query clob data.\n"+e))
			.doAfterTerminate(() -> {
				con.get().release();
			});
		
		return flux.then();
	}
	
	
	/**
	 * Delete all rows in CLOB_SAMPLE table.
	 * 
	 * @param id - account id, integer
	 * @param bdec - initial ammount, BigDecimal
	 * @return Mono<Result> - insert return value
	 */
	public Mono<Void> deleteRows()
	{
		System.out.println("deleteRows() called");
		
		AtomicReference<DB2Connection> con = new AtomicReference<DB2Connection>();
		
		Mono<Void> mono = _pool.getConnection()
			.doOnNext(c -> con.set(c))
			.flatMap(c -> c.executeUpdate("DELETE FROM CLOB_SAMPLE VALUES"))
			.doOnSuccess(s -> System.out.println("Deleted rows."))
			.doOnError(e -> System.err.println("ERROR: Failed to delete rows.\n"+e))
			.doAfterTerminate(() -> con.get().release());
		
		return mono;	
	}
	
	/**
	 * Runs the ClobSample.
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
		
		Mono<Void> mono = Mono.just(new ClobSample())
			.delayUntil(d -> d.createTable())
			.delayUntil(d -> d.insertRow(1, "clob_unicode_data.txt"))
			.delayUntil(d -> d.queryRow(1, "queried_clob_data.txt"))
			.delayUntil(d -> d.deleteRows())
			.delayUntil(d -> d.dropTable())
			.doOnError(e -> System.err.println(e.toString()))
			.then();
		
		System.out.println("CLOB Sample Application is starting...");
		
		mono.block();   // subscribe and wait till this mono completes	
	}
}
