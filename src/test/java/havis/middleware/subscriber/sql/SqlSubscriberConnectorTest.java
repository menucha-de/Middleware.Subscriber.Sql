package havis.middleware.subscriber.sql;

import static mockit.Deencapsulation.getField;
import static mockit.Deencapsulation.setField;

import havis.middleware.ale.base.exception.ImplementationException;
import havis.middleware.ale.base.exception.InvalidURIException;
import havis.middleware.ale.service.EPC;
import havis.middleware.ale.service.cc.CCCmdReport;
import havis.middleware.ale.service.cc.CCOpReport;
import havis.middleware.ale.service.cc.CCReports;
import havis.middleware.ale.service.cc.CCTagReport;
import havis.middleware.ale.service.ec.ECReport;
import havis.middleware.ale.service.ec.ECReportGroup;
import havis.middleware.ale.service.ec.ECReportGroupCount;
import havis.middleware.ale.service.ec.ECReportGroupList;
import havis.middleware.ale.service.ec.ECReportGroupListMember;
import havis.middleware.ale.service.ec.ECReportGroupListMemberExtension;
import havis.middleware.ale.service.ec.ECReportMemberField;
import havis.middleware.ale.service.ec.ECReports;
import havis.middleware.ale.service.pc.PCEventReport;
import havis.middleware.ale.service.pc.PCOpReport;
import havis.middleware.ale.service.pc.PCOpReports;
import havis.middleware.ale.service.pc.PCReport;
import havis.middleware.ale.service.pc.PCReports;
import havis.middleware.ale.subscriber.SubscriberConnector;
import havis.middleware.subscriber.sql.rest.Storage;

import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.stream.XMLStreamWriter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import mockit.Deencapsulation;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.NonStrictExpectations;
import mockit.Verifications;

public class SqlSubscriberConnectorTest {

	public static final String CREATE = "CREATE TABLE test (plain TEXT, spec TEXT, date TIMESTAMP, init_condition TEXT, init_trigger TEXT, term_condition TEXT, term_trigger TEXT, report_name TEXT, total INT, group_name TEXT, group_count INT, epc TEXT, tag TEXT, raw_hex TEXT, field_1 TEXT, report_id TEXT, op_name TEXT, op_status TEXT, data TEXT)";
	public static final String DROP = "DROP TABLE test";

	public final String url = "jdbc:h2:mem:test";
	public final String uri = "sql://?connection=" + url;

	private String resSpecName = "specName00";
	private Date resDate = new Date();
	private long resTotalMilliseconds = 100000001;
	private String resInitiationCondition = "initiationCondition01";
	private String resInitiationTrigger = "initiationTrigger02";
	private String resTerminationCondition = "terminationCondition03";
	private String resTerminationTrigger = "terminationTrigger04";

	private String resReportName = "reportName05";
	private String resGroupName = "groupName06";
	private int resCount = 12345;

	private String resValueEpc = "epc07";
	private String resValueTag = "tag08";
	private String resValueRaw = "rawHex09";

	private String resName = "USER";

	private String resValue = "fieldname10";

	@Before
	public void init() throws SQLException {
		Connection connection = DriverManager.getConnection(url);
		try (Statement statement = connection.createStatement()) {
			statement.execute(CREATE);
		}
	}

	@After
	public void drop() throws SQLException {
		Connection connection = DriverManager.getConnection(url);
		try (Statement statement = connection.createStatement()) {
			statement.execute(DROP);
		}
	}

	public void testPostgreSQL() throws Exception {
		Class.forName("org.postgresql.Driver").newInstance();
		SqlSubscriberConnector connector = new SqlSubscriberConnector();
		String uri = "sql:///?connecton=jdbc:postgresql://localhost/custom?user=custom%26password=secret&table=aletest&plain=plain&spec=spec&date=date&totalMilliseconds=totalMilliseconds&initiationCondition=initiationCondition&initiationTrigger=initiationTrigger&terminationCondition=terminationCondition&terminationTrigger=terminationTrigger&report=report&group=groupname&epc=epc&tag=tag&rawHex=rawHex&data=data";
		connector.init(new URI(uri), new HashMap<String, String>());

		ECReports reports = createEcReports();

		for (int i = 0; i < 10; i++) {
			connector.send(reports);
		}

		connector.dispose();
	}

	
	@Test
	public void initTest(@Mocked final Storage storage, @Mocked final Statement statement,@Mocked final Connection connection) throws URISyntaxException, ImplementationException, InvalidURIException, SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		String text;
		int index;
		int size;
		SqlSubscriberConnector.ColumnMap map;

		final SubscriberConnector connector = new SqlSubscriberConnector();
		// Reaction of init(URI uri) when connectionString = null Expecting:
		// Exception "No connection specified"
		try {
			connector.init(new URI("sql://?"), new HashMap<String, String>());
			Assert.fail("Connection string should not be null");
		} catch (InvalidURIException e) {
			Assert.assertEquals("No connection specified", e.getMessage());
		}

		// Reaction of init(URI uri) when table is null Expecting: Exception
		// "No table specified"
		try {
			connector.init(new URI("sql://?connection=jdbc:mysql:test"), new HashMap<String, String>()); // 
			Assert.fail("Table string should not be null");
		} catch (InvalidURIException e) {
			Assert.assertEquals("No table specified", e.getMessage());
		}

		// Reaction of init(URI uri) when neighter columns.length() > 0 nor
		// plain != null Expecting: Exception
		// "No column nor plain output specified"
		try {
			connector.init(new URI("sql://?connection=jdbc:&table=table"), new HashMap<String, String>());
			Assert.fail("Plain string should not be null");
		} catch (InvalidURIException e) {
			Assert.assertEquals("No column nor plain output specified", e.getMessage());
		}

		// Reaction of init(URI uri) when plain != null Expecting: Field "PLAIN"
		// = "INSERT INTO test (plain) VALUES (?)"
		connector.init(new URI("sql://?connection=jdbc:&table=test&plain=plain&name=name"), new HashMap<String, String>());
		String plain = getField(connector, "PLAIN");
		Assert.assertEquals("INSERT INTO \"test\" (\"plain\") VALUES (?)", plain);

		// Mocking: static instance of Storage
		// storage and connector are correctly added to the Storage instance
		// Expecting: Added object are equal to storage and connector
		final String storageName = "test";
		connector.init(new URI("sql://?connection=jdbc:&table=test&name=name&storage=" + storageName), new HashMap<String, String>());
		new Verifications() {
			{
				String name;
				SqlSubscriberConnector ssc;
				storage.put(name = withCapture(), ssc = withCapture());

				Assert.assertEquals(storageName, name);
				Assert.assertEquals(connector, ssc);

			}
		};

		// Mocking: Connection and creating an new Statement object when
		// createStatement() gets called
		// When init = true create a Statement object which throws an Exception
		// when execute() is called Expecting: Exception
		// "Failed to initialize table: test"
		new MockUp<Connection>() {

			@Mock
			public Statement createStatement() {
				return statement;
			}
		};

		new NonStrictExpectations() {
			{
				statement.execute("CREATE TABLE \"test\" (\"name\" TEXT)");
				result = new SQLException("test");

				connection.isClosed();
				result = false;
			}
		};
		setField(connector, "connection", connection);
		try {
			connector.init(new URI("sql://?connection=jdbc:&table=test&name=name&init=true"), new HashMap<String, String>());
			Assert.fail("Exception expected");
		} catch (InvalidURIException e) {
			Assert.assertEquals("Failed to initialize table: test", e.getMessage());
		}
		setField(connector, "init", false);

		// Is drop correctly initialized Expecting: drop = true
		connector.init(new URI("sql://?connection=jdbc:&table=test&name=name&drop=true"), new HashMap<String, String>());
		boolean drop = getField(connector, "drop");
		Assert.assertTrue(drop);
		setField(connector, "drop", false);

		// Is clear correctly initialized Expecting: Field "delete" =
		// "DELETE FROM test"
		connector.init(new URI("sql://?connection=jdbc:&table=test&name=name&clear=true"), new HashMap<String, String>());
		String delete = getField(connector, "delete");
		Assert.assertEquals("DELETE FROM \"test\"", delete);
		setField(connector, "clear", false);

		// The fields "TEXT","map.spec" get initialized and a sql-statement gets
		// executed
		// Expecting: "TEXT" = "INSERT INTO test (spec) VALUES (?)" "map.spec" =
		// 1 sql-statement = "CREATE TABLE test (spec TEXT)"
		connector.init(new URI("sql://?connection=jdbc:&table=test&spec=spec&init=true"), new HashMap<String, String>());
		text = getField(connector, "TEXT");
		map = getField(connector, "map");
		index = getField(map, "spec");
		Assert.assertEquals("INSERT INTO \"test\" (\"spec\") VALUES (?)", text);
		Assert.assertEquals(1, index);

		new Verifications() {
			{
				String sql;
				statement.execute(sql = withCapture());
				Assert.assertEquals("CREATE TABLE \"test\" (\"spec\" TEXT)", sql);
			}
		};
		setField(connector, "init", false);

		// The fields "TEXT","map.date" get initialized and a sql-statement gets
		// executed
		// Expecting: "TEXT" = "INSERT INTO test (date) VALUES (?)" "map.date" =
		// 1 sql-statement = "CREATE TABLE test (date TEXT)"
		connector.init(new URI("sql://?connection=jdbc:&table=test&date=date&init=true"), new HashMap<String, String>());
		text = getField(connector, "TEXT");
		map = getField(connector, "map");
		index = getField(map, "date");
		Assert.assertEquals("INSERT INTO \"test\" (\"date\") VALUES (?)", text);
		Assert.assertEquals(1, index);

		new Verifications() {
			{
				String sql;
				statement.execute(sql = withCapture());
				Assert.assertEquals("CREATE TABLE \"test\" (\"date\" TIMESTAMP)", sql);
			}
		};
		setField(connector, "init", false);

		// The fields "TEXT","map.totalMilliseconds" get initialized and a
		// sql-statement gets executed
		// Expecting: "TEXT" = "INSERT INTO test (abc) VALUES (?)"
		// "map.totalMilliseconds" = 1 sql-statement =
		// "CREATE TABLE test (abc LONG)"
		connector.init(new URI("sql://?connection=jdbc:&table=test&totalMilliseconds=abc&init=true"), new HashMap<String, String>());
		text = getField(connector, "TEXT");
		map = getField(connector, "map");
		index = getField(map, "totalMilliseconds");
		Assert.assertEquals("INSERT INTO \"test\" (\"abc\") VALUES (?)", text);
		Assert.assertEquals(1, index);

		new Verifications() {
			{
				String sql;
				statement.execute(sql = withCapture());
				Assert.assertEquals("CREATE TABLE \"test\" (\"abc\" LONG)", sql);
			}
		};
		setField(connector, "init", false);

		// The fields "TEXT","map.initiationCondition" get initialized and a
		// sql-statement gets executed
		// Expecting: "TEXT" =
		// "INSERT INTO test (initiationCondition) VALUES (?)"
		// "map.initiationCondition" = 1
		// sql-statement = "CREATE TABLE test (initiationCondition TEXT)"
		connector.init(new URI("sql://?connection=jdbc:&table=test&initiationCondition=initiationCondition&init=true"), new HashMap<String, String>());
		text = getField(connector, "TEXT");
		map = getField(connector, "map");
		index = getField(map, "initiationCondition");
		Assert.assertEquals("INSERT INTO \"test\" (\"initiationCondition\") VALUES (?)", text);
		Assert.assertEquals(1, index);

		new Verifications() {
			{
				String sql;
				statement.execute(sql = withCapture());
				Assert.assertEquals("CREATE TABLE \"test\" (\"initiationCondition\" TEXT)", sql);
			}
		};
		setField(connector, "init", false);

		// The fields "TEXT","map.initiationTrigger" get initialized and a
		// sql-statement gets executed
		// Expecting: "TEXT" = "INSERT INTO test (initiationTrigger) VALUES (?)"
		// "map.initiationTrigger" = 1
		// sql-statement = "CREATE TABLE test (initiationTrigger TEXT)"
		connector.init(new URI("sql://?connection=jdbc:&table=test&initiationTrigger=initiationTrigger&init=true"), new HashMap<String, String>());
		text = getField(connector, "TEXT");
		map = getField(connector, "map");
		index = getField(map, "initiationTrigger");
		Assert.assertEquals("INSERT INTO \"test\" (\"initiationTrigger\") VALUES (?)", text);
		Assert.assertEquals(1, index);

		new Verifications() {
			{
				String sql;
				statement.execute(sql = withCapture());
				Assert.assertEquals("CREATE TABLE \"test\" (\"initiationTrigger\" TEXT)", sql);
			}
		};
		setField(connector, "init", false);

		// The fields "TEXT","map.terminationCondition" get initialized and a
		// sql-statement gets executed
		// Expecting: "TEXT" =
		// "INSERT INTO test (terminationCondition) VALUES (?)"
		// "map.terminationCondition" = 1
		// sql-statement = "CREATE TABLE test (terminationCondition TEXT)"
		connector.init(new URI("sql://?connection=jdbc:&table=test&terminationCondition=terminationCondition&init=true"), new HashMap<String, String>());
		text = getField(connector, "TEXT");
		map = getField(connector, "map");
		index = getField(map, "terminationCondition");
		Assert.assertEquals("INSERT INTO \"test\" (\"terminationCondition\") VALUES (?)", text);
		Assert.assertEquals(1, index);

		new Verifications() {
			{
				String sql;
				statement.execute(sql = withCapture());
				Assert.assertEquals("CREATE TABLE \"test\" (\"terminationCondition\" TEXT)", sql);
			}
		};
		setField(connector, "init", false);

		// The fields "TEXT","map.terminationTrigger" get initialized and a
		// sql-statement gets executed
		// Expecting: "TEXT" =
		// "INSERT INTO test (terminationTrigger) VALUES (?)"
		// "map.terminationTrigger" = 1
		// sql-statement = "CREATE TABLE test (terminationTrigger TEXT)"
		connector.init(new URI("sql://?connection=jdbc:&table=test&terminationTrigger=terminationTrigger&init=true"), new HashMap<String, String>());
		text = getField(connector, "TEXT");
		map = getField(connector, "map");
		index = getField(map, "terminationTrigger");
		Assert.assertEquals("INSERT INTO \"test\" (\"terminationTrigger\") VALUES (?)", text);
		Assert.assertEquals(1, index);

		new Verifications() {
			{
				String sql;
				statement.execute(sql = withCapture());
				Assert.assertEquals("CREATE TABLE \"test\" (\"terminationTrigger\" TEXT)", sql);
			}
		};
		setField(connector, "init", false);

		// The fields "TEXT","map.report" get initialized and a sql-statement
		// gets executed
		// Expecting: "TEXT" = "INSERT INTO test (report) VALUES (?)"
		// "map.report" = 1 sql-statement = "CREATE TABLE test (report TEXT)"
		connector.init(new URI("sql://?connection=jdbc:&table=test&report=report&init=true"), new HashMap<String, String>());
		text = getField(connector, "TEXT");
		map = getField(connector, "map");
		index = getField(map, "report");
		Assert.assertEquals("INSERT INTO \"test\" (\"report\") VALUES (?)", text);
		Assert.assertEquals(1, index);

		new Verifications() {
			{
				String sql;
				statement.execute(sql = withCapture());
				Assert.assertEquals("CREATE TABLE \"test\" (\"report\" TEXT)", sql);
			}
		};
		setField(connector, "init", false);

		// The fields "TEXT","map.group" get initialized and a sql-statement
		// gets executed
		// Expecting: "TEXT" = "INSERT INTO test (group) VALUES (?)" "map.group"
		// = 1 sql-statement = "CREATE TABLE test (group TEXT)"
		connector.init(new URI("sql://?connection=jdbc:&table=test&group=group&init=true"), new HashMap<String, String>());
		text = getField(connector, "TEXT");
		map = getField(connector, "map");
		index = getField(map, "group");
		Assert.assertEquals("INSERT INTO \"test\" (\"group\") VALUES (?)", text);
		Assert.assertEquals(1, index);

		new Verifications() {
			{
				String sql;
				statement.execute(sql = withCapture());
				Assert.assertEquals("CREATE TABLE \"test\" (\"group\" TEXT)", sql);
			}
		};
		setField(connector, "init", false);

		// The fields "TEXT","map.count" get initialized and a sql-statement
		// gets executed
		// Expecting: "TEXT" = "INSERT INTO test (count) VALUES (?)" "map.count"
		// = 1 sql-statement = "CREATE TABLE test (count INT)"
		connector.init(new URI("sql://?connection=jdbc:&table=test&count=count&init=true"), new HashMap<String, String>());
		text = getField(connector, "TEXT");
		map = getField(connector, "map");
		index = getField(map, "count");
		Assert.assertEquals("INSERT INTO \"test\" (\"count\") VALUES (?)", text);
		Assert.assertEquals(1, index);

		new Verifications() {
			{
				String sql;
				statement.execute(sql = withCapture());
				Assert.assertEquals("CREATE TABLE \"test\" (\"count\" INT)", sql);
			}
		};
		setField(connector, "init", false);

		// The fields "TEXT","map.epc" get initialized and a sql-statement gets
		// executed
		// Expecting: "TEXT" = "INSERT INTO test (epc) VALUES (?)" "map.epc" = 1
		// sql-statement = "CREATE TABLE test (epc TEXT)"
		connector.init(new URI("sql://?connection=jdbc:&table=test&epc=epc&init=true"), new HashMap<String, String>());
		text = getField(connector, "TEXT");
		map = getField(connector, "map");
		index = getField(map, "epc");
		Assert.assertEquals("INSERT INTO \"test\" (\"epc\") VALUES (?)", text);
		Assert.assertEquals(1, index);

		new Verifications() {
			{
				String sql;
				statement.execute(sql = withCapture());
				Assert.assertEquals("CREATE TABLE \"test\" (\"epc\" TEXT)", sql);
			}
		};
		setField(connector, "init", false);

		// The fields "TEXT","map.tag" get initialized and a sql-statement gets
		// executed
		// Expecting: "TEXT" = "INSERT INTO test (tag) VALUES (?)" "map.tag" = 1
		// sql-statement = "CREATE TABLE test (tag TEXT)"
		connector.init(new URI("sql://?connection=jdbc:&table=test&tag=tag&init=true"), new HashMap<String, String>());
		text = getField(connector, "TEXT");
		map = getField(connector, "map");
		index = getField(map, "tag");
		Assert.assertEquals("INSERT INTO \"test\" (\"tag\") VALUES (?)", text);
		Assert.assertEquals(1, index);

		new Verifications() {
			{
				String sql;
				statement.execute(sql = withCapture());
				Assert.assertEquals("CREATE TABLE \"test\" (\"tag\" TEXT)", sql);
			}
		};
		setField(connector, "init", false);

		// The fields "TEXT","map.rawHex" get initialized and a sql-statement
		// gets executed
		// Expecting: "TEXT" = "INSERT INTO test (rawHex) VALUES (?)"
		// "map.rawHex" = 1 sql-statement = "CREATE TABLE test (rawHex TEXT)"
		connector.init(new URI("sql://?connection=jdbc:&table=test&rawHex=rawHex&init=true"), new HashMap<String, String>());
		text = getField(connector, "TEXT");
		map = getField(connector, "map");
		index = getField(map, "rawHex");
		Assert.assertEquals("INSERT INTO \"test\" (\"rawHex\") VALUES (?)", text);
		Assert.assertEquals(1, index);

		new Verifications() {
			{
				String sql;
				statement.execute(sql = withCapture());
				Assert.assertEquals("CREATE TABLE \"test\" (\"rawHex\" TEXT)", sql);
			}
		};
		setField(connector, "init", false);

		// The fields "TEXT","map.field" get initialized and a sql-statement
		// gets executed
		// Expecting: "TEXT" =
		// "INSERT INTO test (field0,field1,field2) VALUES (?,?,?)" "map.field"
		// = 1 sql-statement =
		// "CREATE TABLE test (field0 TEXT,field1 TEXT,field2 TEXT)"
		connector.init(new URI("sql://?connection=jdbc:&table=test&field=field0,field1,field2&init=true"), new HashMap<String, String>());
		map = getField(connector, "map");
		size = getField(map, "size");
		text = getField(connector, "TEXT");
		index = getField(map, "field");
		int newindex = (index + (size - 1));
		Assert.assertEquals(3, size);
		Assert.assertEquals("INSERT INTO \"test\" (\"field0\",\"field1\",\"field2\") VALUES (?,?,?)", text);
		Assert.assertEquals(1, index);
		Assert.assertEquals(3, newindex);
			
		new Verifications() {{
			String sql;
			statement.execute(sql = withCapture());
				Assert.assertEquals("CREATE TABLE \"test\" (\"field0\" TEXT,\"field1\" TEXT,\"field2\" TEXT)", sql);
		}};		
		setField(connector, "init", false);
		setField(map, "size", 0);
		setField(connector, "TEXT", null);
		setField(map, "field", 0);
		
		//Value of key "field" has invalid pattern
		//Expecting: Exception with the invalid value and key in it + no changes to other fields
		try{
			connector.init(new URI("sql://?connection=jdbc%3Ah2%3A&table=test&field=field1,2field,field3"), new HashMap<String, String>());
			Assert.fail("Exception expected");
		}catch (InvalidURIException e) {
			map = getField(connector, "map");
			size = getField(map, "size");
			text = getField(connector, "TEXT");
			index = getField(map, "field");
			int nindex = (index+(size-1));
			Assert.assertEquals("Column name '2field' is invalid for field 'field'", e.getMessage());
			Assert.assertEquals(0, size);
			Assert.assertEquals(null, text);  
			Assert.assertEquals(0, index);
			Assert.assertEquals(-1, nindex);
			
			setField(map, "size", 0);
			setField(connector, "TEXT", null);
			setField(map, "field", 0);
		}

		// The fields "TEXT","map.id" get initialized and a sql-statement gets
		// executed
		// Expecting: "TEXT" = "INSERT INTO test (id) VALUES (?)" "map.id" = 1
		// sql-statement = "CREATE TABLE test (id TEXT)"
		connector.init(new URI("sql://?connection=jdbc:&table=test&id=id&init=true"), new HashMap<String, String>());
		text = getField(connector, "TEXT");
		map = getField(connector, "map");
		index = getField(map, "id");
		Assert.assertEquals("INSERT INTO \"test\" (\"id\") VALUES (?)", text);
		Assert.assertEquals(1, index);

		new Verifications() {
			{
				String sql;
				statement.execute(sql = withCapture());
				Assert.assertEquals("CREATE TABLE \"test\" (\"id\" TEXT)", sql);
			}
		};
		setField(connector, "init", false);

		// The fields "TEXT","map.status" get initialized and a sql-statement
		// gets executed WITH MULTIPLE PARAMETERS (3, not in case order)
		// Expecting: "TEXT" =
		// "INSERT INTO test (name1,id,status) VALUES (?,?,?)" "map.status" = 3
		// sql-statement = "CREATE TABLE test (name1 TEXT,id TEXT,status TEXT)"
		connector.init(new URI("sql://?connection=jdbc:&table=test&init=true&name=name1&id=id&status=status"), new HashMap<String, String>());
		text = getField(connector, "TEXT");
		map = getField(connector, "map");
		index = getField(map, "status");
		Assert.assertEquals("INSERT INTO \"test\" (\"name1\",\"id\",\"status\") VALUES (?,?,?)", text);
		Assert.assertEquals(3, index);

		new Verifications() {
			{
				String sql;
				statement.execute(sql = withCapture());
				Assert.assertEquals("CREATE TABLE \"test\" (\"name1\" TEXT,\"id\" TEXT,\"status\" TEXT)", sql);
			}
		};
		setField(connector, "init", false);

		// The fields "TEXT","map.initiationCondition" get initialized and a
		// sql-statement gets executed WITH MULTIPLE PARAMETERS (6, not in case
		// order)
		// Expecting: "TEXT" =
		// "INSERT INTO test1 (epc,tag,status,field0,field1,terminationCondition,initiationCondition) VALUES (?,?,?,?,?,?,?)"
		// "map.initiationCondition" = 7 sql-statement =
		// "CREATE TABLE test1 (epc TEXT,tag TEXT,status TEXT,field0 TEXT,field1 TEXT,terminationCondition TEXT,initiationCondition TEXT)"
		setField(map, "size", 0);
		connector.init(new URI("sql://?connection=jdbc:&epc=epc&tag=tag&status=status&init=true&table=test1&field=field0,"
				+ "field1&terminationCondition=terminationCondition&initiationCondition=initiationCondition"), new HashMap<String, String>());
		text = getField(connector, "TEXT");
		map = getField(connector, "map");
		index = getField(map, "initiationCondition");

		Assert.assertEquals("INSERT INTO \"test1\" (\"epc\",\"tag\",\"status\",\"field0\",\"field1\",\"terminationCondition\",\"initiationCondition\") VALUES (?,?,?,?,?,?,?)", text);
		Assert.assertEquals(7, index);

		new Verifications() {
			{
				String sql;
				statement.execute(sql = withCapture());
				Assert.assertEquals(
						"CREATE TABLE \"test1\" (\"epc\" TEXT,\"tag\" TEXT,\"status\" TEXT,\"field0\" TEXT,\"field1\" TEXT,\"terminationCondition\" TEXT,\"initiationCondition\" TEXT)", sql);
			}
		};
		setField(connector, "init", false);

		// The fields "TEXT","map.status" get initialized and a sql-statement
		// gets executed WITH MULTIPLE PARAMETERS (4, not in case order)
		// Expecting: "TEXT" =
		// "INSERT INTO test (data,id,AaBbCc) VALUES (?,?,?)" "map.count" = 3
		// sql-statement = "CREATE TABLE test (data TEXT,id TEXT,AaBbCc INT)"
		setField(map, "report", 0);
		connector.init(new URI("sql://?connection=jdbc:&table=test&data=data&id=id&count=AaBbCc&init=true"), new HashMap<String, String>());
		text = getField(connector, "TEXT");
		map = getField(connector, "map");
		index = getField(map, "count");
		Assert.assertEquals("INSERT INTO \"test\" (\"data\",\"id\",\"AaBbCc\") VALUES (?,?,?)", text);
		Assert.assertEquals(3, index);

		new Verifications() {
			{
				String sql;
				statement.execute(sql = withCapture());
				Assert.assertEquals("CREATE TABLE \"test\" (\"data\" TEXT,\"id\" TEXT,\"AaBbCc\" INT)", sql);
			}
		};
		setField(connector, "init", false);

		// Reaction of init() with a unknown parameter Expection: Exception:
		// ""Parameter 'x' is unknown!" (x = unknown parameter in URI)
		try {
			connector.init(new URI("sql://?connection=jdbc:&unknownparam=unknown"), new HashMap<String, String>());
			Assert.fail("Expect Exception!");
		} catch (InvalidURIException e) {
			Assert.assertEquals("Parameter 'unknownparam' is unknown!", e.getMessage());
		}

		// Reaction of init() when a parameter-value has invalid pattern
		// Expecting: Exception: "Column name 'x' is invalid for field 'y'" (x =
		// invalid value / y = parameter)
		try {
			connector.init(new URI("sql://?connection=jdbc%3Ah2%3A&spec=:spec"), new HashMap<String, String>());
			Assert.fail();
		} catch (InvalidURIException e) {
			Assert.assertEquals("Column name ':spec' is invalid for field 'spec'", e.getMessage());
		}

		// Reaction of init() when a parameter has an emty value Expecting:
		// Exception: "Value of field 'x' couldn't be empty" (x = parameter)
		try {
			connector.init(new URI("sql://?plain=plain&id"), new HashMap<String, String>());
			Assert.fail("Expect Exception!");
		} catch (Exception e) {
			Assert.assertEquals("Value of field 'id' couldn't be empty", e.getMessage());
		}
	}
	
	@Test (expected = ImplementationException.class)
	public void initImplementationException(@Mocked final URI uri) throws InvalidURIException, ImplementationException, URISyntaxException {
		final SubscriberConnector connector = new SqlSubscriberConnector();
		
		new NonStrictExpectations() {{
			uri.getRawQuery();
			result = new IOException();
		}};
		
		connector.init(uri, new HashMap<String, String>());		
	}
	
	//- Does the PreparedStatement is generated with the correct SQL statement
	//- Does the PreparedStatement takes the correct index for each value
	@Test
	public void sendECReports(@Mocked final DriverManager driverManager, @Mocked final Connection connection, @Mocked final PreparedStatement stat) throws ImplementationException, InvalidURIException, URISyntaxException, SQLException{
		final SubscriberConnector connector = new SqlSubscriberConnector();
		
		new NonStrictExpectations() {{
			DriverManager.getConnection("jdbc:");
			result = connection;
			
			connection.prepareStatement("INSERT INTO test (spec,date,totalMilliseconds,"
					+ "initiationCondition,initiationTrigger,terminationCondition,"
					+ "terminationTrigger,report,group,count,epc,tag,rawhex,field,"
					+ "field2,id,name,status,data) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			result = stat;
		}};

		final String testSpecName = "testSpecName";
		final Date testDate = new Date();
		final long testTotalMilliseconds = 10000;
		final String testInitiationCondition = "testinitiationCondition";
		final String testInitiationTrigger = "testInitiationTrigger";
		final String testTerminationCondition = "testTerminationCondition";
		final String testTerminationTrigger = "testTerminationTrigger";

		final String testReportName = "testReportName";

		final String testGroupName = "testGroupName";
		final ECReportGroupCount testECReportGroupCount = null; // = new ECReportGroupCount();

		final EPC testEPC = new EPC("testEPC");
		final EPC testTAG = new EPC("testTAG");
		final EPC testRawHex = new EPC("testRawHex");
		final ECReportMemberField testECReportMemberField = new ECReportMemberField();
		testECReportMemberField.setValue("field");
		final ECReportMemberField testECReportMemberField2 = new ECReportMemberField();
		testECReportMemberField.setValue("field2");
		final ECReportGroupListMemberExtension testExtension = new ECReportGroupListMemberExtension();
		testExtension.setFieldList(new ECReportGroupListMemberExtension.FieldList());
		testExtension.getFieldList().getField().add(testECReportMemberField);
		testExtension.getFieldList().getField().add(testECReportMemberField2);
		
		connector.init(new URI("sql://?connection=jdbc:&table=test&spec=spec&date=date&totalMilliseconds=totalMilliseconds&initiationCondition=initiationCondition&"
				+ "initiationTrigger=initiationTrigger&terminationCondition=terminationCondition&terminationTrigger=terminationTrigger&report=report&"
				+ "group=group&count=count&epc=epc&tag=tag&rawHex=rawhex&field=field,field2&id=id&name=name&status=status&data=data"), new HashMap<String, String>());
		connector.send(new ECReports(){{
			reports = new Reports(){{
				getReport().add(new ECReport(){{
					specName = testSpecName;
					date = testDate;
					totalMilliseconds = testTotalMilliseconds;
					initiationCondition = testInitiationCondition;
					initiationTrigger = testInitiationTrigger;
					terminationCondition = testTerminationCondition;
					terminationTrigger = testTerminationTrigger;
					
					reportName = testReportName;
					getGroup().add(new ECReportGroup(){{
						groupName = testGroupName;
						groupCount = testECReportGroupCount;
						
						groupList = new ECReportGroupList(){{
						getMember().add(new ECReportGroupListMember(){{
							epc = testEPC;
							tag = testTAG;
							rawHex = testRawHex;
							
							extension = testExtension;
						}});
						}};
					}}); 
				}});
			}};
		}});
		
		new Verifications(){{
				String expectedTEXT = "INSERT INTO \"test\" (\"spec\",\"date\",\"totalMilliseconds\"," + "\"initiationCondition\",\"initiationTrigger\",\"terminationCondition\","
						+ "\"terminationTrigger\",\"report\",\"group\",\"count\",\"epc\",\"tag\",\"rawhex\",\"field\","
						+ "\"field2\",\"id\",\"name\",\"status\",\"data\") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			String TEXT;
			connection.prepareStatement(TEXT = withCapture());
			times = 1;
			
			boolean autoComit;
			connection.setAutoCommit(autoComit = withCapture());
			Assert.assertEquals(false, autoComit);
			
			Assert.assertEquals(expectedTEXT, TEXT);			
			
			stat.setString(1, testSpecName);
			times = 1;
			
			stat.setTimestamp(2, new Timestamp(testDate.getTime()));
			times = 1;
			
			stat.setLong(3, testTotalMilliseconds);
			times = 1;
			
			stat.setString(4, testInitiationCondition);
			times = 1;
			
			stat.setString(5, testInitiationTrigger);
			times = 1;
			
			stat.setString(6, testTerminationCondition);
			times = 1;
			
			stat.setString(7, testTerminationTrigger);
			times = 1;
			
			stat.setString(8, testReportName);
			times = 1;
			
			stat.setString(9, testGroupName);
			times = 1;
			
			stat.setObject(10, testECReportGroupCount != null ? testECReportGroupCount.getCount() : null );
			times = 1;
			
			stat.setString(11, testEPC.getValue());
			times = 1;
			
			stat.setString(12, testTAG.getValue());
			times = 1;
			
			stat.setString(13, testRawHex.getValue());
			times = 1;
			
			stat.setString(14, testECReportMemberField.getValue());
			times = 1;
			
			stat.setString(15, testECReportMemberField2.getValue());
			times = 1;
			
			stat.setString(16, null);
			times = 1;
			
			stat.setString(17, null);
			times = 1;
			
			stat.setString(18, null);
			times = 1;
			
			stat.setString(19, null);
			times = 1;
			
			stat.execute();
			times = 1;
			
			stat.clearParameters();
			times = 1;
			
			connection.commit();
			times = 1;
			
		}};
		connector.dispose();
	}
	
	// ecMarshaller is not longer null, context.createMarshaller() gets called one times
	//plain(Object reports, Marshaller marshaller): PreparedStatement created with correct PLAIN String, PS calls setString(int p, String s) and execute()
	@Test
	public void sendECReportWithPlainNotNull(@Mocked final DriverManager driverManager, @Mocked final Connection connection, @Mocked final JAXBContext context, @Mocked final PreparedStatement stat, @Mocked final Marshaller marshaller, @Mocked final StringWriter writer) throws Exception {
		final SubscriberConnector connector = new SqlSubscriberConnector();
		setField(connector, "PLAIN", "INSERT INTO test (plain) VALUES (?)");
		
		new NonStrictExpectations() {{
			DriverManager.getConnection("jdbc:");
			result = connection;
			
			context.createMarshaller();
			result = marshaller;
			
			connection.isClosed();
			result = false;
			
			connection.prepareStatement("INSERT INTO test (plain) VALUES (?)");
			result = stat;
			
			writer.toString();
			result = "Test";
		}};
		
		final Marshaller oldMarshaller = getField(connector, "ecMarshaller");
		
		ECReports reports = new ECReports(){{
			reports = new Reports(){{
				getReport().add(new ECReport(){{
					getGroup().add(new ECReportGroup(){{						
						groupList = new ECReportGroupList(){{
						getMember().add(new ECReportGroupListMember(){{
						}});
						}};
					}}); 
				}});
			}};
		}};

		connector.send(reports);
		
		Assert.assertNotEquals(oldMarshaller, getField(connector, "ecMarshaller"));
		new Verifications() {{
			context.createMarshaller();
			times = 1;
			
			marshaller.marshal(this.<JAXBElement<ECReports>>withNotNull(), this.<XMLStreamWriter>withNotNull());
			times = 1;
			
			stat.setString(1, "Test");
			times = 1;
			stat.execute();
			times = 1;
		}};
	}
	
	//In case ECReportGroupListMemberExtension is null
	//- Will there be enough indexes with a null value
	@Test
	public void sendECReportsWithNullExtension(@Mocked final DriverManager driverManager, @Mocked final Connection connection, @Mocked final PreparedStatement stat) throws Exception {

		final SubscriberConnector connector = new SqlSubscriberConnector();
		
		new NonStrictExpectations() {{
			DriverManager.getConnection("jdbc:");
			result = connection;
			
			connection.isClosed();
			result = false;
			
			connection.prepareStatement(anyString);
			result = stat;
		}};
		
		connector.init(new URI("sql://?connection=jdbc:&table=test&field=field,field2"), new HashMap<String, String>());
		connector.send(new ECReports() {
			{
				reports = new Reports() {
					{
						getReport().add(new ECReport() {
							{
								getGroup().add(new ECReportGroup() {
									{
										groupList = new ECReportGroupList() {
											{
												getMember().add(new ECReportGroupListMember() {
													{
													}
												});
											}
										};
									}
								});
							}
						});
					}
				};
			}
		});

		new Verifications() {
			{
				String expectedTEXT = "INSERT INTO \"test\" (\"field\",\"field2\") VALUES (?,?)";
				String TEXT;
				connection.prepareStatement(TEXT = withCapture());
				;
				times = 1;
				Assert.assertEquals(expectedTEXT, TEXT);

				// from i(startindex) x - y (x start(1) - y end(2))
				for (int i = 1; i <= 2; i++) {
					stat.setString(i, null);
					times = 1;
				}

				stat.execute();
				times = 1;

				stat.clearParameters();
				times = 1;

				connection.commit();
				times = 1;

			}
		};
		connector.dispose();
	}

	//- Does the PreparedStatement is generated with the correct SQL statement
	//- Does the PreparedStatement takes the correct index for each value
	@Test
	public void sendCCReports(@Mocked final DriverManager driverManager, @Mocked final Connection connection, @Mocked final PreparedStatement stat, @Mocked final Marshaller marshaller) throws ImplementationException, InvalidURIException, URISyntaxException, SQLException{
		final SubscriberConnector connector = new SqlSubscriberConnector();
		
		new NonStrictExpectations() {{
			DriverManager.getConnection("jdbc:");
			result = connection;
			
			connection.prepareStatement(anyString);
			result = stat;
		}};
		
		final String testSpecName = "testSpecName";
		final Date testDate = new Date();
		final long testTotalMilliseconds = 10000;
		final String testInitiationCondition = "testinitiationCondition";
		final String testInitiationTrigger = "testInitiationTrigger";
		final String testTerminationCondition = "testTerminationCondition";
		final String testTerminationTrigger = "testTerminationTrigger";
		
		final String testReportName = "testReportName";
		
		final String testId = "testId";
		
		final String testOpName = "testOpName";
		final String testOpStatus = "testOpStatus";
		final String testData = "testData";
		
		
		connector.init(new URI("sql://?connection=jdbc:&table=test&spec=spec&date=date&totalMilliseconds=totalMilliseconds&initiationCondition=initiationCondition&"
				+ "initiationTrigger=initiationTrigger&terminationCondition=terminationCondition&terminationTrigger=terminationTrigger&report=report&"
				+ "group=group&count=count&epc=epc&tag=tag&rawHex=rawhex&field=field,field2&id=id&name=name&status=status&data=data"), new HashMap<String, String>());
		connector.send(new CCReports(){{
			cmdReports = new CmdReports(){{
				specName = testSpecName;
				date = testDate;
				totalMilliseconds = testTotalMilliseconds;
				initiationCondition = testInitiationCondition;
				initiationTrigger = testInitiationTrigger;
				terminationCondition = testTerminationCondition;
				terminationTrigger = testTerminationTrigger;
				
				getCmdReport().add(new CCCmdReport(){{
					cmdSpecName = testReportName;
					
					tagReports = new TagReports() {{
						getTagReport().add(new CCTagReport() {{
							id = testId;
							
							opReports = new OpReports() {{
								getOpReport().add(new CCOpReport() {{
								opName = testOpName;
								opStatus =testOpStatus;
								data = testData;
								}});
							}};
						}});
					}}; 
				}});
			}};
		}});
		
		new Verifications(){{
				String expectedTEXT = "INSERT INTO \"test\" (\"spec\",\"date\",\"totalMilliseconds\"," + "\"initiationCondition\",\"initiationTrigger\",\"terminationCondition\","
						+ "\"terminationTrigger\",\"report\",\"group\",\"count\",\"epc\",\"tag\",\"rawhex\",\"field\","
						+ "\"field2\",\"id\",\"name\",\"status\",\"data\") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			String TEXT;
			connection.prepareStatement(TEXT = withCapture());
			times = 1;
			Assert.assertEquals(expectedTEXT, TEXT);			
			
			stat.setString(1, testSpecName);
			times = 1;
			
			stat.setTimestamp(2, new Timestamp(testDate.getTime()));
			times = 1;
			
			stat.setLong(3, testTotalMilliseconds);
			times = 1;
			
			stat.setString(4, testInitiationCondition);
			times = 1;
			
			stat.setString(5, testInitiationTrigger);
			times = 1;
			
			stat.setString(6, testTerminationCondition);
			times = 1;
			
			stat.setString(7, testTerminationTrigger);
			times = 1;
			
			stat.setString(8, testReportName);
			times = 1;
			
			stat.setString(9, null);
			times = 1;
			
			stat.setObject(10, null);
			times = 1;
			
			stat.setString(11, null);
			times = 1;
			
			stat.setString(12, null);
			times = 1;
			
			stat.setString(13, null);
			times = 1;
			
			stat.setString(14, null);
			times = 1;
			
			stat.setString(15, null);
			times = 1;
			
			stat.setString(16, testId);
			times = 1;
			
			stat.setString(17, testOpName);
			times = 1;
			
			stat.setString(18, testOpStatus);
			times = 1;
			
			stat.setString(19, testData);
			times = 1;
			
			stat.execute();
			times = 1;
			
			stat.clearParameters();
			times = 1;
			
			connection.commit();
			times = 1;
			
		}};
		connector.dispose();
	}
	
	// ccMarshaller is not longer null, context.createMarshaller() gets called one times
	@Test
	public void sendCCReportWithPlainNotNull(@Mocked final DriverManager driverManager, @Mocked final Connection connection, @Mocked final JAXBContext context) throws SQLException, ImplementationException, JAXBException {
		final SubscriberConnector connector = new SqlSubscriberConnector();
		setField(connector, "PLAIN", "INSERT INTO test (plain) VALUES (?)");
		
		new NonStrictExpectations() {{
			DriverManager.getConnection("jdbc:");
			result = connection;
			
			connection.isClosed();
			result = false;
		}};
		
		final Marshaller oldMarshaller = getField(connector, "ccMarshaller");
	
		connector.send(new CCReports(){{
			cmdReports = new CmdReports(){{					
				getCmdReport().add(new CCCmdReport(){{						
					tagReports = new TagReports() {{
						getTagReport().add(new CCTagReport() {{					
							opReports = new OpReports() {{
								getOpReport().add(new CCOpReport() {{
								}});
							}};
						}});
					}}; 
				}});
			}};
		}});
		
		Assert.assertNotEquals(oldMarshaller, getField(connector, "ccMarshaller"));
		new Verifications() {{
			context.createMarshaller();
			times = 1;
		}};
	}
	
	//- Does the PreparedStatement is generated with the correct SQL statement
	//- Does the PreparedStatement takes the correct index for each value
	@Test
	public void sendPCReports(@Mocked final DriverManager driverManager, @Mocked final Connection connection, @Mocked final PreparedStatement stat, @Mocked final Marshaller marshaller) throws ImplementationException, InvalidURIException, URISyntaxException, SQLException{
		final SubscriberConnector connector = new SqlSubscriberConnector();
		
		new NonStrictExpectations() {{
			DriverManager.getConnection("jdbc:");
			result = connection;
			
			connection.prepareStatement(anyString);
			result = stat;
		}};
		
		final String testSpecName = "testSpecName";
		final Date testDate = new Date();
		final long testTotalMilliseconds = 10000;
		final String testInitiationCondition = "testinitiationCondition";
		final String testInitiationTrigger = "testInitiationTrigger";
		final String testTerminationCondition = "testTerminationCondition";
		final String testTerminationTrigger = "testTerminationTrigger";
		
		final String testReportName = "testReportName";
		
		final String testId = "testId";
		
		final String testOpName = "testOpName";
		final String testOpStatus = "testOpStatus";
		final boolean testState = true;
		
		
		connector.init(new URI("sql://?connection=jdbc:&table=test&spec=spec&date=date&totalMilliseconds=totalMilliseconds&initiationCondition=initiationCondition&"
				+ "initiationTrigger=initiationTrigger&terminationCondition=terminationCondition&terminationTrigger=terminationTrigger&report=report&"
				+ "group=group&count=count&epc=epc&tag=tag&rawHex=rawhex&field=field,field2&id=id&name=name&status=status&data=data"), new HashMap<String, String>());
		connector.send(new PCReports(){{
			reports = new Reports(){{
				specName = testSpecName;
				date = testDate;
				totalMilliseconds = testTotalMilliseconds;
				initiationCondition = testInitiationCondition;
				initiationTrigger = testInitiationTrigger;
				terminationCondition = testTerminationCondition;
				terminationTrigger = testTerminationTrigger;
				
				getReport().add(new PCReport(){{
					reportName = testReportName;
					
					eventReports = new EventReports() {{
						getEventReport().add(new PCEventReport() {{
							id = testId;
							
							opReports = new PCOpReports() {{
								getOpReport().add(new PCOpReport() {{
								opName = testOpName;
								opStatus =testOpStatus;
								state = testState;
								}});
							}};
						}});
					}}; 
				}});
			}};
		}});
		
		new Verifications(){{
				String expectedTEXT = "INSERT INTO \"test\" (\"spec\",\"date\",\"totalMilliseconds\"," + "\"initiationCondition\",\"initiationTrigger\",\"terminationCondition\","
						+ "\"terminationTrigger\",\"report\",\"group\",\"count\",\"epc\",\"tag\",\"rawhex\",\"field\","
						+ "\"field2\",\"id\",\"name\",\"status\",\"data\") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			String TEXT;
			connection.prepareStatement(TEXT = withCapture());
			times = 1;
			Assert.assertEquals(expectedTEXT, TEXT);			
			
			stat.setString(1, testSpecName);
			times = 1;
			
			stat.setTimestamp(2, new Timestamp(testDate.getTime()));
			times = 1;
			
			stat.setLong(3, testTotalMilliseconds);
			times = 1;
			
			stat.setString(4, testInitiationCondition);
			times = 1;
			
			stat.setString(5, testInitiationTrigger);
			times = 1;
			
			stat.setString(6, testTerminationCondition);
			times = 1;
			
			stat.setString(7, testTerminationTrigger);
			times = 1;
			
			stat.setString(8, testReportName);
			times = 1;
			
			stat.setString(9, null);
			times = 1;
			
			stat.setObject(10, null);
			times = 1;
			
			stat.setString(11, null);
			times = 1;
			
			stat.setString(12, null);
			times = 1;
			
			stat.setString(13, null);
			times = 1;
			
			stat.setString(14, null);
			times = 1;
			
			stat.setString(15, null);
			times = 1;
			
			stat.setString(16, testId);
			times = 1;
			
			stat.setString(17, testOpName);
			times = 1;
			
			stat.setString(18, testOpStatus);
			times = 1;
			
			stat.setObject(19, testState);
			times = 1;
			
			stat.execute();
			times = 1;
			
			stat.clearParameters();
			times = 1;
			
			connection.commit();
			times = 1;
			
		}};
		connector.dispose();
	}
	
	// pcMarshaller is not longer null, context.createMarshaller() gets called one times
	@Test
	public void sendPCReportWithPlainNotNull(@Mocked final DriverManager driverManager,@Mocked final Connection connection, @Mocked final JAXBContext context) throws SQLException, ImplementationException, JAXBException {
		final SubscriberConnector connector = new SqlSubscriberConnector();
		setField(connector, "PLAIN", "INSERT INTO test (plain) VALUES (?)");
		
		new NonStrictExpectations() {{
			DriverManager.getConnection("jdbc:");
			result = connection;
			
			connection.isClosed();
			result = false;
		}};
		
		final Marshaller oldMarshaller = getField(connector, "pcMarshaller");
		
		connector.send(new PCReports(){{
			reports = new Reports(){{				
				getReport().add(new PCReport(){{					
					eventReports = new EventReports() {{
						getEventReport().add(new PCEventReport() {{							
							opReports = new PCOpReports() {{
								getOpReport().add(new PCOpReport() {{
								}});
							}};
						}});
					}}; 
				}});
			}};
		}});
		
		Assert.assertNotEquals(oldMarshaller, getField(connector, "pcMarshaller"));
		new Verifications() {{
			context.createMarshaller();
			times = 1;
		}};
	}
	
	// executeUpdate gets called with the correct String, the returned Integer is > -1
	@Test
	public void testClearWhenClearTrue(@Mocked final Connection connection, @Mocked final PreparedStatement stat) throws SQLException, InvalidURIException, ImplementationException, URISyntaxException {
		final SubscriberConnector connector = new SqlSubscriberConnector();
		
		setField(connector, "clear", true);
		setField(connector, "table", "test");
		setField(connector, "delete", "DELETE FROM test");
		setField(connector, "connection", connection);
				
		new NonStrictExpectations() {{
			connection.isClosed();
			result = false;			
		}};
		
		int c = ((SqlSubscriberConnector) connector).clear();
		
		new Verifications() {{
			String del;
			stat.executeUpdate(del = withCapture());
			Assert.assertEquals("DELETE FROM test", del);
		}};
		Assert.assertTrue(c>-1);
	}
	
	@Test
	public void testClearWhenClearFalse() throws SQLException {
		final SubscriberConnector connector = new SqlSubscriberConnector();
		setField(connector, "clear", false);
		
		int cfalse = ((SqlSubscriberConnector) connector).clear();
		Assert.assertEquals(-1, cfalse);
	}
	
	// Statement gets executed with the correct String and the Exception has a certain message
	@Test
	public void testDisposeWhenDropIsTrue(@Mocked final Connection connection, @Mocked final PreparedStatement stat) throws URISyntaxException, InvalidURIException, ImplementationException, SQLException {
		final SubscriberConnector connector = new SqlSubscriberConnector();
				
		new NonStrictExpectations() {{
			stat.execute("DROP TABLE test");
			result = new SQLException("test");
			
			connection.isClosed();
			result = false;			
		}};
		
		setField(connector, "connection", connection);
		setField(connector, "drop", true);
		setField(connector, "table", "test");
		
		try{
			connector.dispose();
			Assert.fail("Exception expected");
		}catch(ImplementationException e){
			Assert.assertEquals("Failed to drop table: test", e.getMessage());
		}		
	}
	
	// Connection gets closed and is set to null
	@Test
	public void testDisposeWhenConnectionNotNull(@Mocked final Connection connection) throws URISyntaxException, InvalidURIException, ImplementationException, SQLException {
		final SubscriberConnector connector = new SqlSubscriberConnector();
		setField(connector, "connection", connection);
				
		connector.dispose();
		final Connection newConnection = getField(connector, "connection");
		new Verifications() {{			
			connection.close();
			times = 1;			
		}};	
		Assert.assertNull(newConnection);
	}
	
	// Catching an Implementation message if the method throws an SQLException 
	@Test (expected = ImplementationException.class)
	public void testDisposeWhenConnectionNotNullWithException(@Mocked final Connection connection) throws URISyntaxException, InvalidURIException, ImplementationException, SQLException {
		final SubscriberConnector connector = new SqlSubscriberConnector();
		setField(connector, "connection", connection);	
		
		new NonStrictExpectations() {{
			connection.close();
			result = new SQLException();
		}};
		
		connector.dispose();
	}
	
	// removes the correct storage
	@Test
	public void testDisposeWhenStorageNotNull(@Mocked final Storage storage) throws ImplementationException {
		final SubscriberConnector connector = new SqlSubscriberConnector();
		setField(connector, "storage", "storage");
		
		connector.dispose();
		
		new Verifications() {{
			String name;
			storage.remove(name = withCapture());
			Assert.assertEquals(getField(connector, "storage"), name);
		}};
	}
	
	@Test
	public void testSendEcReportWithNullValues() throws URISyntaxException,
			InvalidURIException, ImplementationException, SQLException {

		String queryParams = "&table=test&report=report_name&epc=epc&tag=tag&rawHex=raw_hex";
		URI uri = new URI(this.uri + queryParams);

		SubscriberConnector connector = new SqlSubscriberConnector();
		connector.init(uri, new HashMap<String, String>());

		connector.send(new ECReports() {
			{
				aleid = "Middleware";
				date = new Date();
				specName = "Sql Subscriber Connector Test";
				reports = new Reports() {
					{
						getReport().add(new ECReport() {
							{
								reportName = "EC Report Test with Null Values";
								getGroup().add(new ECReportGroup() {
									{
										groupList = new ECReportGroupList() {
											{
												getMember().add(new ECReportGroupListMember() {
													{
														epc = null;
														tag = null;
														rawHex = null;
													};
												});
											}
										};
									}
								});
							}
						});
					}
				};

			}
		});
		connector.dispose();
	}

	@Test
	public void testSendEcReport() throws URISyntaxException, InvalidURIException, ImplementationException, SQLException {
		String queryParams = "&table=test&spec=spec&date=date&totalMilliseconds=total&initiationCondition=init_condition&initiationTrigger=init_trigger&terminationCondition=term_condition&terminationTrigger=term_trigger&report=report_name&group=group_name&count=group_count&epc=epc&tag=tag&rawHex=raw_hex&field=field_1";
		URI uri = new URI(this.uri + queryParams);
		ECReports ecReports;

		SubscriberConnector connector = new SqlSubscriberConnector();
		connector.init(uri, new HashMap<String, String>());

		connector.send(ecReports = new ECReports() {
			{
				aleid = "Middleware";
				date = new Date();
				initiationCondition = "TRIGGER";
				initiationTrigger = "urn.havis...";
				terminationCondition = "TRIGGER";
				terminationTrigger = "urn.havis...";
				specName = "Test";
				totalMilliseconds = 99999;
				reports = new Reports() {
					{
						getReport().add(new ECReport() {
							{
								reportName = "EC Report Test";
								getGroup().add(new ECReportGroup() {
									{
										groupList = new ECReportGroupList() {
											{
												getMember().add(new ECReportGroupListMember() {
													{
														epc = new EPC("urn:epc:id");
														tag = new EPC("urn:epc:tag");
														rawHex = new EPC("urn:epc:raw");
														extension = new ECReportGroupListMemberExtension() {
															{
																fieldList = new FieldList() {
																	{
																		getField().add(new ECReportMemberField() {
																			{
																				value = "FieldValue1";
																			}
																		});
																	};
																};
															}
														};
													}
												});
											}
										};
										groupCount = new ECReportGroupCount() {
											{
												count = 9;
											}
										};
									}
								});
							}
						});
					}
				};

			}
		});

		ResultSet resultSet = null;
		Connection connection = DriverManager.getConnection(url);
		Statement statement = connection.createStatement();
		if (statement.execute("SELECT * FROM test")) {
			resultSet = statement.getResultSet();
		}

		Assert.assertNotNull(resultSet);
		if (resultSet != null) {
			while (resultSet.next()) {
				Assert.assertEquals(ecReports.getInitiationCondition(), resultSet.getString("INIT_CONDITION"));
				Assert.assertEquals(ecReports.getInitiationTrigger(), resultSet.getString("INIT_TRIGGER"));
				Assert.assertEquals(ecReports.getSpecName(), resultSet.getString("SPEC"));
			}
		}

		connector.dispose();
	}

	@Test
	public void testSendCcReport() throws URISyntaxException, InvalidURIException, ImplementationException, SQLException {
		String queryParams = "&table=test&spec=spec&date=date&totalMilliseconds=total&initiationCondition=init_condition&initiationTrigger=init_trigger&terminationCondition=term_condition&terminationTrigger=term_trigger&report=report_name&id=report_id&name=op_name&status=op_status&data=data";
		URI uri = new URI(this.uri + queryParams);
		CCReports ccReports;

		SubscriberConnector connector = new SqlSubscriberConnector();
		connector.init(uri, new HashMap<String, String>());

		connector.send(ccReports = new CCReports() {
			{
				aleid = "Middleware";
				date = new Date();
				initiationCondition = "TRIGGER";
				initiationTrigger = "urn.havis...";
				specName = "CCReportTest";
				terminationCondition = "TRIGGER";
				terminationTrigger = "urn.havis...";
				specName = "Test";
				totalMilliseconds = 88888;
				cmdReports = new CmdReports() {
					{
						getCmdReport().add(new CCCmdReport() {
							{
								cmdSpecName = "CC Cmd Spec Name";
								tagReports = new TagReports() {
									{
										getTagReport().add(new CCTagReport() {
											{
												id = "";
												opReports = new OpReports() {
													{
														getOpReport().add(new CCOpReport() {
															{
																data = "";
																opStatus = "SUCCESS";
																opName = "SQL Subscriber Connector Test CC";
															}
														});
													}
												};
											}
										});
									}

								};
							}
						});
					}
				};

			}
		});

		ResultSet resultSet = null;
		Connection connection = DriverManager.getConnection(url);
		Statement statement = connection.createStatement();
		if (statement.execute("SELECT * FROM test")) {
			resultSet = statement.getResultSet();
		}

		Assert.assertNotNull(resultSet);
		if (resultSet != null) {
			while (resultSet.next()) {
				Assert.assertEquals(ccReports.getInitiationCondition(), resultSet.getString("INIT_CONDITION"));
				Assert.assertEquals(ccReports.getInitiationTrigger(), resultSet.getString("INIT_TRIGGER"));
				Assert.assertEquals(ccReports.getSpecName(), resultSet.getString("SPEC"));
			}
		}

		connector.dispose();
	}

	@Test
	public void testSendPcReport() throws URISyntaxException, InvalidURIException, ImplementationException, SQLException {
		String queryParams = "&table=test&spec=spec&date=date&totalMilliseconds=total&initiationCondition=init_condition&initiationTrigger=init_trigger&terminationCondition=term_condition&terminationTrigger=term_trigger&report=report_name&id=report_id&name=op_name&status=op_status&data=data&plain=plain";
		URI uri = new URI(this.uri + queryParams);

		SubscriberConnector connector = new SqlSubscriberConnector();
		connector.init(uri, new HashMap<String, String>());

		connector.send(new PCReports() {
			{
				aleid = "Middleware";
				date = new Date();
				initiationCondition = "TRIGGER";
				initiationTrigger = "urn.havis...";
				specName = "PCReportTest";
				terminationCondition = "TRIGGER";
				terminationTrigger = "urn.havis...";
				specName = "Test";
				totalMilliseconds = 77777;
				reports = new PCReports.Reports() {
					{
						getReport().add(new PCReport() {
							{
								reportName = "PC Report Test";
								eventReports = new EventReports() {
									{
										getEventReport().add(new PCEventReport() {
											{
												id = "";
												opReports = new PCOpReports() {
													{
														getOpReport().add(new PCOpReport() {
															{
																opStatus = "SUCCESS";
																opName = "SQL Subscriber Connector Test PC";
																state = true;
															}
														});
													}
												};
											}
										});
									}
								};
							}
						});
					}
				};
			}
		});

		ResultSet resultSet = null;
		Connection connection = DriverManager.getConnection(url);
		Statement statement = connection.createStatement();
		if (statement.execute("SELECT * FROM test")) {
			resultSet = statement.getResultSet();
		}

		Assert.assertNotNull(resultSet);
		if (resultSet != null) {
			while (resultSet.next()) {
				// Assert.assertEquals(pcReports.getInitiationCondition(),
				// resultSet.getString("INIT_CONDITION"));
				// Assert.assertEquals(pcReports.getInitiationTrigger(),
				// resultSet.getString("INIT_TRIGGER"));
				// Assert.assertEquals(pcReports.getSpecName(),
				// resultSet.getString("SPEC"));
			}
		}

		connector.dispose();
	}

	@Test
	public void testSendWithClosedConnection(@Mocked final Connection connection, @Mocked final DriverManager manager) throws SQLException,
			UnsupportedEncodingException, InvalidURIException, ImplementationException, URISyntaxException {

		final SqlSubscriberConnector connector = new SqlSubscriberConnector();
		Deencapsulation.setField(connector, "connection", connection);
		Deencapsulation.setField(connector, "connectionString", url);

		new NonStrictExpectations() {
			{
				DriverManager.getConnection(this.<String> withNotNull());
				result = connection;

				connection.isClosed();
				result = true;
			}
		};

		connector.send(new ECReports());
		connector.send(new CCReports());
		connector.send(new PCReports());
		connector.dispose();

		new Verifications() {
			{
				connection.setAutoCommit(false);
				times = 3;
			}
		};
	}

	@Test(expected = ImplementationException.class)
	public void testSendEcReportsThrowsException(@Mocked final Connection connection) throws ImplementationException, SQLException {

		final SqlSubscriberConnector connector = new SqlSubscriberConnector();
		Deencapsulation.setField(connector, "connection", connection);

		new NonStrictExpectations() {
			{
				connection.isClosed();
				result = new Exception();
			}
		};

		connector.send(new ECReports());
	}

	@Test(expected = ImplementationException.class)
	public void testSendCcReportsThrowsException(@Mocked final Connection connection) throws ImplementationException, SQLException {

		final SqlSubscriberConnector connector = new SqlSubscriberConnector();
		Deencapsulation.setField(connector, "connection", connection);

		new NonStrictExpectations() {
			{
				connection.isClosed();
				result = new Exception();
			}
		};

		connector.send(new CCReports());
	}

	@Test(expected = ImplementationException.class)
	public void testSendPcReportsThrowsException(@Mocked final Connection connection) throws ImplementationException, SQLException {

		final SqlSubscriberConnector connector = new SqlSubscriberConnector();
		Deencapsulation.setField(connector, "connection", connection);

		new NonStrictExpectations() {
			{
				connection.isClosed();
				result = new Exception();
			}
		};

		connector.send(new PCReports());
	}

	@Test(expected = ImplementationException.class)
	public void testDisposeThrowsSqlException(@Mocked final Connection connection) throws SQLException, ImplementationException, UnsupportedEncodingException {

		SqlSubscriberConnector connector = new SqlSubscriberConnector();
		Deencapsulation.setField(connector, "connection", connection);

		new NonStrictExpectations() {
			{
				connection.close();
				result = new SQLException();
			}
		};

		connector.dispose();
	}

	@Test(expected = InvalidURIException.class)
	public void testNoConnectionString() throws URISyntaxException, InvalidURIException, ImplementationException {
		String queryParams = "&table=test&spec=spec&date=date&totalMilliseconds=total&initiationCondition=init_condition&initiationTrigger=init_trigger&terminationCondition=term_condition&terminationTrigger=term_trigger&plain=plain";
		URI uri = new URI("sql://?connection=" + queryParams);

		SubscriberConnector connector = new SqlSubscriberConnector();
		connector.init(uri, new HashMap<String, String>());
		connector.dispose();
	}

	@Test(expected = InvalidURIException.class)
	public void testNoTable() throws URISyntaxException, InvalidURIException, ImplementationException {
		String queryParams = "&spec=spec&date=date&totalMilliseconds=total&initiationCondition=init_condition&initiationTrigger=init_trigger&terminationCondition=term_condition&terminationTrigger=term_trigger&plain=plain";
		URI uri = new URI(this.uri + queryParams);

		SubscriberConnector connector = new SqlSubscriberConnector();
		connector.init(uri, new HashMap<String, String>());
		connector.dispose();
	}

	@Test(expected = InvalidURIException.class)
	public void testNoColumnNorPlain() throws URISyntaxException, InvalidURIException, ImplementationException {
		String queryParams = "&table=test";
		URI uri = new URI(this.uri + queryParams);

		SubscriberConnector connector = new SqlSubscriberConnector();
		connector.init(uri, new HashMap<String, String>());

		connector.dispose();
	}

	private ECReports createEcReports() {
		ECReports ecReports = new ECReports();
		havis.middleware.ale.service.ec.ECReports.Reports reports = new havis.middleware.ale.service.ec.ECReports.Reports();
		List<ECReport> ecReportList = new ArrayList<>();
		ECReport ecReport = new ECReport();
		List<ECReportGroup> groupList = new ArrayList<>();
		ECReportGroup ecReportGroup = new ECReportGroup();
		ECReportGroupCount ecReportGroupCount = new ECReportGroupCount();
		ECReportGroupList ecGroupList = new ECReportGroupList();
		ECReportGroupListMember ecReportGroupListMember = new ECReportGroupListMember();
		List<ECReportGroupListMember> memberList = new ArrayList<>();
		ECReportGroupListMemberExtension ecReportGroupListMemberExtension = new ECReportGroupListMemberExtension();
		havis.middleware.ale.service.ec.ECReportGroupListMemberExtension.FieldList fieldList = new havis.middleware.ale.service.ec.ECReportGroupListMemberExtension.FieldList();
		List<ECReportMemberField> memberFieldList = new ArrayList<>();
		ECReportMemberField ecReportMemberField = new ECReportMemberField();

		EPC tag = new EPC();
		EPC epc = new EPC();
		EPC rawHex = new EPC();

		ecReportGroupListMember.setTag(tag);
		ecReportGroupListMember.setEpc(epc);
		ecReportGroupListMember.setRawHex(rawHex);

		ecReportGroupListMemberExtension.setFieldList(fieldList);
		ecReportGroupListMember.setExtension(ecReportGroupListMemberExtension);

		memberFieldList.add(ecReportMemberField);
		groupList.add(ecReportGroup);
		memberList.add(ecReportGroupListMember);
		ecReportGroup.setGroupCount(ecReportGroupCount);
		ecGroupList.getMember().addAll(memberList);
		ecReportGroup.setGroupList(ecGroupList);
		ecReport.getGroup().addAll(groupList);
		ecReportList.add(ecReport);
		reports.getReport().addAll(ecReportList);
		ecReports.setReports(reports);

		fieldList.getField().addAll(memberFieldList);

		ecReports.setSpecName(resSpecName);
		ecReports.setDate(resDate);
		ecReports.setTotalMilliseconds(resTotalMilliseconds);
		ecReports.setInitiationCondition(resInitiationCondition);
		ecReports.setInitiationTrigger(resInitiationTrigger);
		ecReports.setTerminationCondition(resTerminationCondition);
		ecReports.setTerminationTrigger(resTerminationTrigger);

		ecReport.setReportName(resReportName);
		ecReportGroup.setGroupName(resGroupName);
		ecReportGroupCount.setCount(resCount);

		ecReportGroupListMember.getEpc().setValue(resValueEpc);
		ecReportGroupListMember.getTag().setValue(resValueTag);
		ecReportGroupListMember.getRawHex().setValue(resValueRaw);

		ecReportMemberField.setName(resName);
		ecReportMemberField.setValue(resValue);

		return ecReports;
	}

	static ECReports getReport(final String _epc, final Date _date, final long _totalMilliseconds) {
		return new ECReports() {
			{
				date = _date;
				totalMilliseconds = _totalMilliseconds;
				reports = new Reports() {
					{
						getReport().add(new ECReport() {
							{
								getGroup().add(new ECReportGroup() {
									{
										groupList = new ECReportGroupList() {
											{
												getMember().add(new ECReportGroupListMember() {
													{
														epc = new EPC(_epc);
													}
												});
											}
										};
									}
								});
							}
						});
					}
				};
			}
		};
	}

	@Test
	public void marshallTest() throws InvalidURIException, ImplementationException, URISyntaxException, SQLException, IOException {
		SqlSubscriberConnector connector = new SqlSubscriberConnector();

		String uri = "sql://?connection=" + url + "&table=test&epc=epc&date=date&storage=test";
		connector.init(new URI(uri), new HashMap<String, String>());

		connector.send(getReport("epc1", new Date(), 1000));
		connector.send(getReport("epc2", new Date(), 1111));

		connector.marshal(new FileWriter("/tmp/report.csv"), -1, 0);
	}

	// @Test
	public void testMsSql(@Mocked final Connection connection, @Mocked final DriverManager manager)
			throws SQLException, UnsupportedEncodingException, InvalidURIException, ImplementationException, URISyntaxException {

		final SqlSubscriberConnector connector = new SqlSubscriberConnector();
		connector.init(new URI(
				"sql:///?connection=jdbc%3Asqlserver%3A%2F%2F172.16..203%3A1433%3BdatabaseName%3Dnavrep%3Buser%3Dideharsrfidwebsrv%3Bpassword%3DStart1010!%3B&table=REP+DE%24RFID+Reader+Import&epc=RFID+Tag"),
				null);

		new NonStrictExpectations() {
			{
				DriverManager.getConnection(this.<String>withNotNull());
				result = connection;
			}
		};

		ECReports reports = new ECReports();
		ECReports.Reports r = new ECReports.Reports();
		ECReport e = new ECReport();
		ECReportGroup group = new ECReportGroup();
		ECReportGroupList list = new ECReportGroupList();
		ECReportGroupListMember member = new ECReportGroupListMember();
		member.setEpc(new EPC("urn:epc:raw:96.xE2801160600002112F516172"));
		list.getMember().add(member);
		group.setGroupList(list);
		e.getGroup().add(group);
		r.getReport().add(e);
		reports.setReports(r);
		connector.send(reports);
		connector.dispose();
	}
}
