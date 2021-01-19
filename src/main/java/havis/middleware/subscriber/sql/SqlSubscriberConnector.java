package havis.middleware.subscriber.sql;

import havis.middleware.ale.base.NamespaceXMLStreamWriter;
import havis.middleware.ale.base.exception.ALEException;
import havis.middleware.ale.base.exception.ImplementationException;
import havis.middleware.ale.base.exception.InvalidURIException;
import havis.middleware.ale.service.cc.CCCmdReport;
import havis.middleware.ale.service.cc.CCOpReport;
import havis.middleware.ale.service.cc.CCReports;
import havis.middleware.ale.service.cc.CCTagReport;
import havis.middleware.ale.service.ec.ECReport;
import havis.middleware.ale.service.ec.ECReportGroup;
import havis.middleware.ale.service.ec.ECReportGroupListMember;
import havis.middleware.ale.service.ec.ECReportMemberField;
import havis.middleware.ale.service.ec.ECReports;
import havis.middleware.ale.service.pc.PCEventReport;
import havis.middleware.ale.service.pc.PCOpReport;
import havis.middleware.ale.service.pc.PCReport;
import havis.middleware.ale.service.pc.PCReports;
import havis.middleware.ale.subscriber.SubscriberConnector;
import havis.middleware.subscriber.sql.rest.Storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URI;
import java.net.URLDecoder;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvResultSetWriter;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.CsvContext;

public class SqlSubscriberConnector implements SubscriberConnector {

	private final static Logger log = Logger.getLogger(SqlSubscriberConnector.class.getName());

	private static havis.middleware.ale.service.ec.ObjectFactory ecFactory = new havis.middleware.ale.service.ec.ObjectFactory();
	private static havis.middleware.ale.service.cc.ObjectFactory ccFactory = new havis.middleware.ale.service.cc.ObjectFactory();
	private static havis.middleware.ale.service.pc.ObjectFactory pcFactory = new havis.middleware.ale.service.pc.ObjectFactory();

	private Marshaller ecMarshaller, ccMarshaller, pcMarshaller;

	private final static String JDBC_PREFIX = "jdbc:";
	private final static String CREATE = "CREATE TABLE %s (%s)";
	private final static String INSERT = "INSERT INTO %s (%s) VALUES (%s)";
	private static final String SELECT = "SELECT %s FROM %s LIMIT ? OFFSET ?";
	private static final String DELETE = "DELETE FROM %s";
	private static final String DROP = "DROP TABLE %s";

	private final Pattern STRICT_IDENTIFIER_PATTERN = Pattern.compile("^[a-z][\\w\\-]*$", Pattern.CASE_INSENSITIVE);

	private final static CellProcessor processor = new CellProcessor() {

		@SuppressWarnings("unchecked")
		@Override
		public String execute(Object value, CsvContext context) {
			if (value instanceof Clob) {
				Clob clob = (Clob) value;
				try {
					InputStream stream = clob.getAsciiStream();
					byte[] bytes = new byte[stream.available()];
					stream.read(bytes);
					return new String(bytes);
				} catch (Exception e) {
					log.log(Level.FINE, "Failed to read column data", e);
				}
			}
			return null;
		}
	};

	class ColumnMap {
		protected int spec, date, totalMilliseconds, initiationCondition, initiationTrigger, terminationCondition, terminationTrigger, report;
		// EC
		protected int group, count, epc, tag, rawHex;
		protected int field, size;
		// CC + PC
		protected int id, name, status, data;
	}

	private ColumnMap map = new ColumnMap();

	private Connection connection;
	private Pattern identifierPattern = null;
	private String identifierQuoteFormat;
	private String table;
	private String connectionString, storage, select, delete;
	private String TEXT;
	private String PLAIN;

	boolean init, drop, clear;

	@Override
	public void init(URI uri, Map<String, String> properties) throws InvalidURIException, ImplementationException {
		identifierQuoteFormat = "\"%s\"";
		identifierPattern = null;
		String plain = null;
		StringBuilder columns = new StringBuilder(), values = new StringBuilder(), types = new StringBuilder();
		try {
			Map<String, String> param = split(uri.getRawQuery());
			for (Entry<String, String> entry : param.entrySet()) {
				if ("connection".equalsIgnoreCase(entry.getKey())) {
					connectionString = URLDecoder.decode(entry.getValue(), "UTF-8");
					if (connectionString.startsWith(JDBC_PREFIX)) {
						int columnIndex = connectionString.indexOf(':', JDBC_PREFIX.length());
						if (columnIndex > 0) {
							switch (connectionString.substring(JDBC_PREFIX.length(), columnIndex).toLowerCase()) {
							case "mysql":
								identifierQuoteFormat = "`%s`";
								Class.forName("com.mysql.jdbc.Driver").newInstance();
								break;
							case "sqlserver":
								identifierQuoteFormat = "[%s]";
								Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance();
								break;
							case "h2":
								// quoting makes identifiers case sensitive, so we don't allow quoting
								// see: http://h2database.com/html/grammar.html#name
								identifierQuoteFormat = "%s";
								identifierPattern = STRICT_IDENTIFIER_PATTERN;
								break;
							}
						}
					}
					break;
				}
			}

			int index = 0;
			for (Entry<String, String> entry : param.entrySet()) {
				switch (entry.getKey()) {
				case "connection":
					break;
				case "table":
					if (match(entry)) {
						table = String.format(identifierQuoteFormat, entry.getValue());
					}
					break;
				case "plain":
					if (match(entry)) {
						plain = String.format(identifierQuoteFormat, entry.getValue());
						types.append(",");
						types.append(plain);
						types.append(" TEXT");
					}
					break;
				case "storage":
					if (match(entry)) {
						storage = entry.getValue();
					}					
					break;
				case "init":
					init = Boolean.parseBoolean(entry.getValue());
					break;
				case "drop":
					drop = Boolean.parseBoolean(entry.getValue());
					break;
				case "clear":
					clear = Boolean.parseBoolean(entry.getValue());
					break;
				case "spec":
					if (match(entry)) {
						columns.append(',');
						columns.append(String.format(identifierQuoteFormat, entry.getValue()));
						values.append(",?");
						types.append(",");
						types.append(String.format(identifierQuoteFormat, entry.getValue()));
						types.append(" TEXT");
						map.spec = ++index;
					}
					break;
				case "date":
					if (match(entry)) {
						columns.append(',');
						columns.append(String.format(identifierQuoteFormat, entry.getValue()));
						values.append(",?");
						types.append(",");
						types.append(String.format(identifierQuoteFormat, entry.getValue()));
						types.append(" TIMESTAMP");
						map.date = ++index;
					}
					break;
				case "totalMilliseconds":
					if (match(entry)) {
						columns.append(',');
						columns.append(String.format(identifierQuoteFormat, entry.getValue()));
						values.append(",?");
						types.append(",");
						types.append(String.format(identifierQuoteFormat, entry.getValue()));
						types.append(" LONG");
						map.totalMilliseconds = ++index;
					}
					break;
				case "initiationCondition":
					if (match(entry)) {
						columns.append(',');
						columns.append(String.format(identifierQuoteFormat, entry.getValue()));
						values.append(",?");
						types.append(",");
						types.append(String.format(identifierQuoteFormat, entry.getValue()));
						types.append(" TEXT");
						map.initiationCondition = ++index;
					}
					break;
				case "initiationTrigger":
					if (match(entry)) {
						columns.append(',');
						columns.append(String.format(identifierQuoteFormat, entry.getValue()));
						values.append(",?");
						types.append(",");
						types.append(String.format(identifierQuoteFormat, entry.getValue()));
						types.append(" TEXT");
						map.initiationTrigger = ++index;
					}
					break;
				case "terminationCondition":
					if (match(entry)) {
						columns.append(',');
						columns.append(String.format(identifierQuoteFormat, entry.getValue()));
						values.append(",?");
						types.append(",");
						types.append(String.format(identifierQuoteFormat, entry.getValue()));
						types.append(" TEXT");
						map.terminationCondition = ++index;
					}
					break;
				case "terminationTrigger":
					if (match(entry)) {
						columns.append(',');
						columns.append(String.format(identifierQuoteFormat, entry.getValue()));
						values.append(",?");
						types.append(",");
						types.append(String.format(identifierQuoteFormat, entry.getValue()));
						types.append(" TEXT");
						map.terminationTrigger = ++index;
					}
					break;
				case "report":
					if (match(entry)) {
						columns.append(',');
						columns.append(String.format(identifierQuoteFormat, entry.getValue()));
						values.append(",?");
						types.append(",");
						types.append(String.format(identifierQuoteFormat, entry.getValue()));
						types.append(" TEXT");
						map.report = ++index;
					}
					break;
				// EC
				case "group":
					if (match(entry)) {
						columns.append(',');
						columns.append(String.format(identifierQuoteFormat, entry.getValue()));
						values.append(",?");
						types.append(",");
						types.append(String.format(identifierQuoteFormat, entry.getValue()));
						types.append(" TEXT");
						map.group = ++index;
					}
					break;
				case "count":
					if (match(entry)) {
						columns.append(',');
						columns.append(String.format(identifierQuoteFormat, entry.getValue()));
						values.append(",?");
						types.append(",");
						types.append(String.format(identifierQuoteFormat, entry.getValue()));
						types.append(" INT");
						map.count = ++index;
					}
					break;
				case "epc":
					if (match(entry)) {
						columns.append(',');
						columns.append(String.format(identifierQuoteFormat, entry.getValue()));
						values.append(",?");
						types.append(",");
						types.append(String.format(identifierQuoteFormat, entry.getValue()));
						types.append(" TEXT");
						map.epc = ++index;
					}
					break;
				case "tag":
					if (match(entry)) {
						columns.append(',');
						columns.append(String.format(identifierQuoteFormat, entry.getValue()));
						values.append(",?");
						types.append(",");
						types.append(String.format(identifierQuoteFormat, entry.getValue()));
						types.append(" TEXT");
						map.tag = ++index;
					}
					break;
				case "rawHex":
					if (match(entry)) {
						columns.append(',');
						columns.append(String.format(identifierQuoteFormat, entry.getValue()));
						values.append(",?");
						types.append(",");
						types.append(String.format(identifierQuoteFormat, entry.getValue()));
						types.append(" TEXT");
						map.rawHex = ++index;
					}
					break;
				case "field":
					String[] fields = URLDecoder.decode(entry.getValue(), "UTF-8").split(",");
					
					if (match(fields, entry.getKey())){
						for (String field : fields) {
							map.size++;
							columns.append(',');
							columns.append(String.format(identifierQuoteFormat, field));
							values.append(",?");
							types.append(",");
							types.append(String.format(identifierQuoteFormat, field));
							types.append(" TEXT");
						}
						map.field = ++index;
						index += map.size - 1;
					}
					break;
				// CC + PC
				case "id":
					if (match(entry)) {
						columns.append(',');
						columns.append(String.format(identifierQuoteFormat, entry.getValue()));
						values.append(",?");
						types.append(",");
						types.append(String.format(identifierQuoteFormat, entry.getValue()));
						types.append(" TEXT");
						map.id = ++index;
					}
					break;
				case "name":
					if (match(entry)) {
						columns.append(',');
						columns.append(String.format(identifierQuoteFormat, entry.getValue()));
						values.append(",?");
						types.append(",");
						types.append(String.format(identifierQuoteFormat, entry.getValue()));
						types.append(" TEXT");
						map.name = ++index;
					}
					break;
				case "status":
					if (match(entry)) {
						columns.append(',');
						columns.append(String.format(identifierQuoteFormat, entry.getValue()));
						values.append(",?");
						types.append(",");
						types.append(String.format(identifierQuoteFormat, entry.getValue()));
						types.append(" TEXT");
						map.status = ++index;
					}
					break;
				case "data":
					if (match(entry)) {
						columns.append(',');
						columns.append(String.format(identifierQuoteFormat, entry.getValue()));
						values.append(",?");
						types.append(",");
						types.append(String.format(identifierQuoteFormat, entry.getValue()));
						types.append(" TEXT");
						map.data = ++index;
					}
					break;
				default:
					throw new InvalidURIException("Parameter '" + (entry.getKey()) + "' is unknown!");
				}
			}

			if (connectionString == null)
				throw new InvalidURIException("No connection specified");

			if (table == null)
				throw new InvalidURIException("No table specified");

			if (columns.length() > 0 || plain != null) {
				if (plain != null)
					PLAIN = String.format(INSERT, table, plain, "?");

				if (columns.length() > 0) {
					String column = columns.substring(1);
					TEXT = String.format(INSERT, table, column, values.toString().substring(1));
					if (storage != null) {
						select = String.format(SELECT, column, table);
						Storage.INSTANCE.put(storage, this);
						if (clear)
							delete = String.format(DELETE, table);
					}
				}
				if (init) {
					try {
						connect();

						String init = String.format(CREATE, table, types.substring(1));

						try (Statement stmt = connection.createStatement()) {
							stmt.execute(init);
						}
					} catch (SQLException e) {
						throw new InvalidURIException("Failed to initialize table: " + e.getMessage());
					}
				}
			} else {
				throw new InvalidURIException("No column nor plain output specified");
			}

		} catch (ALEException e) {
			throw e;
		} catch (Exception e) {
			throw new ImplementationException(e);
		}
	}

	protected void connect() throws SQLException {
		if (connection == null || connection.isClosed()) {
			connection = DriverManager.getConnection(connectionString);
			connection.setAutoCommit(false);
		}
	}

	private Map<String, String> split(String query) throws UnsupportedEncodingException, InvalidURIException {
		final Map<String, String> map = new LinkedHashMap<String, String>();
		if (query != null && query.length() > 0) {
			for (String pair : query.split("&")) {
				final int index = pair.indexOf('=');
				if (index > 0 && index < pair.length() - 1) {
					map.put(URLDecoder.decode(pair.substring(0, index), "UTF-8"), URLDecoder.decode(pair.substring(index + 1), "UTF-8"));
				} else
					throw new InvalidURIException("Value of field '" + (index > 0 ? pair.substring(0, index) : pair) + "' couldn't be empty");
			}
		}
		return map;
	}

	private boolean match(Entry<String, String> entry) throws InvalidURIException {
		if (identifierPattern == null)
			return true;
		Matcher matcher = identifierPattern.matcher(entry.getValue());
		if (matcher.matches())
			return true;
		throw new InvalidURIException("Column name '" + entry.getValue() + "' is invalid for field '" + entry.getKey() + "'");
	}
	
	private boolean match(String[] values, String key) throws InvalidURIException {
		for (int i = 0; i < values.length; i++) {
			Matcher matcher = identifierPattern != null ? identifierPattern.matcher(values[i]) : null;
			if (matcher == null || matcher.matches()) {
				if (i == (values.length)-1) {
					return true;
				}
				continue;
			} else {
				throw new InvalidURIException("Column name '" + values[i] + "' is invalid for field '" + key + "'");
			}
		}
		throw new InvalidURIException("Value of field '" + key + "' couldn't be empty");		
	}

	private void plain(Object reports, Marshaller marshaller) throws SQLException, JAXBException, IOException {
		try (StringWriter writer = new StringWriter()) {
			marshaller.marshal(reports, NamespaceXMLStreamWriter.create(writer));
			try (PreparedStatement stmt = connection.prepareStatement(PLAIN)) {
				stmt.setString(1, writer.toString());
				stmt.execute();
			}
		}
	}

	@Override
	public void send(ECReports reports) throws ImplementationException {
		try {
			connect();

			if (PLAIN != null) {
				if (ecMarshaller == null) {
					JAXBContext context = JAXBContext.newInstance(ECReports.class);
					ecMarshaller = context.createMarshaller();
				}
				plain(ecFactory.createECReports(reports), ecMarshaller);
			}

			if (TEXT != null) {
				if (reports.getReports() != null)
					try (PreparedStatement stmt = connection.prepareStatement(TEXT)) {
						for (ECReport report : reports.getReports().getReport()) {
							for (ECReportGroup group : report.getGroup()) {
								if (group.getGroupList().getMember() != null)
									for (ECReportGroupListMember member : group.getGroupList().getMember()) {
										if (map.spec > 0)
											stmt.setString(map.spec, reports.getSpecName());

										if (map.date > 0)
											stmt.setTimestamp(map.date, new Timestamp(reports.getDate().getTime()));

										if (map.totalMilliseconds > 0) {
											stmt.setLong(map.totalMilliseconds, reports.getTotalMilliseconds());
										}
										if (map.initiationCondition > 0) {
											stmt.setString(map.initiationCondition, reports.getInitiationCondition());
										}
										if (map.initiationTrigger > 0)
											stmt.setString(map.initiationTrigger, reports.getInitiationTrigger());

										if (map.terminationCondition > 0)
											stmt.setString(map.terminationCondition, reports.getTerminationCondition());

										if (map.terminationTrigger > 0)
											stmt.setString(map.terminationTrigger, reports.getTerminationTrigger());

										if (map.report > 0)
											stmt.setString(map.report, report.getReportName());

										if (map.group > 0)
											stmt.setString(map.group, group.getGroupName());

										if (map.count > 0)
											stmt.setObject(map.count, group.getGroupCount() != null ? group.getGroupCount().getCount() : null);

										if (map.epc > 0)
											stmt.setString(map.epc, member.getEpc() != null ? member.getEpc().getValue() : null);

										if (map.tag > 0)
											stmt.setString(map.tag, member.getTag() != null ? member.getTag().getValue() : null);

										if (map.rawHex > 0)
											stmt.setString(map.rawHex, member.getRawHex() != null ? member.getRawHex().getValue() : null);

										if (map.field > 0) {
											int index = map.field;
											if (member.getExtension() != null && member.getExtension().getFieldList() != null) {
												for (ECReportMemberField field : member.getExtension().getFieldList().getField()) {
													stmt.setString(index++, field.getValue());
													if (index == map.field + map.size)
														break;
												}
											}
											for (int i = index; i < map.field + map.size; i++)
												stmt.setString(i, null);
										}

										if (map.id > 0) {
											stmt.setString(map.id, null);
										}
										if (map.name > 0) {
											stmt.setString(map.name, null);
										}
										if (map.status > 0) {
											stmt.setString(map.status, null);
										}
										if (map.data > 0) {
											stmt.setString(map.data, null);
										}

										stmt.execute();
										stmt.clearParameters();
									}
							}
						}
					}
			}
			connection.commit();
		} catch (Exception e) {
			throw new ImplementationException(e);
		}

	}

	@Override
	public void send(CCReports ccReports) throws ImplementationException {
		try {
			connect();

			if (PLAIN != null) {
				if (ccMarshaller == null) {
					JAXBContext context = JAXBContext.newInstance(CCReports.class);
					ccMarshaller = context.createMarshaller();
				}
				plain(ccFactory.createCCReports(ccReports), ccMarshaller);
			}

			if (TEXT != null) {
				if (ccReports.getCmdReports() != null)
					try (PreparedStatement stmt = connection.prepareStatement(TEXT)) {
						for (CCCmdReport ccCmdreport : ccReports.getCmdReports().getCmdReport()) {
							if (ccCmdreport.getTagReports() != null)
								for (CCTagReport ccTagReport : ccCmdreport.getTagReports().getTagReport()) {
									if (ccTagReport.getOpReports() != null)
										for (CCOpReport ccOpReport : ccTagReport.getOpReports().getOpReport()) {
											if (map.spec > 0)
												stmt.setString(map.spec, ccReports.getSpecName());

											if (map.date > 0)
												stmt.setTimestamp(map.date, new Timestamp(ccReports.getDate().getTime()));

											if (map.totalMilliseconds > 0)
												stmt.setLong(map.totalMilliseconds, ccReports.getTotalMilliseconds());

											if (map.initiationCondition > 0)
												stmt.setString(map.initiationCondition, ccReports.getInitiationCondition());

											if (map.initiationTrigger > 0)
												stmt.setString(map.initiationTrigger, ccReports.getInitiationTrigger());

											if (map.terminationCondition > 0)
												stmt.setString(map.terminationCondition, ccReports.getTerminationCondition());

											if (map.terminationTrigger > 0)
												stmt.setString(map.terminationTrigger, ccReports.getTerminationTrigger());

											if (map.report > 0) {
												stmt.setString(map.report, ccCmdreport.getCmdSpecName());
											}

											if (map.group > 0)
												stmt.setString(map.group, null);

											if (map.count > 0)
												stmt.setObject(map.count, null);

											if (map.epc > 0)
												stmt.setString(map.epc, null);

											if (map.tag > 0)
												stmt.setString(map.tag, null);

											if (map.rawHex > 0)
												stmt.setString(map.rawHex, null);

											if (map.field > 0)
												for (int i = 0; i < map.size; i++)
													stmt.setString(map.field + i, null);

											if (map.id > 0) {
												stmt.setString(map.id, ccTagReport.getId());
											}
											if (map.name > 0) {
												stmt.setString(map.name, ccOpReport.getOpName());
											}
											if (map.status > 0) {
												stmt.setString(map.status, ccOpReport.getOpStatus());
											}
											if (map.data > 0) {
												stmt.setString(map.data, ccOpReport.getData());
											}

											stmt.execute();
											stmt.clearParameters();
										}
								}
						}
					}
			}
			connection.commit();
		} catch (Exception e) {
			throw new ImplementationException(e);
		}
	}

	@Override
	public void send(PCReports pcReports) throws ImplementationException {
		try {
			connect();

			if (PLAIN != null) {
				if (pcMarshaller == null) {
					JAXBContext context = JAXBContext.newInstance(PCReports.class);
					pcMarshaller = context.createMarshaller();
				}
				plain(pcFactory.createPCReports(pcReports), pcMarshaller);
			}

			if (TEXT != null) {
				if (pcReports.getReports() != null)
					try (PreparedStatement stmt = connection.prepareStatement(TEXT)) {
						for (PCReport pcReport : pcReports.getReports().getReport()) {
							if (pcReport.getEventReports() != null)
								for (PCEventReport pcEventReport : pcReport.getEventReports().getEventReport()) {
									if (pcEventReport.getOpReports() != null)
										for (PCOpReport pcOpReport : pcEventReport.getOpReports().getOpReport()) {
											if (map.spec > 0)
												stmt.setString(map.spec, pcReports.getSpecName());

											if (map.date > 0)
												stmt.setTimestamp(map.date, new Timestamp(pcReports.getDate().getTime()));

											if (map.totalMilliseconds > 0)
												stmt.setLong(map.totalMilliseconds, pcReports.getTotalMilliseconds());

											if (map.initiationCondition > 0)
												stmt.setString(map.initiationCondition, pcReports.getInitiationCondition());

											if (map.initiationTrigger > 0)
												stmt.setString(map.initiationTrigger, pcReports.getInitiationTrigger());

											if (map.terminationCondition > 0)
												stmt.setString(map.terminationCondition, pcReports.getTerminationCondition());

											if (map.terminationTrigger > 0)
												stmt.setString(map.terminationTrigger, pcReports.getTerminationTrigger());

											if (map.report > 0)
												stmt.setString(map.report, pcReport.getReportName());

											if (map.group > 0)
												stmt.setString(map.group, null);

											if (map.count > 0)
												stmt.setObject(map.count, null);

											if (map.epc > 0)
												stmt.setString(map.epc, null);

											if (map.tag > 0)
												stmt.setString(map.tag, null);

											if (map.rawHex > 0)
												stmt.setString(map.rawHex, null);

											if (map.field > 0)
												for (int i = 0; i < map.size; i++)
													stmt.setString(map.field + i, null);

											if (map.id > 0)
												stmt.setString(map.id, pcEventReport.getId());

											if (map.name > 0)
												stmt.setString(map.name, pcOpReport.getOpName());

											if (map.status > 0)
												stmt.setString(map.status, pcOpReport.getOpStatus());

											if (map.data > 0)
												stmt.setObject(map.data, pcOpReport.isState());

											stmt.execute();
											stmt.clearParameters();
										}
								}
						}
					}
			}
			connection.commit();
		} catch (Exception e) {
			throw new ImplementationException(e);
		}
	}

	public void marshal(Writer writer, int limit, int offset) throws SQLException, IOException {
		connect();

		try (PreparedStatement stmt = connection.prepareStatement(select)) {
			stmt.setInt(1, limit);
			stmt.setInt(2, offset);
			try (ResultSet rs = stmt.executeQuery()) {
				ResultSetMetaData data = rs.getMetaData();
				CellProcessor[] processors = new CellProcessor[data.getColumnCount()];
				for (int i = 0; i < data.getColumnCount(); i++)
					if (data.getColumnType(i + 1) == Types.CLOB)
						processors[i] = processor;
				try (CsvResultSetWriter csv = new CsvResultSetWriter(writer, CsvPreference.EXCEL_PREFERENCE)) {
					csv.write(rs, processors);
					csv.flush();
				}
			}
		}
	}

	public int clear() throws SQLException {
		if (clear) {
			connect();

			try (Statement stmt = connection.createStatement()) {
				return stmt.executeUpdate(delete);
			}
		}
		return -1;
	}

	@Override
	public void dispose() throws ImplementationException {

		if (drop) {
			try {
				connect();

				try (Statement statement = connection.createStatement()) {
					statement.execute(String.format(DROP, table));
				}
			} catch (SQLException e) {
				throw new ImplementationException("Failed to drop table: " + e.getMessage());
			}
		}

		if (connection != null) {
			try {

				connection.close();
				connection = null;
			} catch (SQLException e) {
				throw new ImplementationException(e);
			}
		}

		if (storage != null)
			Storage.INSTANCE.remove(storage);
	}
}
