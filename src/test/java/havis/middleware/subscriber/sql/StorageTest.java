package havis.middleware.subscriber.sql;

import havis.middleware.ale.base.exception.ImplementationException;
import havis.middleware.ale.base.exception.InvalidURIException;
import havis.middleware.subscriber.sql.rest.RESTApplication;
import havis.net.server.http.HttpService;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;
import java.util.Scanner;

public class StorageTest {

	public static final String CREATE = "CREATE TABLE test (plain TEXT, spec TEXT, date TIMESTAMP, init_condition TEXT, init_trigger TEXT, term_condition TEXT, term_trigger TEXT, report_name TEXT, total INT, group_name TEXT, group_count INT, epc TEXT, tag TEXT, raw_hex TEXT, field_1 TEXT, report_id TEXT, op_name TEXT, op_status TEXT, data TEXT)";
	public static final String DROP = "DROP TABLE test";

	public final String url = "jdbc:h2:mem:test";
	public final String uri = "sql://?connection=" + url;

	public void test() throws InvalidURIException, ImplementationException, URISyntaxException {

		HttpService service = new HttpService();
		service.start();
		service.add(new RESTApplication());

		SqlSubscriberConnector connector = new SqlSubscriberConnector();
		String uri = "sql://?connection=" + url
				+ "&table=test&storage=hurz&clear=True&init=True&drop=True&epc=epc&totalMilliseconds=total_milliseconds&date=date";
		connector.init(new URI(uri), new HashMap<String, String>());
		for (int i = 0; i < 1000; i++) {
			connector.send(SqlSubscriberConnectorTest.getReport("epc" + i, new Date(), 1000));
		}
		connector.send(SqlSubscriberConnectorTest.getReport("epc", new Date(), 1111));
		try (Scanner scanner = new Scanner(System.in)) {
			scanner.nextLine();
		}
		connector.dispose();
		service.stop();
	}
}
