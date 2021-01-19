package havis.middleware.subscriber.sql.rest;

import havis.middleware.subscriber.sql.SqlSubscriberConnector;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("middleware/subscriber/sql")
public class Storage {

	private Map<String, SqlSubscriberConnector> map = new HashMap<>();

	public final static Storage INSTANCE = new Storage();

	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	@GET
	@Path("{name}")
	public Response download(@PathParam("name") String name, @QueryParam("limit") @DefaultValue("1000") int limit,
			@QueryParam("offset") @DefaultValue("0") int offset) {
		SqlSubscriberConnector connector = map.get(name);

		if (connector == null)
			return Response.noContent().build();

		StringWriter writer = new StringWriter();
		try {
			connector.marshal(writer, limit, offset);
			return Response.ok(writer.toString()).build();
		} catch (SQLException | IOException e) {
			return Response.serverError().build();
		}
	}

	@DELETE
	@Path("{name}")
	public Response clear(@PathParam("name") String name) {
		SqlSubscriberConnector connector = map.get(name);

		if (connector == null)
			return Response.noContent().build();

		try {
			int result = connector.clear();
			if (result > 0)
				return Response.ok().build();
			return Response.notModified().build();
		} catch (SQLException e) {
			return Response.serverError().build();
		}
	}

	public void put(String name, SqlSubscriberConnector connector) {
		map.put(name, connector);
	}

	public void remove(String name) {
		map.remove(name);
	}
}