package havis.middleware.subscriber.sql.osgi;

import havis.middleware.ale.subscriber.SubscriberConnector;
import havis.middleware.subscriber.sql.SqlSubscriberConnector;
import havis.middleware.subscriber.sql.rest.RESTApplication;

import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.core.Application;

import org.osgi.framework.Bundle;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.PrototypeServiceFactory;
import org.osgi.framework.ServiceRegistration;

public class Activator implements BundleActivator {

	private final Logger log = Logger.getLogger(Activator.class.getName());

	private final static String NAME = "name";
	private final static String VALUE = "sql";

	private List<ServiceRegistration<?>> serviceRegistration = new ArrayList<>();

	@Override
	public void start(BundleContext context) throws Exception {
		Dictionary<String, String> properties = new Hashtable<>();
		properties.put(NAME, VALUE);

		log.log(Level.FINE, "Register prototype service factory {0} (''{1}'': ''{2}'')", new Object[] { SqlSubscriberConnector.class.getName(), NAME, VALUE });
		serviceRegistration.add(context.registerService(SubscriberConnector.class.getName(), new PrototypeServiceFactory<SubscriberConnector>() {
			@Override
			public SubscriberConnector getService(Bundle bundle, ServiceRegistration<SubscriberConnector> registration) {
				return new SqlSubscriberConnector();
			}

			@Override
			public void ungetService(Bundle bundle, ServiceRegistration<SubscriberConnector> registration, SubscriberConnector service) {
			}
		}, properties));

		log.log(Level.FINE, "Register application ''{0}''", RESTApplication.class.getName());
		serviceRegistration.add(context.registerService(Application.class, new RESTApplication(), null));
	}

	@Override
	public void stop(BundleContext context) throws Exception {
		for (ServiceRegistration<?> serviceRegistration : this.serviceRegistration)
			serviceRegistration.unregister();
		serviceRegistration.clear();
	}
}