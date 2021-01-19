package havis.middleware.subscriber.sql;

import havis.middleware.ale.subscriber.SubscriberConnector;
import havis.middleware.subscriber.sql.osgi.Activator;

import java.util.Dictionary;

import mockit.Mocked;
import mockit.Verifications;

import org.junit.Test;
import org.osgi.framework.BundleContext;
import org.osgi.framework.PrototypeServiceFactory;
import org.osgi.framework.ServiceRegistration;

public class ActivatorTest {

	PrototypeServiceFactory<SubscriberConnector> factory;
	ServiceRegistration<?> registration;

	@Test
	public void activatorTest(@Mocked final BundleContext context) throws Exception {
		Activator activator = new Activator();
		activator.start(context);

		new Verifications() {
			{
				PrototypeServiceFactory<SubscriberConnector> factory;

				context.registerService(SubscriberConnector.class.getName(), factory = withCapture(), this.<Dictionary<String, String>> withNotNull());
				times = 1;

				ActivatorTest.this.factory = factory;
			}
		};

		factory.getService(null, null);
		factory.ungetService(null, null, null);

		activator.stop(context);
	}
}