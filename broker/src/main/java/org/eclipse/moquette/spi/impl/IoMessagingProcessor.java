package org.eclipse.moquette.spi.impl;

import com.lmax.disruptor.WorkHandler;
import org.eclipse.moquette.proto.messages.AbstractMessage;
import org.eclipse.moquette.server.ServerChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 创建人：xy
 * 创建时间：15/5/31
 *
 * @version 1.0
 */

public class IoMessagingProcessor implements WorkHandler<ValueEvent> {
	private static final Logger LOG = LoggerFactory.getLogger(ProtocolProcessor.class);

	@Override
	public void onEvent(ValueEvent event) throws Exception {
		try {
			//always ProtocolExEvent instance
			ProtocolExEvent evt = (ProtocolExEvent) event.getEvent();
			ServerChannel session = evt.getSession();
			AbstractMessage message = evt.getMessage();
			AnnotationSupport annotationSupport = evt.annotationSupport;

			annotationSupport.dispatch(session, message);
		} catch (Throwable th) {
			LOG.error(th.getMessage(), th);
		} finally {
			event.setEvent(null);
		}
	}
}
