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
		//always ProtocolExEvent instance
		ProtocolExEvent evt = (ProtocolExEvent) event.getEvent();
		try {
			ServerChannel session = evt.getSession();
			AbstractMessage message = evt.getMessage();
			AnnotationSupport annotationSupport = evt.annotationSupport;
			long delay = 0;
			try {
				long startTime = System.nanoTime();
				delay = System.nanoTime() - startTime;
				annotationSupport.dispatch(session, message);
				LOG.debug("IoMessagingProcessor process msgType {} takes: {} ms ", message.getMessageType(), (delay / 1000000));
			} catch (Throwable th) {
				LOG.error("IoMessagingProcessor Serious error processing the message {} for {}", message, session, th);
			}

		} finally {
			event.setEvent(null);
		}
	}
}
