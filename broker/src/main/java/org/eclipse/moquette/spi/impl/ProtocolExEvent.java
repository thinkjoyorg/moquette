package org.eclipse.moquette.spi.impl;

import org.eclipse.moquette.proto.messages.AbstractMessage;
import org.eclipse.moquette.server.ServerChannel;
import org.eclipse.moquette.spi.impl.events.ProtocolEvent;

/**
 * 创建人：xy
 * 创建时间：15/5/31
 *
 * @version 1.0
 */

public class ProtocolExEvent extends ProtocolEvent {

	protected AnnotationSupport annotationSupport;

	public ProtocolExEvent(ServerChannel session, AbstractMessage message, AnnotationSupport annotationSupport) {
		super(session, message);
		this.annotationSupport = annotationSupport;
	}

	public AnnotationSupport getAnnotationSupport() {
		return annotationSupport;
	}

	public void setAnnotationSupport(AnnotationSupport annotationSupport) {
		this.annotationSupport = annotationSupport;
	}

	@Override
	public String toString() {
		return "ProtocolExEvent wrapping " + org.eclipse.moquette.proto.Utils.msgType2String(message.getMessageType());
	}
}
