package org.eclipse.moquette.spi.impl.events;

import org.eclipse.moquette.spi.impl.ProtocolProcessor;

/**
 * 创建人：xy
 * 创建时间：15/6/13
 *
 * @version 1.0
 */
@Deprecated
public class LostConnExEvent extends LostConnectionEvent {

	private ProtocolProcessor processor;

	public LostConnExEvent(String clientID, ProtocolProcessor processor) {
		super(clientID);
		this.processor = processor;
	}

	@Override
	public String toString() {
		return "LostConnExEvent{" +
				"processor=" + processor +
				'}';
	}

	public ProtocolProcessor getProcessor() {
		return processor;
	}

	public void setProcessor(ProtocolProcessor processor) {
		this.processor = processor;
	}
}
