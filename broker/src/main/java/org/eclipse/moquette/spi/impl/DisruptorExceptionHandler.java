package org.eclipse.moquette.spi.impl;

import com.lmax.disruptor.ExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 创建人：xy
 * 创建时间：15/6/8
 *
 * @version 1.0
 */

public class DisruptorExceptionHandler implements ExceptionHandler<ValueEvent> {
	private static final Logger LOG = LoggerFactory.getLogger(IoTaskProcessor.class);


	@Override
	public void handleEventException(Throwable ex, long sequence, ValueEvent event) {
		LOG.error("event [{}] process error, seq is [{}]", event.getEvent().toString(), sequence);
		LOG.error(ex.getMessage(), ex);
	}


	@Override
	public void handleOnStartException(Throwable ex) {
		LOG.error("start error");
		LOG.error(ex.getMessage(), ex);
	}


	@Override
	public void handleOnShutdownException(Throwable ex) {
		LOG.error("shutdown error");
		LOG.error(ex.getMessage(), ex);
	}
}
