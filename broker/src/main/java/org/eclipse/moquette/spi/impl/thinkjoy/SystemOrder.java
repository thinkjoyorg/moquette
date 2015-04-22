package org.eclipse.moquette.spi.impl.thinkjoy;

import java.io.Serializable;

/**
 * Created by Michael on 4/20/15.
 */

public abstract class SystemOrder implements Serializable {

	protected String from;
	protected String to;

	public void setFrom(String from) {
		this.from = from;
	}

	public void setTo(String to) {
		this.to = to;
	}


	abstract String pickBizSys();

	abstract String pickBizType();


	public String pickFrom() {
		return from;
	}

	public String pickTo() {
		return to;
	}
}
