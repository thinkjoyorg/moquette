package org.eclipse.moquette.spi.impl;

import org.eclipse.moquette.server.IAuthenticator;

/**
 * Created by andrea on 8/23/14.
 */
class AcceptAllAuthenticator implements IAuthenticator {
	public boolean checkValid(String token, String password, String clientID) {
		return true;
    }
}
