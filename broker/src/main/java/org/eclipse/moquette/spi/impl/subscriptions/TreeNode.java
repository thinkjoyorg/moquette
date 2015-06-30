/*
 * Copyright (c) 2012-2014 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package org.eclipse.moquette.spi.impl.subscriptions;

import java.util.*;

import com.google.common.collect.Lists;

class TreeNode {

    Token m_token;
	List<Subscription> m_subscriptions = new ArrayList<>();

    Token getToken() {
        return m_token;
    }

    void setToken(Token topic) {
        this.m_token = topic;
    }

    void addSubscription(Subscription s) {
        //avoid double registering for same clientID, topic and QoS
        if (m_subscriptions.contains(s)) {
            return;
        }

        m_subscriptions.add(s);
    }

    List<Subscription> subscriptions() {
        return m_subscriptions;
    }

	List<Subscription> matches(String topic) {
		List<Subscription> result = Lists.newArrayList();
		for (Subscription subscription : m_subscriptions) {
			if (subscription.match(topic)) {
				result.add(subscription);
			}
		}
		return result;
	}

    /**
     * Return the number of registered subscriptions
     */
    int size() {
	    return m_subscriptions.size();
    }

	//TODO:优化点
	void removeClientSubscriptions(String clientID) {
		Iterator<Subscription> it = m_subscriptions.iterator();
		while (it.hasNext()) {
			Subscription sub = it.next();
			if (sub.getClientId().equals(clientID)) {
				it.remove();
			}
		}
	}


    /**
     * @return the set of subscriptions for the given client.
     * */
    Set<Subscription> findAllByClientID(String clientID) {
        Set<Subscription> subs = new HashSet<Subscription>();
	    Iterator<Subscription> it = m_subscriptions.iterator();
	    while (it.hasNext()) {
		    Subscription sub = it.next();
		    if (sub.getClientId().equals(clientID)) {
			    subs.add(sub);
		    }
	    }
        return subs;
    }

	private class ClientIDComparator implements Comparator<Subscription> {

		public int compare(Subscription o1, Subscription o2) {
			return o1.getClientId().compareTo(o2.getClientId());
		}

    }
}
