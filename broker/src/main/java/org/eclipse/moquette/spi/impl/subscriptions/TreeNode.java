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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;

@Deprecated
class TreeNode {

    Token m_token;
	// rowKey : clientId, columnKey : topic, value : subscription
	// 业务特性(qos为0，cleanSession为true): clientID和topic可以确定唯一的Subscription
	Table<String, String, Subscription> subs = TreeBasedTable.create();

    Token getToken() {
        return m_token;
    }

    void setToken(Token topic) {
        this.m_token = topic;
    }

    void addSubscription(Subscription s) {
	    subs.put(s.getClientId(), s.getTopicFilter(), s);
    }

	Set<Subscription> subscriptions() {
		Collection<Subscription> values = subs.values();
		Set<Subscription> result = Sets.newLinkedHashSet();
		result.addAll(values);
		return result;
	}

	List<Subscription> matches(String topic) {
		List<Subscription> result = Lists.newArrayList();
		Map<String, Subscription> subscriptions = subs.column(topic);
		for (Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
			result.add(entry.getValue());
		}
		return result;
	}

	void removeMatchedSubscription(String topic, String clientID) {
		subs.remove(clientID, topic);
	}

    /**
     * Return the number of registered subscriptions
     */
    int size() {
	    return subs.size();
    }

	void removeClientSubscriptions(String clientID) {
		Map<String, Subscription> subscriptions = subs.row(clientID);
		for (Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
			Subscription subscription = entry.getValue();
			String column = subscription.getTopicFilter();
			subs.remove(clientID, column);
		}
	}


    /**
     * @return the set of subscriptions for the given client.
     * */
    Set<Subscription> findAllByClientID(String clientID) {
	    Map<String, Subscription> subscriptions = subs.row(clientID);
	    Set<Subscription> result = Sets.newLinkedHashSet();
	    for (Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
		    result.add(entry.getValue());
	    }
	    return result;
    }
}
