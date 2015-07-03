package org.eclipse.moquette.spi.impl.subscriptions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * 创建人：xy
 * 创建时间：15/7/1
 *
 * @version 1.0
 */

public class Subscriptions {
	Token m_token;
	Set<Subscription> subs = Sets.newHashSet();

	//空间换时间
	Map<String, Set<Subscription>> topicSubCache = Maps.newHashMap();
	Map<String, Set<Subscription>> clientIDSubCache = Maps.newHashMap();

	Token getToken() {
		return m_token;
	}

	void setToken(Token topic) {
		this.m_token = topic;
	}

	void addSubscription(Subscription s) {
		subs.add(s);

		buildTopicSubCache(s);
		buildClientIDSubCache(s);
	}

	private void buildClientIDSubCache(Subscription s) {
		Set<Subscription> subscriptions = clientIDSubCache.get(s.getClientId());

		if (subscriptions != null) {
			subscriptions.add(s);
		} else {
			subscriptions = Sets.newHashSet(s);
		}
		clientIDSubCache.put(s.getClientId(), subscriptions);

	}

	private void buildTopicSubCache(Subscription s) {
		Set<Subscription> subscriptions = topicSubCache.get(s.getTopicFilter());

		if (subscriptions != null) {
			subscriptions.add(s);
		} else {
			subscriptions = Sets.newHashSet(s);
		}
		topicSubCache.put(s.getTopicFilter(), subscriptions);

	}

	Set<Subscription> subscriptions() {
		return subs;
	}

	List<Subscription> matches(String topic) {
		List<Subscription> result = new ArrayList<>();
		Set<Subscription> subscriptions = topicSubCache.get(topic);
		if (subscriptions != null) {
			result.addAll(subscriptions);
		}
		return result;
	}

	void removeMatchedSubscription(String topic, final String clientID) {
		Set<Subscription> temp = this.findAllByClientID(clientID);

		Set<Subscription> subscriptions = Sets.newHashSet(temp);

		for (Subscription subscription : subscriptions) {
			if (subscription.match(topic)) {
				subs.remove(subscription);
				removeSubFromClientIDCache(subscription);
				removeSubFromTopicCache(subscription);
			}
		}

	}

	private void removeSubFromTopicCache(Subscription s) {
		Set<Subscription> subscriptions = topicSubCache.get(s.getTopicFilter());

		boolean removed = subscriptions.remove(s);
		if (removed) {
			if (subscriptions.size() > 0) {
				topicSubCache.put(s.getTopicFilter(), subscriptions);
			} else {
				topicSubCache.remove(s.getTopicFilter());
			}
		}

	}

	private void removeSubFromClientIDCache(Subscription s) {
		Set<Subscription> subscriptions = clientIDSubCache.get(s.getClientId());

		boolean removed = subscriptions.remove(s);
		if (removed) {
			if (subscriptions.size() > 0) {
				clientIDSubCache.put(s.getClientId(), subscriptions);
			} else {
				clientIDSubCache.remove(s.getClientId());
			}
		}

	}

	/**
	 * Return the number of registered subscriptions
	 */
	int size() {
		return subs.size();
	}

	void removeClientSubscriptions(String clientID) {
		Set<Subscription> temp = clientIDSubCache.get(clientID);

		Set<Subscription> subscriptions = Sets.newHashSet(temp);

		if (subscriptions != null) {
			for (Subscription subscription : subscriptions) {
				subs.remove(subscription);
				removeSubFromClientIDCache(subscription);
				removeSubFromTopicCache(subscription);
			}
		}
	}


	/**
	 * @return the set of subscriptions for the given client.
	 */
	Set<Subscription> findAllByClientID(final String clientID) {
		Set<Subscription> subscriptions = clientIDSubCache.get(clientID);

		if (subscriptions != null) {
			return subscriptions;
		} else {
			return Sets.newHashSet();
		}
	}
}
