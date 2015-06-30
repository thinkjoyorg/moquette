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

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.eclipse.moquette.spi.ISessionsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a tree of topics subscriptions.
 *
 * @author andrea
 */
public class SubscriptionsStore {

	private static final Logger LOG = LoggerFactory.getLogger(SubscriptionsStore.class);
	private TreeNode subscriptions = new TreeNode();
	private ISessionsStore m_sessionsStore;

	/**
	 * Verify if the 2 topics matching respecting the rules of MQTT Appendix A
	 */
	//TODO reimplement with iterators or with queues
	public static boolean matchTopics(String msgTopic, String subscriptionTopic) {
		try {
			List<Token> msgTokens = SubscriptionsStore.parseTopic(msgTopic);
			List<Token> subscriptionTokens = SubscriptionsStore.parseTopic(subscriptionTopic);
			int i = 0;
			Token subToken = null;
			for (; i < subscriptionTokens.size(); i++) {
				subToken = subscriptionTokens.get(i);
				if (subToken != Token.MULTI && subToken != Token.SINGLE) {
					if (i >= msgTokens.size()) {
						return false;
					}
					Token msgToken = msgTokens.get(i);
					if (!msgToken.equals(subToken)) {
						return false;
					}
				} else {
					if (subToken == Token.MULTI) {
						return true;
					}
					if (subToken == Token.SINGLE) {
						//skip a step forward
					}
				}
			}
			//if last token was a SINGLE then treat it as an empty
//            if (subToken == Token.SINGLE && (i - msgTokens.size() == 1)) {
//               i--;
//            }
			return i == msgTokens.size();
		} catch (ParseException ex) {
			LOG.error(null, ex);
			throw new RuntimeException(ex);
		}
	}

	protected static List<Token> parseTopic(String topic) throws ParseException {
		List<Token> res = new ArrayList<>();
		String[] splitted = topic.split("/");

		if (splitted.length == 0) {
			res.add(Token.EMPTY);
		}

		if (topic.endsWith("/")) {
			//Add a fictious space
			String[] newSplitted = new String[splitted.length + 1];
			System.arraycopy(splitted, 0, newSplitted, 0, splitted.length);
			newSplitted[splitted.length] = "";
			splitted = newSplitted;
		}

		for (int i = 0; i < splitted.length; i++) {
			String s = splitted[i];
			if (s.isEmpty()) {
//                if (i != 0) {
//                    throw new ParseException("Bad format of topic, expetec topic name between separators", i);
//                }
				res.add(Token.EMPTY);
			} else if (s.equals("#")) {
				//check that multi is the last symbol
				if (i != splitted.length - 1) {
					throw new ParseException("Bad format of topic, the multi symbol (#) has to be the last one after a separator", i);
				}
				res.add(Token.MULTI);
			} else if (s.contains("#")) {
				throw new ParseException("Bad format of topic, invalid subtopic name: " + s, i);
			} else if (s.equals("+")) {
				res.add(Token.SINGLE);
			} else if (s.contains("+")) {
				throw new ParseException("Bad format of topic, invalid subtopic name: " + s, i);
			} else {
				res.add(new Token(s));
			}
		}

		return res;
	}

	/**
	 * Check if the topic filter of the subscription is well formed
	 */
	public static boolean validate(Subscription newSubscription) {
		try {
			parseTopic(newSubscription.topicFilter);
			return true;
		} catch (ParseException pex) {
			LOG.info("Bad matching topic filter <{}>", newSubscription.topicFilter);
			return false;
		}
	}

    /**
     * Initialize the subscription tree with the list of subscriptions.
     */
    //TODO:去掉 re-sub
    public void init(ISessionsStore sessionsStore) {
        LOG.debug("init invoked");
        m_sessionsStore = sessionsStore;
//        List<Subscription> subscriptions = sessionsStore.listAllSubscriptions();
//        //reload any subscriptions persisted
//        if (LOG.isDebugEnabled()) {
//            LOG.debug("Reloading all stored subscriptions...subscription tree before {}", dumpTree());
//        }
//
//        for (Subscription subscription : subscriptions) {
//            LOG.debug("Re-subscribing {} to topic {}", subscription.getClientId(), subscription.getTopicFilter());
//            addDirect(subscription);
//        }
//        if (LOG.isDebugEnabled()) {
//            LOG.debug("Finished loading. Subscription tree after {}", dumpTree());
//        }
    }

	protected void addDirect(Subscription newSubscription) {
		subscriptions.addSubscription(newSubscription);
	}

	public void add(Subscription newSubscription) {
		addDirect(newSubscription);

        //log the subscription
//        String clientID = newSubscription.getClientId();
//        m_storageService.addNewSubscription(newSubscription, clientID);
    }

    public void removeSubscription(String topic, String clientID) {
	    Set<Subscription> all = subscriptions.findAllByClientID(clientID);

	    //search for the subscription to remove
	    Iterator<Subscription> it = all.iterator();
	    while (it.hasNext()) {
		    Subscription sub = it.next();
		    if (sub.match(topic)) {
			    it.remove();
		    }
	    }
    }

	/**
	 * TODO implement testing
	 */
	public void clearAllSubscriptions() {

	}

	/**
	 * Visit the topics tree to remove matching subscriptions with clientID
	 */
	public void removeForClient(String clientID) {
        subscriptions.removeClientSubscriptions(clientID);
        //persist the update
        m_sessionsStore.wipeSubscriptions(clientID);
    }

    public void deactivate(String clientID) {
	    //TODO:暂不使用
//        subscriptions.deactivate(clientID);
//        //persist the update
//        Set<Subscription> subs = subscriptions.findAllByClientID(clientID);
//        m_sessionsStore.updateSubscriptions(clientID, subs);
    }

    public void activate(String clientID) {
	    //TODO:暂不使用
//        LOG.debug("Activating subscriptions for clientID <{}>", clientID);
//        subscriptions.activate(clientID);
//        //persist the update
//        Set<Subscription> subs = subscriptions.findAllByClientID(clientID);
//        m_sessionsStore.updateSubscriptions(clientID, subs);
    }

    /**
     * Given a topic string return the clients subscriptions that matches it.
     * Topic string can't contain character # and + because they are reserved to
     * listeners subscriptions, and not topic publishing.
     */
    public List<Subscription> matches(String topic) {
	    return subscriptions.matches(topic);
    }

    public boolean contains(Subscription sub) {
        return !matches(sub.topicFilter).isEmpty();
    }

    public int size() {
        return subscriptions.size();
    }

	public String dumpTree() {
		//TODO:暂不使用
//		DumpTreeVisitor visitor = new DumpTreeVisitor();
//		bfsVisit(subscriptions, visitor);
//        return visitor.getResult();
		return "";
	}

    private void bfsVisit(TreeNode node, IVisitor visitor) {
	    //TODO:暂不使用
//        if (node == null) {
//            return;
//        }
//        visitor.visit(node);
//        for (TreeNode child : node.m_children) {
//            bfsVisit(child, visitor);
//        }
    }


	public static interface IVisitor<T> {
		void visit(TreeNode node);

		T getResult();
	}

	private class DumpTreeVisitor implements IVisitor<String> {

		String s = "";

		public void visit(TreeNode node) {
			String subScriptionsStr = "";
			for (Subscription sub : node.m_subscriptions) {
				subScriptionsStr += sub.toString();
			}
			s += node.getToken() == null ? "" : node.getToken().toString();
			s += subScriptionsStr + "\n";
		}

		public String getResult() {
			return s;
		}
	}

	private class SubscriptionTreeCollector implements IVisitor<List<Subscription>> {

		private List<Subscription> m_allSubscriptions = new ArrayList<Subscription>();

		public void visit(TreeNode node) {
			m_allSubscriptions.addAll(node.subscriptions());
		}

		public List<Subscription> getResult() {
			return m_allSubscriptions;
		}
	}
}
