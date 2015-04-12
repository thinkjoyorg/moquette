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
package org.eclipse.moquette.commons;

import java.io.File;

/**
 * Contains some useful constants.
 */
public class Constants {
	public static final int PORT = 1883;
	public static final int WEBSOCKET_PORT = 8088;
	public static final String HOST = "0.0.0.0";
	public static final int DEFAULT_CONNECT_TIMEOUT = 60;
	public static final String DEFAULT_MOQUETTE_STORE_MAP_DB_FILENAME = "moquette_store.mapdb";
	public static final String DEFAULT_PERSISTENT_PATH = System.getProperty("user.home") + File.separator + DEFAULT_MOQUETTE_STORE_MAP_DB_FILENAME;
	public static final String PERSISTENT_STORE_PROPERTY_NAME = "persistent_store";
	public static final String PASSWORD_FILE_PROPERTY_NAME = "password_file";
	public static final String PORT_PROPERTY_NAME = "port";
	public static final String HOST_PROPERTY_NAME = "host";
	public static final String WEB_SOCKET_PORT_PROPERTY_NAME = "websocket_port";
	public static final String SSL_PORT_PROPERTY_NAME = "ssl_port";
	public static final String JKS_PATH_PROPERTY_NAME = "jks_path";
	public static final String KEY_STORE_PASSWORD_PROPERTY_NAME = "key_store_password";
	public static final String KEY_MANAGER_PASSWORD_PROPERTY_NAME = "key_manager_password";

	///////////////////////// Redis ///////////////////

	/* 此key是存储的允许多终端登录前缀 */
	public static final String KEY_MUTI_CLIENT_ALLOWABLE = "muti_client_allowable";

	/////////////////// 重复登录消息 ///////////////////
	/* 如果检测到有用户重复登录，并且该域下不允许重复登录，则给已经登录的用户发送登录冲突的消息，内容就是CONNECT_CONFLICT  */
	public static final String M_CONNECT_CONFLICT = "CONNECT_CONFLICT";

	//
	public static final String KEY_AREA_ACCOUNT = "area_account";
}
