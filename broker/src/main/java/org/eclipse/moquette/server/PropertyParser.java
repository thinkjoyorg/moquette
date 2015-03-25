package org.eclipse.moquette.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.eclipse.moquette.proto.MQTTException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 属性文件解析器
 * 创建人：xy
 * 创建时间：15/3/24
 *
 * @version 1.0
 */

public final class PropertyParser {
	private static final Logger LOG = LoggerFactory.getLogger(RedisPool.class);

	//empty constructor
	private PropertyParser() {
	}

	public static final Properties getProperties(File file) {
		Properties prop = new Properties();
		try {
			InputStream inputStream = new FileInputStream(file);
			prop.load(inputStream);
			inputStream.close();
		} catch (IOException e) {
			LOG.error("read file {} fail...", file.getName());
			throw new MQTTException(String.format("read file %s fail...", file.getName()));
		}
		return prop;
	}
}
