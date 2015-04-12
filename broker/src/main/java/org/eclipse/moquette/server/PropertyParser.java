package org.eclipse.moquette.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.google.common.base.Preconditions;
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
	private static final Logger LOG = LoggerFactory.getLogger(PropertyParser.class);

	//empty constructor
	private PropertyParser() {
	}

	public static final Properties getProperties(File file) {
		Preconditions.checkNotNull(file, "config file must not be null");

		Properties prop = new Properties();
		if (!file.exists()) {
			prop.setProperty("driver", "com.mysql.jdbc.Driver");
			prop.setProperty("url", "jdbc:mysql://10.10.66.12:3306/im");
			prop.setProperty("username", "root");
			prop.setProperty("password", "im.db");
			return prop;
		}

		InputStream inputStream = null;
		try {
			inputStream = new FileInputStream(file);
			prop.load(inputStream);
		} catch (IOException e) {
			LOG.error("read file {} fail...", file.getName());
			throw new MQTTException(String.format("read file %s fail...", file.getName()));
		} finally {
			try {
				if (null != inputStream) {
					inputStream.close();
				}
			} catch (IOException ioe) {
				//ignore
			}
		}
		return prop;
	}
}
