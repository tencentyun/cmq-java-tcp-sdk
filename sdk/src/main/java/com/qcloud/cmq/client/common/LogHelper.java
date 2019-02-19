package com.qcloud.cmq.client.common;

import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.net.URL;

public class LogHelper {
    private static final String CLIENT_LOG_NAME = "CmqClient";
    private static final String CLIENT_LOG_ROOT = "cmq.client.logRoot";
    private static final String CLIENT_LOG_MAX_INDEX = "cmq.client.logFileMaxIndex";
    private static final String CLIENT_LOG_LEVEL = "cmq.client.logLevel";
    private static Logger log;

    public static boolean LOG_REQUEST = false;

    static {
        log = createLogger(CLIENT_LOG_NAME);
    }

    private static Logger createLogger(final String loggerName) {
        String logConfigFilePath = System.getProperty("cmq.client.log.configFile",
                System.getenv("CMQ_CLIENT_LOG_CONFIGFILE"));
        Boolean isLoadConfig =
                Boolean.parseBoolean(System.getProperty("cmq.client.log.loadconfig", "true"));

        final String log4JResourceFile = System.getProperty("cmq.client.log4j.resource.fileName",
                "log4j_cmq_client.xml");

        final String logbackResourceFile = System.getProperty("cmq.client.logback.resource.fileName",
                "logback_cmq_client.xml");

        String clientLogRoot = System.getProperty(CLIENT_LOG_ROOT, "${user.home}/logs/cmqlogs");
        System.setProperty("client.logRoot", clientLogRoot);
        String clientLogLevel = System.getProperty(CLIENT_LOG_LEVEL, "INFO");
        System.setProperty("client.logLevel", clientLogLevel);
        String clientLogMaxIndex = System.getProperty(CLIENT_LOG_MAX_INDEX, "10");
        System.setProperty("client.logFileMaxIndex", clientLogMaxIndex);

        if (isLoadConfig) {
            try {
                ILoggerFactory iLoggerFactory = LoggerFactory.getILoggerFactory();
                Class classType = iLoggerFactory.getClass();
                if (classType.getName().equals("org.slf4j.impl.Log4jLoggerFactory")) {
                    Class<?> domconfigurator;
                    Object domconfiguratorobj;
                    domconfigurator = Class.forName("org.apache.log4j.xml.DOMConfigurator");
                    domconfiguratorobj = domconfigurator.newInstance();
                    if (null == logConfigFilePath) {
                        Method configure = domconfiguratorobj.getClass().getMethod("configure", URL.class);
                        URL url = LogHelper.class.getClassLoader().getResource(log4JResourceFile);
                        configure.invoke(domconfiguratorobj, url);
                    } else {
                        Method configure = domconfiguratorobj.getClass().getMethod("configure", String.class);
                        configure.invoke(domconfiguratorobj, logConfigFilePath);
                    }

                } else if (classType.getName().equals("ch.qos.logback.classic.LoggerContext")) {
                    Class<?> joranConfigurator;
                    Class<?> context = Class.forName("ch.qos.logback.core.Context");
                    Object joranConfiguratoroObj;
                    joranConfigurator = Class.forName("ch.qos.logback.classic.joran.JoranConfigurator");
                    joranConfiguratoroObj = joranConfigurator.newInstance();
                    Method setContext = joranConfiguratoroObj.getClass().getMethod("setContext", context);
                    setContext.invoke(joranConfiguratoroObj, iLoggerFactory);
                    if (null == logConfigFilePath) {
                        URL url = LogHelper.class.getClassLoader().getResource(logbackResourceFile);
                        Method doConfigure =
                                joranConfiguratoroObj.getClass().getMethod("doConfigure", URL.class);
                        doConfigure.invoke(joranConfiguratoroObj, url);
                    } else {
                        Method doConfigure =
                                joranConfiguratoroObj.getClass().getMethod("doConfigure", String.class);
                        doConfigure.invoke(joranConfiguratoroObj, logConfigFilePath);
                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return LoggerFactory.getLogger(loggerName);
    }

    public static Logger getLog() {
        return log;
    }

    public static void setLog(Logger log) {
        LogHelper.log = log;
    }
}
