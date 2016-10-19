// ============================================================================
//
// Copyright (C) 2006-2015 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.common.oauth.util;

import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * created by bchen on Aug 26, 2013 Detailed comment
 * 
 */
public class HttpsService {

    Server server;

    /**
     * DOC bchen HttpService constructor comment.
     * 
     * @throws Exception
     */
    public HttpsService(String host, int port, Handler handler) throws Exception {

        server = new Server();

        HttpConfiguration httpConfig = new HttpConfiguration();
        httpConfig.setSecureScheme("https");
        httpConfig.setSecurePort(port);

        HttpConfiguration httpsConfig = new HttpConfiguration(httpConfig);
        httpsConfig.addCustomizer(new SecureRequestCustomizer());

        SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setKeyStorePath(HttpsService.class.getResource("sslkey").toString());
        sslContextFactory.setKeyStorePassword("talend");
        sslContextFactory.setKeyManagerPassword("talend");

        ServerConnector sslConnector = new ServerConnector(server,
                new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
                new HttpConnectionFactory(httpsConfig));
        sslConnector.setPort(port);
        sslConnector.setHost(host);
        server.addConnector(sslConnector);

        server.setHandler(handler);
        server.start();
    }

    public void stop() throws Exception {
        server.stop();
        server.join();
    }

}
