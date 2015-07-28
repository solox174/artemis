package com.comcast.artemis.jersey.resource;

import com.comcast.artemis.cassandra.CassandraConnect;
import com.datastax.driver.core.utils.UUIDs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Created by kmatth002c on 12/10/2014.
 */
public class BaseResource {
    @Context
    ServletContext servletContext;
    private final static Logger LOG = LoggerFactory.getLogger(BaseResource.class);

    @GET
    @Path("heartBeat")
    public void heartBeat(@Suspended final AsyncResponse asyncResponse) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                long nowMillis = 0;
                try {
                    UUID now = CassandraConnect.getSession().execute("select now() as now from System.local").one().getUUID("now");
                    nowMillis = UUIDs.unixTimestamp(now);
                    /**MUST have finally block resume or any uncaught exceptions will leave the browser request hung*/
                } finally {
                    asyncResponse.resume(nowMillis);
                }
            }
        }).start();
    }

    @GET
    @Path("version")
    @Produces("text/plain")
    public void version(@Suspended final AsyncResponse asyncResponse) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                StringBuilder stringBuffer = new StringBuilder();
                try {
                    InputStream inputStream = servletContext.getResourceAsStream("/META-INF/MANIFEST.MF");
                    Manifest manifest = new Manifest(inputStream);
                    Attributes attributes = manifest.getMainAttributes();
                    stringBuffer.append(attributes.getValue("Implementation-Title"));
                    stringBuffer.append("\n");
                    stringBuffer.append(attributes.getValue("Implementation-Version"));
                    stringBuffer.append("\n");
                    stringBuffer.append(attributes.getValue("Implementation-Branch"));
                    stringBuffer.append("\n");
                    stringBuffer.append(attributes.getValue("Implementation-Build"));
                    stringBuffer.append("\n");
                    stringBuffer.append(attributes.getValue("Implementation-Date"));
                } catch (IOException e) {
                    LOG.warn("Count not read MANIFEST.MF", e);
                /**MUST have finally block resume or any uncaught exceptions will leave the browser request hung*/
                } finally {
                    asyncResponse.resume(stringBuffer.toString());
                }
            }
        }).start();
    }
}
