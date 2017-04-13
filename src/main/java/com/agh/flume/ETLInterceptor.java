//add begin
package com.agh.flume;
//add end

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ETLInterceptor implements Interceptor{
    private static Logger logger = LoggerFactory.getLogger(ETLInterceptor.class);
    @Override
    public void initialize() {

    }
    @Override
    public Event intercept(Event event) {
        String body = new String(event.getBody(), Charsets.UTF_8);
        String nodeName = System.getenv("nodeName");
        StringBuilder sb = new StringBuilder();
        StringBuilder append = sb.append("[").append(nodeName).append("]").append(body);
        event.setBody(append.toString().getBytes());
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> intercepted = Lists.newArrayListWithCapacity(events.size());
        for (Event event : events) {
            Event interceptedEvent = intercept(event);
            if (interceptedEvent != null) {
                intercepted.add(interceptedEvent);
            }
        }
        return intercepted;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{
        //使用Builder初始化Interceptor
        @Override
        public Interceptor build() {
	//add begin
            return new ETLInterceptor();
	//add end
        }

        @Override
        public void configure(Context context) {

        }
    }
}