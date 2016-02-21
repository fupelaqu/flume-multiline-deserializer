package com.ebiznext.flume.serialization;

import com.google.common.base.Charsets;
import junit.framework.Assert;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.EventDeserializerFactory;
import org.apache.flume.serialization.ResettableInputStream;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestMultilineDeserializer {

    private String mini;
    private String multiline;

    @Before
    public void setup() {
        StringBuilder sb = new StringBuilder();
        sb.append("line 1\n");
        sb.append("line 2\n");
        mini = sb.toString();
        sb.append("org.apache.cxf.interceptor.Fault: MonService.maMethod() - message\n");
        sb.append("\tat org.apache.cxf.service.invoker.AbstractInvoker.createFault(AbstractInvoker.java:155)\n");
        multiline = sb.toString();
    }

    @Test
    public void testSimple() throws IOException {
        ResettableInputStream in = new ResettableTestStringInputStream(mini);
        EventDeserializer des = new MultilineDeserializer(new Context(), in);
        validateMiniParse(des);
    }

    @Test
    public void testSimpleViaBuilder() throws IOException {
        ResettableInputStream in = new ResettableTestStringInputStream(mini);
        EventDeserializer.Builder builder = new MultilineDeserializer.Builder();
        EventDeserializer des = builder.build(new Context(), in);
        validateMiniParse(des);
    }

    @Test
    public void testSimpleViaFactory() throws IOException {
        ResettableInputStream in = new ResettableTestStringInputStream(mini);
        EventDeserializer des;
        des = EventDeserializerFactory.getInstance("LINE", new Context(), in);
        validateMiniParse(des);
    }

    @Test
    public void testBatch() throws IOException {
        ResettableInputStream in = new ResettableTestStringInputStream(mini);
        EventDeserializer des = new MultilineDeserializer(new Context(), in);
        List<Event> events;

        events = des.readEvents(1); // only try to read 1
        Assert.assertEquals(1, events.size());
        assertEventBodyEquals("line 1", events.get(0));

        events = des.readEvents(10); // try to read more than we should have
        Assert.assertEquals(1, events.size());
        assertEventBodyEquals("line 2", events.get(0));

        des.mark();
        des.close();
    }

    // truncation occurs at maxLineLength boundaries
    @Test
    public void testMaxLineLength() throws IOException {
        String longLine = "abcdefghijklmnopqrstuvwxyz\n";
        Context ctx = new Context();
        ctx.put(MultilineDeserializer.MAXLINE_KEY, "10");

        ResettableInputStream in = new ResettableTestStringInputStream(longLine);
        EventDeserializer des = new MultilineDeserializer(ctx, in);

        assertEventBodyEquals("abcdefghij", des.readEvent());
        assertEventBodyEquals("klmnopqrst", des.readEvent());
        assertEventBodyEquals("uvwxyz", des.readEvent());
        Assert.assertNull(des.readEvent());
    }

  /*
   * TODO: need test for output charset
  @Test
  public void testOutputCharset {

  }
  */

    private void assertEventBodyEquals(String expected, Event event) {
        String bodyStr = new String(event.getBody(), Charsets.UTF_8);
        Assert.assertEquals(expected, bodyStr);
    }

    private void validateMiniParse(EventDeserializer des) throws IOException {
        Event evt;

        evt = des.readEvent();
        Assert.assertEquals(new String(evt.getBody()), "line 1");
        des.mark();

        evt = des.readEvent();
        Assert.assertNotNull(evt);
        Assert.assertEquals(new String(evt.getBody()), "line 2");
        des.reset(); // reset!

        evt = des.readEvent();
        Assert.assertEquals("Line 2 should be repeated, " +
                "because we reset() the stream", new String(evt.getBody()), "line 2");

        evt = des.readEvent();
        Assert.assertNull("Event should be null because there are no lines " +
                "left to read", evt);

        des.mark();
        des.close();
    }

    @Test
    public void testMultiline() throws IOException {
        ResettableInputStream in = new ResettableTestStringInputStream(multiline);
        EventDeserializer des = new MultilineDeserializer(new Context(), in);
        validateMultilineParse(des);
    }

    private void validateMultilineParse(EventDeserializer des) throws IOException {
        Event evt;

        evt = des.readEvent();
        Assert.assertEquals(new String(evt.getBody()), "line 1");
        des.mark();

        final String line = "line 2\n" +
                "org.apache.cxf.interceptor.Fault: MonService.maMethod() - message\n" +
                "\tat org.apache.cxf.service.invoker.AbstractInvoker.createFault(AbstractInvoker.java:155)";

        evt = des.readEvent();
        Assert.assertNotNull(evt);
        Assert.assertEquals(new String(evt.getBody()), line);
        des.reset(); // reset!

        evt = des.readEvent();
        Assert.assertEquals("Line 2 should be repeated, " +
                "because we reset() the stream", new String(evt.getBody()), line);

        evt = des.readEvent();
        Assert.assertNull("Event should be null because there are no lines " +
                "left to read", evt);

        des.mark();
        des.close();
    }

}
