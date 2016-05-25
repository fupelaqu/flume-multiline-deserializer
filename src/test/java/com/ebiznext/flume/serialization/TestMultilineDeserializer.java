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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        sb.append("2016-01-25 19:10:59,452 INFO  [org.apache.cxf.interceptor.LoggingInInterceptor] (http-0.0.0.0-8080-2) : Inbound Message\n");
        sb.append("----------------------------\n");
        sb.append("ID: 140\n");
        sb.append("Address: http://localhost:8080/test/services/MonService/doSomething/\n");
        sb.append("Encoding: ISO-8859-1\n");
        sb.append("Http-Method: POST\n");
        sb.append("Content-Type: application/json\n");
        sb.append("Headers: {Accept=[application/json], Authorization=[Basic cGFpZW1lbnQtY2xpZW50QHNuY2YuY29tOmNoYW5nZWl0], cache-control=[no-cache], connection=[keep-alive], Content-Length=[565], content-type=[application/json], host=[localhost:8080], pragma=[no-cache], user-agent=[Apache CXF 2.4.2]}\n");
        sb.append("Payload: {\"numeroTransaction\":\"16025185029809\",\"numeroAutorisation\":\"250225\",\"numeroCarte\":\"XXXXXXXXXXXXYYYY\",\"identifiantCommercant\":\"DUMMY\",\"numeroCommercant\":\"99999999999999\",\"dateExpirationCarte\":\"2017-05-01T00:00:00.000+0200\",\"montant\":21.0,\"certificatTransaction\":null,\"dateTransaction\":\"2016-01-25T18:50:00.000+0100\",\"dateAchat\":\"2016-01-25T18:50:30.710+0100\",\"codeReponseAuthorisation\":\"00000\",\"typeCarte\":{\"value\":\"CB\"},\"numeroCarteEnClair\":\"XXXXXXXXXXXXYYYY\",\"numeroDossier\":null,\"modeValidation\":{\"value\":0},\"modeSaisie\":{\"value\":1},\"codeCVV\":\"xxx\",\"status\":null}\n");
        sb.append("--------------------------------------\n");
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
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put(
                "regex",
                "^([\\-]+)|^(ID:).*|" +
                        "^(Address:).*|" +
                        "^(Encoding:).*|" +
                        "^(Http\\-Method:).*|" +
                        "^(Content\\-Type:).*|" +
                        "^(Headers:).*|" +
                        "^(Payload:).*|" +
                        ".*(\\s*.*(Exception|Error|Fault): .+)|" +
                        ".*(\\s+at .+)|" +
                        ".*(\\s*Caused by:.+)|" +
                        ".*(\\s+... (\\d+) more)"
        );
        EventDeserializer des = new MultilineDeserializer(new Context(parameters), in);
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

        evt = des.readEvent(); // another multiline event
        System.err.println(new String(evt.getBody()));

        evt = des.readEvent();
        Assert.assertNull("Event should be null because there are no lines " +
                "left to read", evt);

        des.mark();
        des.close();
    }

}
