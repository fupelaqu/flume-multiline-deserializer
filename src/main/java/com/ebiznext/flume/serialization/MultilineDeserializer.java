package com.ebiznext.flume.serialization;

import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.ResettableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * Created by smanciot on 21/02/16.
 */
public class MultilineDeserializer implements EventDeserializer{

    private static final Logger logger = LoggerFactory.getLogger
            (MultilineDeserializer.class);

    private final ResettableInputStream in;
    private final Charset outputCharset;
    private final int maxLineLength;
    private volatile boolean isOpen;

    private final Matcher regex;

    public static final String OUT_CHARSET_KEY = "outputCharset";
    public static final String CHARSET_DFLT = "UTF-8";

    public static final String MAXLINE_KEY = "maxLineLength";
    public static final int MAXLINE_DFLT = 2048;

    public static final String CONFIG_REGEX = "regex";
    public static final String DEFAULT_REGEX
            = ".*(\\s*.*(Exception|Error|Fault): .+)|.*(\\s+at .+)|.*(\\s*Caused by:.+)|.*(\\s+... (\\d+) more)";

    public MultilineDeserializer(Context context, ResettableInputStream in) {
        this.in = in;
        this.outputCharset = Charset.forName(
                context.getString(OUT_CHARSET_KEY, CHARSET_DFLT));
        this.maxLineLength = context.getInteger(MAXLINE_KEY, MAXLINE_DFLT);
        this.regex = Pattern.compile(context.getString(CONFIG_REGEX, DEFAULT_REGEX)).matcher("");
        this.isOpen = true;
    }

    public Event readEvent() throws IOException {
        ensureOpen();
        String line = null;
        StringBuilder lines = null;

        boolean isMatch = true;

        do{
            if (lines == null) {
                line = readLine();
                if(line != null){
                    lines = new StringBuilder(line);
                }
                else{
                    isMatch = false;
                }
            } else {
                long pos = in.tell();
                line = readLine();
                isMatch = line != null && regex.reset(line).matches();
                if(isMatch){
                    lines.append('\n');
                    lines.append(line);
                }
                else if(line != null){
                    in.seek(pos);
                }
            }

        }while(isMatch);

        if (lines == null) {
            return null;
        } else {
            return EventBuilder.withBody(lines.toString(), outputCharset);
        }
    }

    public List<Event> readEvents(int numEvents) throws IOException {
        ensureOpen();
        List<Event> events = Lists.newLinkedList();
        for (int i = 0; i < numEvents; i++) {
            Event event = readEvent();
            if (event != null) {
                events.add(event);
            } else {
                break;
            }
        }
        return events;
    }

    public void mark() throws IOException {
        ensureOpen();
        in.mark();
    }

    public void reset() throws IOException {
        ensureOpen();
        in.reset();
    }

    public void close() throws IOException {
        if (isOpen) {
            reset();
            in.close();
            isOpen = false;
        }
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw new IllegalStateException("Serializer has been closed");
        }
    }

    // TODO: consider not returning a final character that is a high surrogate
    // when truncating
    private String readLine() throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        int readChars = 0;
        while ((c = in.readChar()) != -1) {
            readChars++;

            // FIXME: support \r\n
            if (c == '\n') {
                break;
            }

            sb.append((char)c);

            if (readChars >= maxLineLength) {
                logger.warn("Line length exceeds max ({}), truncating line!",
                        maxLineLength);
                break;
            }
        }

        if (readChars > 0) {
            return sb.toString();
        } else {
            return null;
        }
    }

    public static class Builder implements EventDeserializer.Builder {

        public EventDeserializer build(Context context, ResettableInputStream in) {
            return new MultilineDeserializer(context, in);
        }

    }
}
