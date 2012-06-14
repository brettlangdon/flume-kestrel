package com.blangdon.flume.Kestrel;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;

/**
 * Sink that sends events to HornetQ queue
 */
public class KestrelSink extends EventSink.Base {
	

    public KestrelSink(){
	//constructor
    }

    @Override
	public void open() throws IOException {
	// Initialized the sink
    }
    
    @Override
	public void append(Event e) throws IOException {
	//send to Kestrel
    }


    @Override
	public void close() throws IOException {
	// Cleanup
    }
    
    public static SinkBuilder builder() {
	return new SinkBuilder() {
	    // construct a new parameterized sink
	    @Override
		public EventSink build(Context context, String... argv) {
		Preconditions.checkArgument(argv.length > 1,
					    "usage: kestrelSink(queueName, server:port, [server2:port, server3:port,...])");

		return new KestrelSink();
	    }
	};
    }
    
    /**
     * This is a special function used by the SourceFactory to pull in this class
     * as a plugin sink.
     */
    public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
	List<Pair<String, SinkBuilder>> builders =
	    new ArrayList<Pair<String, SinkBuilder>>();
	builders.add(new Pair<String, SinkBuilder>("kestrelSink", builder()));
	return builders;
    }
}
