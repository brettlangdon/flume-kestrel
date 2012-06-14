package com.blangdon.flume.Kestrel;


import iinteractive.kestrel.*;

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
 * Sink that sends events to Kestrel queue
 */
public class KestrelSink extends EventSink.Base {
	

    private String queue = null;
    private String server = null;
    private Client client = null;

    public KestrelSink(String queue, String server){
	//constructor
	this.queue = queue;
	this.server = server;
	this.client = new Client(this.server);
    }

    @Override
	public void open() throws IOException {
	// Initialized the sink
	this.client.connect();
    }
    
    @Override
	public void append(Event e) throws IOException {
	//send to Kestrel
	String message = new String(e.getBody());
	try{
	    this.client.put(this.queue, message);
	}catch( KestrelException ke ){
	    throw new IOException(ke.getMessage());
	}
    }


    @Override
	public void close() throws IOException {
	// Cleanup
	this.client.disconnect();
    }
    
    public static SinkBuilder builder() {
	return new SinkBuilder() {
	    // construct a new parameterized sink
	    @Override
		public EventSink build(Context context, String... argv) {
		Preconditions.checkArgument(argv.length > 1,
					    "usage: kestrelSink(queueName, server:port, [server2:port, server3:port,...])");

		return new KestrelSink(argv[0], argv[1]);
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
