package com.blangdon.flume.kestrel;


import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.command.KestrelCommandFactory;
import net.rubyeye.xmemcached.utils.AddrUtil;
import net.rubyeye.xmemcached.exception.MemcachedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

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
    private String servers = null;
    private MemcachedClient client = null;
    private MemcachedClientBuilder builder = null;

    public KestrelSink(String queue, String servers){
	//constructor
	this.queue = queue;
	this.servers = servers;

	this.builder = new XMemcachedClientBuilder( AddrUtil.getAddresses(this.servers) );
	builder.setCommandFactory(new KestrelCommandFactory());

    }

    @Override
	public void open() throws IOException {
	// Initialized the sink
	this.client = this.builder.build();
    }
    
    @Override
	public void append(Event e) throws IOException {
	//send to Kestrel

	try{
	    String message = new String(e.getBody());
	    this.client.set(this.queue, 0, message);
	}catch(MemcachedException ex){
	    throw new IOException("Kestrel Command Failed: " + ex.getMessage());
	}catch(TimeoutException ex){
	    throw new IOException("Kestrel Command Timeout: " + ex.getMessage());
	}catch(InterruptedException ex){
	    //meh
	}
    }


    @Override
	public void close() throws IOException {
	// Cleanup
	this.client.shutdown();
    }
    
    public static SinkBuilder builder() {
	return new SinkBuilder() {
	    // construct a new parameterized sink
	    @Override
		public EventSink build(Context context, String... argv) {
		Preconditions.checkArgument(argv.length > 1,
					    "usage: kestrelSink(queueName, server:port, [server2:port, server3:port,...])");

		String servers = "";
		for( int i = 1; i < argv.length; ++i ){
		    if( i > 1 ){
			servers += " ";
		    }
		    servers += argv[i];
		}

		return new KestrelSink(argv[0], servers);
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
