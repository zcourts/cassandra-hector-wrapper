package com.scriptandscroll.adt;

import static me.prettyprint.hector.api.factory.HFactory.*;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;

/**
 *
 * @author Courtney
 */
public class Keyspace {

	private final StringSerializer se = new StringSerializer();
	private Cluster cluster;
	private me.prettyprint.hector.api.Keyspace keyspace;
	//
	private String keyspaceName;
	private String clusterName;
	private String hostAndPort;

	/**
	 * Create a keyspace
	 * @param clusterName the name of your cluster
	 * @param keyspace the name of the keyspace
	 * @param hostAndPort  in the form "127.0.0.1:9170"
	 */
	public Keyspace(String clusterName, String keyspace, String hostAndPort) {
//		System.out.printf("%s %s %s", clusterName,keyspace,hostAndPort);	
		this.clusterName = clusterName;
		this.keyspaceName = keyspace;
		this.hostAndPort = hostAndPort;
		cluster = getOrCreateCluster(this.clusterName, this.hostAndPort);
		this.keyspace = createKeyspace(this.keyspaceName, cluster);
	}

	public me.prettyprint.hector.api.Keyspace getHectorKeyspace() {
		return keyspace;
	}

	public StringSerializer getSerializer() {
		return se;
	}
}
