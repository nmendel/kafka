/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainsConsumerInterceptor<K, V> implements ConsumerInterceptor<K, V> {

	private static final Logger log = LoggerFactory.getLogger(ContainsConsumerInterceptor.class);
	
	public static final String CONTAINED = "interceptor.contains.value";
	
	public static final AtomicReference<ClusterResource> CLUSTER_META = new AtomicReference<>();
    public static final ClusterResource NO_CLUSTER_ID = new ClusterResource("no_cluster_id");
    public static final AtomicReference<ClusterResource> CLUSTER_ID_BEFORE_ON_CONSUME = new AtomicReference<>(NO_CLUSTER_ID);
    private String contained;
	
    @Override
    public void configure(Map<String, ?> configs) {
        // clientId must be in configs
        Object clientIdValue = configs.get(ConsumerConfig.CLIENT_ID_CONFIG);
        if (clientIdValue == null)
            throw new ConfigException("Consumer interceptor expects configuration " + ProducerConfig.CLIENT_ID_CONFIG);
        
        Object v = configs.get(CONTAINED);
        if(v == null) {
        	throw new ConfigException("Consumer interceptor expects configuration " + CONTAINED);
        }
        contained = (String)v;
    }
	
    @Override
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        // This will ensure that we get the cluster metadata when onConsume is called for the first time
        // as subsequent compareAndSet operations will fail.
        CLUSTER_ID_BEFORE_ON_CONSUME.compareAndSet(NO_CLUSTER_ID, CLUSTER_META.get());

        // filters out topic/partitions with partition == FILTER_PARTITION
        Map<TopicPartition, List<ConsumerRecord<K, V>>> recordMap = new HashMap<>();
        for (TopicPartition tp : records.partitions()) {
    		List<ConsumerRecord<K, V>> lst = new ArrayList<>();
    	
            for (ConsumerRecord<K, V> record: records.records(tp)) {
            	String value = new String((byte[])record.value());
            	if(value.toString().indexOf(contained) > -1) {
		            lst.add(new ConsumerRecord<>(record.topic(), record.partition(), record.offset(),
		                                         record.timestamp(), record.timestampType(),
		                                         record.checksum(), record.serializedKeySize(),
		                                         record.serializedValueSize(),
		                                         record.key(), (V)value.getBytes()));
            	}
            }
        	recordMap.put(tp, lst);
        }
        return new ConsumerRecords<K, V>(recordMap);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    @Override
    public void close() {

    }


}
