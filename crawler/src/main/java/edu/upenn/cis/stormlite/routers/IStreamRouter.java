/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.upenn.cis.stormlite.routers;

import java.util.ArrayList;
import java.util.List;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.tasks.BoltTask;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;

/**
 * A StreamRouter is an internal class used to determine where an item placed on
 * a stream should go. It doesn't actually run the downstream bolt, but rather
 * queues it up as a task.
 * 
 * @author zives
 *
 */
public abstract class IStreamRouter implements OutputFieldsDeclarer {

	List<IRichBolt> bolts;
	Fields schema;

	public IStreamRouter() {
		bolts = new ArrayList<>();
	}

	public IStreamRouter(IRichBolt bolt) {
		this();
		bolts.add(bolt);
	}

	public IStreamRouter(List<IRichBolt> bolts) {
		this.bolts = bolts;
	}

	/**
	 * Add another bolt instance as a consumer of this stream
	 * 
	 * @param bolt
	 */
	public void addBolt(IRichBolt bolt) {
		bolts.add(bolt);
	}

	/**
	 * The selector for the destination bolt(s)
	 * 
	 * @param tuple
	 * @return
	 */
	protected abstract List<IRichBolt> getBoltsFor(List<Object> tuple);

	/**
	 * The destination bolts, as a list so we can assign each a unique position
	 * 
	 * @return
	 */
	public List<IRichBolt> getBolts() {
		return bolts;
	}

	/**
	 * Queues up a bolt task (for future scheduling) to process a single stream
	 * tuple
	 * 
	 * @param tuple
	 */
	public synchronized void execute(List<Object> tuple, TopologyContext context) {
		List<IRichBolt> bolts = getBoltsFor(tuple);

		if (bolts != null && !bolts.isEmpty()) {
			for (IRichBolt bolt : bolts) {
				context.addStreamTask(new BoltTask(bolt, new Tuple(schema, tuple)));
			}
		} else {
			throw new RuntimeException("Unable to find a bolt for the tuple");
		}
	}

	/**
	 * Process a tuple with fields
	 * 
	 * @param tuple
	 */
	public void execute(Tuple tuple, TopologyContext context) {
		execute(tuple.getValues(), context);
	}

	/**
	 * Sets the schema of the object
	 */
	@Override
	public void declare(Fields fields) {
		schema = fields;
	}

}
