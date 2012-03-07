/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 */
public class ThreadPoolStats implements Streamable, ToXContent, Iterable<ThreadPoolStats.Stats> {

    public static class Stats implements Streamable, ToXContent {

        private String name;
        private int threads;
        private int queue;
        private int active;

        Stats() {

        }

        public Stats(String name, int threads, int queue, int active) {
            this.name = name;
            this.threads = threads;
            this.queue = queue;
            this.active = active;
        }

        public String name() {
            return this.name;
        }

        public String getName() {
            return this.name;
        }

        public int threads() {
            return this.threads;
        }

        public int getThreads() {
            return this.threads;
        }

        public int queue() {
            return this.queue;
        }

        public int getQueue() {
            return this.queue;
        }

        public int active() {
            return this.active;
        }

        public int getActive() {
            return this.active;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readUTF();
            threads = in.readInt();
            queue = in.readInt();
            active = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeUTF(name);
            out.writeInt(threads);
            out.writeInt(queue);
            out.writeInt(active);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(name, XContentBuilder.FieldCaseConversion.NONE);
            if (threads != -1) {
                builder.field(Fields.THREADS, threads);
            }
            if (queue != -1) {
                builder.field(Fields.QUEUE, queue);
            }
            if (active != -1) {
                builder.field(Fields.ACTIVE, active);
            }
            builder.endObject();
            return builder;
        }
    }

    private List<Stats> stats;

    ThreadPoolStats() {

    }

    public ThreadPoolStats(List<Stats> stats) {
        this.stats = stats;
    }

    @Override
    public Iterator<Stats> iterator() {
        return stats.iterator();
    }

    public static ThreadPoolStats readThreadPoolStats(StreamInput in) throws IOException {
        ThreadPoolStats stats = new ThreadPoolStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        stats = new ArrayList<Stats>(size);
        for (int i = 0; i < size; i++) {
            Stats stats1 = new Stats();
            stats1.readFrom(in);
            stats.add(stats1);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(stats.size());
        for (Stats stat : stats) {
            stat.writeTo(out);
        }
    }

    static final class Fields {
        static final XContentBuilderString THREAD_POOL = new XContentBuilderString("thread_pool");
        static final XContentBuilderString THREADS = new XContentBuilderString("threads");
        static final XContentBuilderString QUEUE = new XContentBuilderString("queue");
        static final XContentBuilderString ACTIVE = new XContentBuilderString("active");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.THREAD_POOL);
        for (Stats stat : stats) {
            stat.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }
}
