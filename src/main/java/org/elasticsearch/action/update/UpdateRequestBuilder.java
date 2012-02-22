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

package org.elasticsearch.action.update;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.support.BaseRequestBuilder;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Map;

/**
 */
public class UpdateRequestBuilder extends BaseRequestBuilder<UpdateRequest, UpdateResponse> {

    public UpdateRequestBuilder(Client client) {
        super(client, new UpdateRequest());
    }

    public UpdateRequestBuilder(Client client, String index, String type, String id) {
        super(client, new UpdateRequest(index, type, id));
    }

    /**
     * Sets the index the document will exists on.
     */
    public UpdateRequestBuilder setIndex(String index) {
        request.index(index);
        return this;
    }

    /**
     * Sets the type of the indexed document.
     */
    public UpdateRequestBuilder setType(String type) {
        request.type(type);
        return this;
    }

    /**
     * Sets the id of the indexed document.
     */
    public UpdateRequestBuilder setId(String id) {
        request.id(id);
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    public UpdateRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    public UpdateRequestBuilder setParent(String parent) {
        request.parent(parent);
        return this;
    }

    /**
     * The script to execute. Note, make sure not to send different script each times and instead
     * use script params if possible with the same (automatically compiled) script.
     */
    public UpdateRequestBuilder setScript(String script) {
        request.script(script);
        return this;
    }

    /**
     * The language of the script to execute.
     */
    public UpdateRequestBuilder setScriptLang(String scriptLang) {
        request.scriptLang(scriptLang);
        return this;
    }

    /**
     * Sets the script parameters to use with the script.
     */
    public UpdateRequestBuilder setScriptParams(Map<String, Object> scriptParams) {
        request.scriptParams(scriptParams);
        return this;
    }

    /**
     * Add a script parameter.
     */
    public UpdateRequestBuilder addScriptParam(String name, Object value) {
        request.addScriptParam(name, value);
        return this;
    }

    /**
     * Sets the number of retries of a version conflict occurs because the document was updated between
     * getting it and updating it. Defaults to 1.
     */
    public UpdateRequestBuilder setRetryOnConflict(int retryOnConflict) {
        request.retryOnConflict(retryOnConflict);
        return this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to <tt>1m</tt>.
     */
    public UpdateRequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to <tt>1m</tt>.
     */
    public UpdateRequestBuilder setTimeout(String timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * Should a refresh be executed post this update operation causing the operation to
     * be searchable. Note, heavy indexing should not set this to <tt>true</tt>. Defaults
     * to <tt>false</tt>.
     */
    public UpdateRequestBuilder setRefresh(boolean refresh) {
        request.refresh(refresh);
        return this;
    }

    /**
     * Sets the replication type.
     */
    public UpdateRequestBuilder setReplicationType(ReplicationType replicationType) {
        request.replicationType(replicationType);
        return this;
    }

    /**
     * Sets the consistency level of write. Defaults to {@link org.elasticsearch.action.WriteConsistencyLevel#DEFAULT}
     */
    public UpdateRequestBuilder setConsistencyLevel(WriteConsistencyLevel consistencyLevel) {
        request.consistencyLevel(consistencyLevel);
        return this;
    }

    /**
     * Causes the updated document to be percolated. The parameter is the percolate query
     * to use to reduce the percolated queries that are going to run against this doc. Can be
     * set to <tt>*</tt> to indicate that all percolate queries should be run.
     */
    public UpdateRequestBuilder setPercolate(String percolate) {
        request.percolate(percolate);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<UpdateResponse> listener) {
        client.update(request, listener);
    }
}
