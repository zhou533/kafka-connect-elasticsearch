/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/
package io.confluent.connect.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.confluent.connect.elasticsearch.bulk.BulkClient;
import io.confluent.connect.elasticsearch.bulk.BulkResponse;

public class BulkIndexingClient implements BulkClient<IndexableRecord, BulkRequestBuilder> {

  private static final Logger LOG = LoggerFactory.getLogger(BulkIndexingClient.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Client client;

  public BulkIndexingClient(Client client) {
    this.client = client;
  }

  @Override
  public BulkRequestBuilder bulkRequest(List<IndexableRecord> batch) {
    /*final Bulk.Builder builder = new Bulk.Builder();
    for (IndexableRecord record : batch) {
      builder.addAction(record.toIndexRequest());
    }
    return builder.build();*/
    BulkRequestBuilder bulkRequest = client.prepareBulk();
    for (IndexableRecord record : batch) {
      bulkRequest.add(record.toIndexRequest(client));
    }
    return bulkRequest;
  }

  @Override
  public BulkResponse execute(BulkRequestBuilder bulk) throws IOException {
    final org.elasticsearch.action.bulk.BulkResponse result = bulk.get();

    if (!result.hasFailures()) {
      return BulkResponse.success();
    }

    boolean retriable = true;

    final List<Key> versionConflicts = new ArrayList<>();
    final List<String> errors = new ArrayList<>();

    for (BulkItemResponse item : result.getItems()) {
      if (item.isFailed()) {
        //final ObjectNode parsedError = (ObjectNode) OBJECT_MAPPER.readTree(item.error);
        //final String errorType = parsedError.get("type").asText("");
        if (item.getFailure().getCause() instanceof VersionConflictEngineException/*"version_conflict_engine_exception".equals(errorType)*/) {
          versionConflicts.add(new Key(item.getIndex(), item.getType(), item.getId()));
        } else if (item.getFailure().getCause() instanceof MapperParsingException/*"mapper_parse_exception".equals(errorType)*/) {
          retriable = false;
          errors.add(item.getFailureMessage());
        } else {
          errors.add(item.getFailureMessage());
        }
      }
    }

    if (!versionConflicts.isEmpty()) {
      LOG.debug("Ignoring version conflicts for items: {}", versionConflicts);
      if (errors.isEmpty()) {
        // The only errors were version conflicts
        return BulkResponse.success();
      }
    }

    final String errorInfo = errors.isEmpty() ? result.buildFailureMessage() : errors.toString();

    return BulkResponse.failure(retriable, errorInfo);
  }

}
