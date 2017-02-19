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

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;

public class IndexableRecord {

  public final Key key;
  public final String payload;
  public final Long version;

  public IndexableRecord(Key key, String payload, Long version) {
    this.key = key;
    this.version = version;
    this.payload = payload;
  }

  public IndexRequestBuilder toIndexRequest(Client client) {
    /*Index.Builder req = new Index.Builder(payload)
        .index(key.index)
        .type(key.type)
        .id(key.id);
    if (version != null) {
      req.setParameter("version_type", "external").setParameter("version", version);
    }
    return req.build();*/
    IndexRequestBuilder request = client.prepareIndex(key.index, key.type, key.id)
            .setSource(
              payload      
            );
    return request;
  }

}
