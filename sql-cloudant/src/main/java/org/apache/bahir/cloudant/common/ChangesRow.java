/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bahir.cloudant.common;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.List;

/**
 * Class representing a single row in a changes feed. Structure:
 *
 * {
 *   last_seq": 5
 *   "results": [
 *     ---*** This next items is the ChangesRow ***---
 *     {
 *       "changes": [ {"rev": "2-eec205a9d413992850a6e32678485900"}, ... ],
 *       "deleted": true,
 *       "id": "deleted",
 *       "seq": 5,
 *       "doc": ... structure ...
 *     }
 *   ]
 * }
 */
public class ChangesRow {

    public class Rev {
        private String rev;

        public String getRev() {
            return rev;
        }
    }

    private List<Rev> changes;
    public Boolean deleted;
    private String id;
    private JsonElement seq;
    private JsonObject doc;

    public List<Rev> getChanges() {
        return changes;
    }

    public String getSeq() {
        if (seq.isJsonNull()) {
            return null;
        } else {
            return seq.toString();
        }
    }

    public String getId() {
        return id;
    }

    public JsonObject getDoc() {
        return doc;
    }

}
