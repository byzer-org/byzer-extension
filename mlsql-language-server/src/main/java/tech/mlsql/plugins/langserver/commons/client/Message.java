/*
 * Copyright (c) 2018, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tech.mlsql.plugins.langserver.commons.client;
/**
 * {@link Message} Parsed log message sent to client.
 *
 */
public class Message {
    private String id;
    private String direction;
    private String headers;
    private String httpMethod;
    private String path;
    private String contentType;
    private String payload;
    private String headerType;

    public Message(String id, String direction, String headers, String httpMethod, String path, String contentType,
                   String payload, String headerType) {
        this.id = id;
        this.direction = direction;
        this.headers = headers;
        this.httpMethod = httpMethod;
        this.path = path;
        this.contentType = contentType;
        this.payload = payload;
        this.headerType = headerType;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public String getDirection() {
        return direction;
    }

    public String getHeaders() {
        return headers;
    }

    public String getHttpMethod() {
        return httpMethod;
    }

    public String getPath() {
        return path;
    }

    public String getContentType() {
        return contentType;
    }

    public String getPayload() {
        return payload;
    }

    public String getHeaderType() {
        return headerType;
    }
}

