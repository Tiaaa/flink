/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.file.src;

import java.time.temporal.ChronoUnit;
import org.apache.flink.annotation.Internal;

import java.io.Serializable;
import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Settings describing how to do continuous file discovery and enumeration for the file source's
 * continuous discovery and streaming mode.
 */
@Internal
public final class ContinuousEnumerationSettings implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Duration discoveryInterval;

    private final Duration fileExpireTime;

    public ContinuousEnumerationSettings(Duration discoveryInterval) {
        this(discoveryInterval, ChronoUnit.FOREVER.getDuration());
    }

    public ContinuousEnumerationSettings(Duration discoveryInterval, Duration fileExpireTime) {
        this.discoveryInterval = checkNotNull(discoveryInterval);
        this.fileExpireTime = checkNotNull(fileExpireTime);
    }

    public Duration getDiscoveryInterval() {
        return discoveryInterval;
    }

    public Duration getFileExpireTime() {
        return fileExpireTime;
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "ContinuousEnumerationSettings{" +
            "discoveryInterval=" + discoveryInterval +
            ", fileExpireTime=" + fileExpireTime +
            '}';
    }
}
