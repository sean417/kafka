/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.kafka.common.record.RecordBatch.NO_PARTITION_LEADER_EPOCH;

/**
 * A class encapsulating some of the logic around metadata.
 * 这个类封装了一些关于元数据的逻辑。
 * <p>
 *
 * This class is shared by the client thread (for partitioning) and the background sender thread.
 * 这个类会被生产端者线程和生产者后台线程sender使用
 * Metadata is maintained for only a subset of topics, which can be added to over time. When we request metadata for a
 * topic we don't have any metadata for it will trigger a metadata update.
 * 元数据仅仅维护着一个主题子集的相关数据，并且每个主题都有对应的过期时间。当我们请求一个元数据里没有的主题元信息时，会触发元数据的更新。
 * <p>
 * If topic expiry is enabled for the metadata, any topic that has not been used within the expiry interval
 * is removed from the metadata refresh set after an update. Consumers disable topic expiry since they explicitly
 * manage topics while producers rely on topic expiry to limit the refresh set.
 *如果元数据中能够主题过期，任何在过期时间内没使用过的主题都会从刷新主题的集合中删除。消费者能够通过显示的管理主题关闭主题过期的功能，但是
 * 生产者要依赖主题过期来限制刷新主题的集合
 *
 *
 */
public class Metadata implements Closeable {
    private final Logger log;
    private final long refreshBackoffMs;//退避时间，默认100ms
    private final long metadataExpireMs;//元数据过期时间，默认5分钟
    private int updateVersion;  // 每次更新元数据时加一
    private int requestVersion; // 每次添加一个新的主题都会加一.
    private long lastRefreshMs;//最后一次更新时间
    private long lastSuccessfulRefreshMs;//最后一次全部主题成功更新时间戳
    private KafkaException fatalException;
    private Set<String> invalidTopics;//无效的主题
    private Set<String> unauthorizedTopics;//没有权限的主题
    private MetadataCache cache = MetadataCache.empty();//元数据
    private boolean needFullUpdate;//是否需要整体更新
    private boolean needPartialUpdate;//是否需要部分更新
    private final ClusterResourceListeners clusterResourceListeners;
    private boolean isClosed;
    private final Map<TopicPartition, Integer> lastSeenLeaderEpochs;

    /**
     * Create a new Metadata instance
     *
     * @param refreshBackoffMs         The minimum amount of time that must expire between metadata refreshes to avoid busy
     *                                 polling
     * @param metadataExpireMs         The maximum amount of time that metadata can be retained without refresh
     * @param logContext               Log context corresponding to the containing client
     * @param clusterResourceListeners List of ClusterResourceListeners which will receive metadata updates.
     */
    public Metadata(long refreshBackoffMs,
                    long metadataExpireMs,
                    LogContext logContext,
                    ClusterResourceListeners clusterResourceListeners) {
        this.log = logContext.logger(Metadata.class);
        this.refreshBackoffMs = refreshBackoffMs;
        this.metadataExpireMs = metadataExpireMs;
        this.lastRefreshMs = 0L;
        this.lastSuccessfulRefreshMs = 0L;
        this.requestVersion = 0;
        this.updateVersion = 0;
        this.needFullUpdate = false;
        this.needPartialUpdate = false;
        this.clusterResourceListeners = clusterResourceListeners;
        this.isClosed = false;
        this.lastSeenLeaderEpochs = new HashMap<>();
        this.invalidTopics = Collections.emptySet();
        this.unauthorizedTopics = Collections.emptySet();
    }

    /**
     * Get the current cluster info without blocking
     */
    public synchronized Cluster fetch() {
        return cache.cluster();
    }

    /**
     * Return the next time when the current cluster info can be updated (i.e., backoff time has elapsed).
     *
     * @param nowMs current time in ms
     * @return remaining time in ms till the cluster info can be updated again
     */
    public synchronized long timeToAllowUpdate(long nowMs) {
        return Math.max(this.lastRefreshMs + this.refreshBackoffMs - nowMs, 0);
    }

    /**
     * The next time to update the cluster info is the maximum of the time the current info will expire and the time the
     * current info can be updated (i.e. backoff time has elapsed); If an update has been request then the expiry time
     * is now
     *
     * @param nowMs current time in ms
     * @return remaining time in ms till updating the cluster info
     *
     * 判断是否要更新元数据，
     * 1.如果标识要求更新，就立即更新。
     * 2.元数据过期了。
     */
    public synchronized long timeToNextUpdate(long nowMs) {
        long timeToExpire = updateRequested() ? 0 : Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0);
        return Math.max(timeToExpire, timeToAllowUpdate(nowMs));
    }

    public long metadataExpireMs() {
        return this.metadataExpireMs;
    }

    /**
     * Request an update of the current cluster metadata info, return the current updateVersion before the update
     * 做更新客户端全部主题的标记
     */
    public synchronized int requestUpdate() {
        this.needFullUpdate = true;
        return this.updateVersion;
    }
    //做更新新主题的标记
    public synchronized int requestUpdateForNewTopics() {
        // Override the timestamp of last refresh to let immediate update.
        this.lastRefreshMs = 0;
        this.needPartialUpdate = true;
        this.requestVersion++;
        return this.updateVersion;
    }

    /**
     * Request an update for the partition metadata iff we have seen a newer leader epoch. This is called by the client
     * any time it handles a response from the broker that includes leader epoch, except for UpdateMetadata which
     * follows a different code path ({@link #update}).
     *
     * @param topicPartition
     * @param leaderEpoch
     * @return true if we updated the last seen epoch, false otherwise
     */
    public synchronized boolean updateLastSeenEpochIfNewer(TopicPartition topicPartition, int leaderEpoch) {
        Objects.requireNonNull(topicPartition, "TopicPartition cannot be null");
        if (leaderEpoch < 0)
            throw new IllegalArgumentException("Invalid leader epoch " + leaderEpoch + " (must be non-negative)");

        Integer oldEpoch = lastSeenLeaderEpochs.get(topicPartition);
        log.trace("Determining if we should replace existing epoch {} with new epoch {} for partition {}", oldEpoch, leaderEpoch, topicPartition);

        final boolean updated;
        if (oldEpoch == null) {
            log.debug("Not replacing null epoch with new epoch {} for partition {}", leaderEpoch, topicPartition);
            updated = false;
        } else if (leaderEpoch > oldEpoch) {
            log.debug("Updating last seen epoch from {} to {} for partition {}", oldEpoch, leaderEpoch, topicPartition);
            lastSeenLeaderEpochs.put(topicPartition, leaderEpoch);
            updated = true;
        } else {
            log.debug("Not replacing existing epoch {} with new epoch {} for partition {}", oldEpoch, leaderEpoch, topicPartition);
            updated = false;
        }

        this.needFullUpdate = this.needFullUpdate || updated;
        return updated;
    }

    public Optional<Integer> lastSeenLeaderEpoch(TopicPartition topicPartition) {
        return Optional.ofNullable(lastSeenLeaderEpochs.get(topicPartition));
    }

    /**
     * Check whether an update has been explicitly requested.
     * 元数据是否已经被更新了
     * @return true if an update was requested, false otherwise
     */
    public synchronized boolean updateRequested() {
        return this.needFullUpdate || this.needPartialUpdate;
    }

    /**
     * Return the cached partition info if it exists and a newer leader epoch isn't known about.
     */
    synchronized Optional<MetadataResponse.PartitionMetadata> partitionMetadataIfCurrent(TopicPartition topicPartition) {
        Integer epoch = lastSeenLeaderEpochs.get(topicPartition);
        Optional<MetadataResponse.PartitionMetadata> partitionMetadata = cache.partitionMetadata(topicPartition);
        if (epoch == null) {
            // old cluster format (no epochs)
            return partitionMetadata;
        } else {
            return partitionMetadata.filter(metadata ->
                    metadata.leaderEpoch.orElse(NO_PARTITION_LEADER_EPOCH).equals(epoch));
        }
    }

    public synchronized LeaderAndEpoch currentLeader(TopicPartition topicPartition) {
        Optional<MetadataResponse.PartitionMetadata> maybeMetadata = partitionMetadataIfCurrent(topicPartition);
        if (!maybeMetadata.isPresent())
            return new LeaderAndEpoch(Optional.empty(), Optional.ofNullable(lastSeenLeaderEpochs.get(topicPartition)));

        MetadataResponse.PartitionMetadata partitionMetadata = maybeMetadata.get();
        Optional<Integer> leaderEpochOpt = partitionMetadata.leaderEpoch;
        Optional<Node> leaderNodeOpt = partitionMetadata.leaderId.flatMap(cache::nodeById);
        return new LeaderAndEpoch(leaderNodeOpt, leaderEpochOpt);
    }
    //元数据引导方法。
    public synchronized void bootstrap(List<InetSocketAddress> addresses) {
        this.needFullUpdate = true;
        this.updateVersion += 1;
        this.cache = MetadataCache.bootstrap(addresses);
    }

    /**
     * Update metadata assuming the current request version.
     *
     * For testing only.
     */
    public synchronized void updateWithCurrentRequestVersion(MetadataResponse response, boolean isPartialUpdate, long nowMs) {
        this.update(this.requestVersion, response, isPartialUpdate, nowMs);
    }

    /**
     * Updates the cluster metadata. If topic expiry is enabled, expiry time
     * is set for topics if required and expired topics are removed from the metadata.
     *
     * @param requestVersion The request version corresponding to the update response, as provided by
     *     {@link #newMetadataRequestAndVersion(long)}.
     * @param response metadata response received from the broker
     * @param isPartialUpdate whether the metadata request was for a subset of the active topics
     * @param nowMs current time in milliseconds
     */
    public synchronized void update(int requestVersion, MetadataResponse response, boolean isPartialUpdate, long nowMs) {
        Objects.requireNonNull(response, "Metadata response cannot be null");
        if (isClosed())
            throw new IllegalStateException("Update requested after metadata close");
        //1.判断是否是部分主题更新，以及更新几个字段
        this.needPartialUpdate = requestVersion < this.requestVersion;
        this.lastRefreshMs = nowMs;
        this.updateVersion += 1;
        if (!isPartialUpdate) {
            this.needFullUpdate = false;
            this.lastSuccessfulRefreshMs = nowMs;
        }

        String previousClusterId = cache.clusterResource().clusterId();
        //2.解析元数据响应
        this.cache = handleMetadataResponse(response, isPartialUpdate, nowMs);

        Cluster cluster = cache.cluster();
        maybeSetMetadataError(cluster);

        this.lastSeenLeaderEpochs.keySet().removeIf(tp -> !retainTopic(tp.topic(), false, nowMs));

        String newClusterId = cache.clusterResource().clusterId();
        if (!Objects.equals(previousClusterId, newClusterId)) {
            log.info("Cluster ID: {}", newClusterId);
        }
        clusterResourceListeners.onUpdate(cache.clusterResource());

        log.debug("Updated cluster metadata updateVersion {} to {}", this.updateVersion, this.cache);
    }

    private void maybeSetMetadataError(Cluster cluster) {
        clearRecoverableErrors();
        checkInvalidTopics(cluster);
        checkUnauthorizedTopics(cluster);
    }

    private void checkInvalidTopics(Cluster cluster) {
        if (!cluster.invalidTopics().isEmpty()) {
            log.error("Metadata response reported invalid topics {}", cluster.invalidTopics());
            invalidTopics = new HashSet<>(cluster.invalidTopics());
        }
    }

    private void checkUnauthorizedTopics(Cluster cluster) {
        if (!cluster.unauthorizedTopics().isEmpty()) {
            log.error("Topic authorization failed for topics {}", cluster.unauthorizedTopics());
            unauthorizedTopics = new HashSet<>(cluster.unauthorizedTopics());
        }
    }

    /**
     * Transform a MetadataResponse into a new MetadataCache instance.
     * 解析元数据并根据元数据实例化新的MetadataCache对象。
     *
     */
    private MetadataCache handleMetadataResponse(MetadataResponse metadataResponse, boolean isPartialUpdate, long nowMs) {
        // All encountered topics.

        Set<String> topics = new HashSet<>();

        // 1.初始化相关集合Retained topics to be passed to the metadata cache.
        Set<String> internalTopics = new HashSet<>();
        Set<String> unauthorizedTopics = new HashSet<>();
        Set<String> invalidTopics = new HashSet<>();

        List<MetadataResponse.PartitionMetadata> partitions = new ArrayList<>();
        //2.轮询响应中的主题元数据。
        for (MetadataResponse.TopicMetadata metadata : metadataResponse.topicMetadata()) {
            topics.add(metadata.topic());
            //3.判断是否保留主题元数据。
            if (!retainTopic(metadata.topic(), metadata.isInternal(), nowMs))
                continue;
            //4.判断是否是内部主题。
            if (metadata.isInternal())
                internalTopics.add(metadata.topic());
            //5.如果元数据响应没有错误就更新本地元数据缓存
            if (metadata.error() == Errors.NONE) {
                //6.遍历分区信息
                for (MetadataResponse.PartitionMetadata partitionMetadata : metadata.partitionMetadata()) {
                    // Even if the partition's metadata includes an error, we need to handle
                    // the update to catch new epochs
                    updateLatestMetadata(partitionMetadata, metadataResponse.hasReliableLeaderEpochs())
                        .ifPresent(partitions::add);
                    //分区数据有问题
                    if (partitionMetadata.error.exception() instanceof InvalidMetadataException) {
                        log.debug("Requesting metadata update for partition {} due to error {}",
                                partitionMetadata.topicPartition, partitionMetadata.error);
                        //标记需要更新元数据
                        requestUpdate();
                    }
                }
                //如果元数据响应有错误
            } else {
                //无效元数据异常
                if (metadata.error().exception() instanceof InvalidMetadataException) {
                    log.debug("Requesting metadata update for topic {} due to error {}", metadata.topic(), metadata.error());
                    //标记需要更新元数据
                    requestUpdate();
                }
                //如果是无效主题的错误
                if (metadata.error() == Errors.INVALID_TOPIC_EXCEPTION)
                    invalidTopics.add(metadata.topic());
                //如果是无权限主题的错误
                else if (metadata.error() == Errors.TOPIC_AUTHORIZATION_FAILED)
                    unauthorizedTopics.add(metadata.topic());
            }
        }

        Map<Integer, Node> nodes = metadataResponse.brokersById();
        //8.如果是部分主题的响应就和现在的元数据缓存整合在一起，如果不是就重建元数据缓存
        if (isPartialUpdate)
            return this.cache.mergeWith(metadataResponse.clusterId(), nodes, partitions,
                unauthorizedTopics, invalidTopics, internalTopics, metadataResponse.controller(),
                (topic, isInternal) -> !topics.contains(topic) && retainTopic(topic, isInternal, nowMs));
        else
            return new MetadataCache(metadataResponse.clusterId(), nodes, partitions,
                unauthorizedTopics, invalidTopics, internalTopics, metadataResponse.controller());
    }

    /**
     * Compute the latest partition metadata to cache given ordering by leader epochs (if both
     * available and reliable).
     */
    private Optional<MetadataResponse.PartitionMetadata> updateLatestMetadata(
            MetadataResponse.PartitionMetadata partitionMetadata,
            boolean hasReliableLeaderEpoch) {
        TopicPartition tp = partitionMetadata.topicPartition;
        if (hasReliableLeaderEpoch && partitionMetadata.leaderEpoch.isPresent()) {
            int newEpoch = partitionMetadata.leaderEpoch.get();
            // If the received leader epoch is at least the same as the previous one, update the metadata
            Integer currentEpoch = lastSeenLeaderEpochs.get(tp);
            if (currentEpoch == null || newEpoch >= currentEpoch) {
                log.debug("Updating last seen epoch for partition {} from {} to epoch {} from new metadata", tp, currentEpoch, newEpoch);
                lastSeenLeaderEpochs.put(tp, newEpoch);
                return Optional.of(partitionMetadata);
            } else {
                // Otherwise ignore the new metadata and use the previously cached info
                log.debug("Got metadata for an older epoch {} (current is {}) for partition {}, not updating", newEpoch, currentEpoch, tp);
                return cache.partitionMetadata(tp);
            }
        } else {
            // Handle old cluster formats as well as error responses where leader and epoch are missing
            lastSeenLeaderEpochs.remove(tp);
            return Optional.of(partitionMetadata.withoutLeaderEpoch());
        }
    }

    /**
     * If any non-retriable exceptions were encountered during metadata update, clear and throw the exception.
     * This is used by the consumer to propagate any fatal exceptions or topic exceptions for any of the topics
     * in the consumer's Metadata.
     */
    public synchronized void maybeThrowAnyException() {
        clearErrorsAndMaybeThrowException(this::recoverableException);
    }

    /**
     * If any fatal exceptions were encountered during metadata update, throw the exception. This is used by
     * the producer to abort waiting for metadata if there were fatal exceptions (e.g. authentication failures)
     * in the last metadata update.
     */
    protected synchronized void maybeThrowFatalException() {
        KafkaException metadataException = this.fatalException;
        if (metadataException != null) {
            fatalException = null;
            throw metadataException;
        }
    }

    /**
     * If any non-retriable exceptions were encountered during metadata update, throw exception if the exception
     * is fatal or related to the specified topic. All exceptions from the last metadata update are cleared.
     * This is used by the producer to propagate topic metadata errors for send requests.
     */
    public synchronized void maybeThrowExceptionForTopic(String topic) {
        clearErrorsAndMaybeThrowException(() -> recoverableExceptionForTopic(topic));
    }

    private void clearErrorsAndMaybeThrowException(Supplier<KafkaException> recoverableExceptionSupplier) {
        KafkaException metadataException = Optional.ofNullable(fatalException).orElseGet(recoverableExceptionSupplier);
        fatalException = null;
        clearRecoverableErrors();
        if (metadataException != null)
            throw metadataException;
    }

    // We may be able to recover from this exception if metadata for this topic is no longer needed
    private KafkaException recoverableException() {
        if (!unauthorizedTopics.isEmpty())
            return new TopicAuthorizationException(unauthorizedTopics);
        else if (!invalidTopics.isEmpty())
            return new InvalidTopicException(invalidTopics);
        else
            return null;
    }

    private KafkaException recoverableExceptionForTopic(String topic) {
        if (unauthorizedTopics.contains(topic))
            return new TopicAuthorizationException(Collections.singleton(topic));
        else if (invalidTopics.contains(topic))
            return new InvalidTopicException(Collections.singleton(topic));
        else
            return null;
    }

    private void clearRecoverableErrors() {
        invalidTopics = Collections.emptySet();
        unauthorizedTopics = Collections.emptySet();
    }

    /**
     * Record an attempt to update the metadata that failed. We need to keep track of this
     * to avoid retrying immediately.
     */
    public synchronized void failedUpdate(long now) {
        this.lastRefreshMs = now;
    }

    /**
     * Propagate a fatal error which affects the ability to fetch metadata for the cluster.
     * Two examples are authentication and unsupported version exceptions.
     *
     * @param exception The fatal exception
     */
    public synchronized void fatalError(KafkaException exception) {
        this.fatalException = exception;
    }

    /**
     * @return The current metadata updateVersion
     */
    public synchronized int updateVersion() {
        return this.updateVersion;
    }

    /**
     * The last time metadata was successfully updated.
     */
    public synchronized long lastSuccessfulUpdate() {
        return this.lastSuccessfulRefreshMs;
    }

    /**
     * Close this metadata instance to indicate that metadata updates are no longer possible.
     */
    @Override
    public synchronized void close() {
        this.isClosed = true;
    }

    /**
     * Check if this metadata instance has been closed. See {@link #close()} for more information.
     *
     * @return True if this instance has been closed; false otherwise
     */
    public synchronized boolean isClosed() {
        return this.isClosed;
    }

    public synchronized MetadataRequestAndVersion newMetadataRequestAndVersion(long nowMs) {
        MetadataRequest.Builder request = null;
        boolean isPartialUpdate = false;

        // Perform a partial update only if a full update hasn't been requested, and the last successful
        // hasn't exceeded the metadata refresh time.
        // 如果是部分主题更新就进入判断。
        // 1.needFullUpdate为false:不是全部主题的元数据都要求更新。
        // 2.没有到元数据超时时间，默认五分钟。
        if (!this.needFullUpdate && this.lastSuccessfulRefreshMs + this.metadataExpireMs > nowMs) {
            request = newMetadataRequestBuilderForNewTopics();
            isPartialUpdate = true;
        }
        if (request == null) {
            request = newMetadataRequestBuilder();
            isPartialUpdate = false;
        }
        return new MetadataRequestAndVersion(request, requestVersion, isPartialUpdate);
    }

    /**
     * Constructs and returns a metadata request builder for fetching cluster data and all active topics.
     *
     * @return the constructed non-null metadata builder
     */
    protected MetadataRequest.Builder newMetadataRequestBuilder() {
        return MetadataRequest.Builder.allTopics();
    }

    /**
     * Constructs and returns a metadata request builder for fetching cluster data and any uncached topics,
     * otherwise null if the functionality is not supported.
     *
     * @return the constructed metadata builder, or null if not supported
     */
    protected MetadataRequest.Builder newMetadataRequestBuilderForNewTopics() {
        return null;
    }

    protected boolean retainTopic(String topic, boolean isInternal, long nowMs) {
        return true;
    }

    public static class MetadataRequestAndVersion {
        public final MetadataRequest.Builder requestBuilder;
        public final int requestVersion;
        public final boolean isPartialUpdate;

        private MetadataRequestAndVersion(MetadataRequest.Builder requestBuilder,
                                          int requestVersion,
                                          boolean isPartialUpdate) {
            this.requestBuilder = requestBuilder;
            this.requestVersion = requestVersion;
            this.isPartialUpdate = isPartialUpdate;
        }
    }

    /**
     * Represents current leader state known in metadata. It is possible that we know the leader, but not the
     * epoch if the metadata is received from a broker which does not support a sufficient Metadata API version.
     * It is also possible that we know of the leader epoch, but not the leader when it is derived
     * from an external source (e.g. a committed offset).
     */
    public static class LeaderAndEpoch {
        private static final LeaderAndEpoch NO_LEADER_OR_EPOCH = new LeaderAndEpoch(Optional.empty(), Optional.empty());

        public final Optional<Node> leader;
        public final Optional<Integer> epoch;

        public LeaderAndEpoch(Optional<Node> leader, Optional<Integer> epoch) {
            this.leader = Objects.requireNonNull(leader);
            this.epoch = Objects.requireNonNull(epoch);
        }

        public static LeaderAndEpoch noLeaderOrEpoch() {
            return NO_LEADER_OR_EPOCH;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LeaderAndEpoch that = (LeaderAndEpoch) o;

            if (!leader.equals(that.leader)) return false;
            return epoch.equals(that.epoch);
        }

        @Override
        public int hashCode() {
            int result = leader.hashCode();
            result = 31 * result + epoch.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "LeaderAndEpoch{" +
                    "leader=" + leader +
                    ", epoch=" + epoch.map(Number::toString).orElse("absent") +
                    '}';
        }
    }
}
