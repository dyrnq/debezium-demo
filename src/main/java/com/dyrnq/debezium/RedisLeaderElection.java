package com.dyrnq.debezium;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Redis-based leader election with fencing token (epoch) to prevent
 * dual-write / brain-split scenarios.
 *
 * <h3>Fencing design</h3>
 * <ul>
 *   <li>Lock value = {@code instanceId:epoch} (e.g. {@code a1b2c3d4:7})</li>
 *   <li>Epoch is a monotonically increasing counter stored in a separate Redis key</li>
 *   <li>On acquire: Lua script atomically checks lock absence → INCR epoch → SET lock</li>
 *   <li>On renew: Lua script atomically verifies lock ownership → extends TTL → returns epoch</li>
 *   <li>Consumer can call {@link #isValidLeader()} before each batch write to verify
 *       that this instance still holds the lock with the correct epoch</li>
 * </ul>
 */
@Component
@Slf4j
public class RedisLeaderElection {

    private final StringRedisTemplate redisTemplate;
    private final String lockKey;
    private final String epochKey;
    private final String instanceId;
    private final AtomicBoolean leader = new AtomicBoolean(false);
    private final AtomicBoolean released = new AtomicBoolean(false);

    /** The epoch (fencing token) we hold for this leadership period. */
    private final AtomicLong fencingToken = new AtomicLong(-1);

    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> renewalTask;

    private static final long LOCK_TTL_SECONDS = 30;

    // ── Lua scripts ─────────────────────────────────────────────

    /**
     * ACQUIRE: KEYS[1]=lockKey, KEYS[2]=epochKey, ARGV[1]=instanceId
     *
     * If the lock is absent or expired:
     *   1. INCR epoch → new epoch
     *   2. SET lock = "{instanceId}:{epoch}" with TTL
     *   3. RETURN epoch
     * Otherwise RETURN -1 (lock held by someone else).
     */
    private static final DefaultRedisScript<Long> ACQUIRE_SCRIPT;
    static {
        ACQUIRE_SCRIPT = new DefaultRedisScript<>();
        ACQUIRE_SCRIPT.setScriptText(
                "local cur = redis.call('get', KEYS[1]) " +
                "if cur == false or cur == nil then " +
                "  local ep = redis.call('incr', KEYS[2]) " +
                "  local val = ARGV[1] .. ':' .. tostring(ep) " +
                "  redis.call('set', KEYS[1], val, 'EX', " + LOCK_TTL_SECONDS + ") " +
                "  return ep " +
                "end " +
                "return -1");
        ACQUIRE_SCRIPT.setResultType(Long.class);
    }

    /**
     * RENEW: KEYS[1]=lockKey, ARGV[1]=lockValue (instanceId:epoch)
     *
     * If lock value matches ours → extend TTL, return 1.
     * Otherwise → return -1 (we lost leadership).
     */
    private static final DefaultRedisScript<Long> RENEW_SCRIPT;
    static {
        RENEW_SCRIPT = new DefaultRedisScript<>();
        RENEW_SCRIPT.setScriptText(
                "local cur = redis.call('get', KEYS[1]) " +
                "if cur == ARGV[1] then " +
                "  redis.call('expire', KEYS[1], " + LOCK_TTL_SECONDS + ") " +
                "  return 1 " +
                "end " +
                "return -1");
        RENEW_SCRIPT.setResultType(Long.class);
    }

    /**
     * RELEASE: KEYS[1]=lockKey, ARGV[1]=lockValue (instanceId:epoch)
     *
     * Only deletes the key if the value matches (prevents releasing a lock
     * that was already re-acquired by another instance).
     */
    private static final DefaultRedisScript<Boolean> RELEASE_SCRIPT;
    static {
        RELEASE_SCRIPT = new DefaultRedisScript<>();
        RELEASE_SCRIPT.setScriptText(
                "if redis.call('get', KEYS[1]) == ARGV[1] then " +
                "  return redis.call('del', KEYS[1]) " +
                "else " +
                "  return 0 " +
                "end");
        RELEASE_SCRIPT.setResultType(Boolean.class);
    }

    // ── Constructor ─────────────────────────────────────────────

    public RedisLeaderElection(
            StringRedisTemplate redisTemplate,
            @Value("${debezium.namespace:default}") String namespace) {
        this.redisTemplate = redisTemplate;
        this.lockKey = "debezium:leader:" + namespace;
        this.epochKey = lockKey + ":epoch";
        String host = System.getenv("HOSTNAME");
        String seed = (host != null ? host : "unknown") + "-" + UUID.randomUUID().toString();
        this.instanceId = Integer.toHexString(seed.hashCode());
    }

    // ── Acquire / Renew / Release ───────────────────────────────

    /**
     * Try to acquire the leadership lock. Returns true if this instance
     * became the leader, with a new fencing token (epoch).
     */
    public boolean tryAcquire() {
        Long epoch = redisTemplate.execute(ACQUIRE_SCRIPT,
                Arrays.asList(lockKey, epochKey), instanceId);
        if (epoch != null && epoch > 0) {
            fencingToken.set(epoch);
            leader.set(true);
            log.info("Acquired leadership lock (instance={}, epoch={})", instanceId, epoch);
            return true;
        }
        return false;
    }

    /**
     * Start periodic renewal of the leadership lock.
     * If the lock value no longer matches ours (another leader took over),
     * the leader flag is cleared and renewal stops.
     */
    public void startRenewal() {
        if (scheduler == null || scheduler.isShutdown()) {
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "leader-renewal");
                t.setDaemon(true);
                return t;
            });
        }
        if (renewalTask != null) {
            renewalTask.cancel(false);
        }
        renewalTask = scheduler.scheduleAtFixedRate(() -> {
            if (!leader.get()) return;
            String lockValue = instanceId + ":" + fencingToken.get();
            Long result = redisTemplate.execute(RENEW_SCRIPT,
                    Collections.singletonList(lockKey), lockValue);
            if (!Long.valueOf(1L).equals(result)) {
                log.warn("Leadership lock lost (fencing token mismatch) — stepping down");
                leader.set(false);
                stopRenewal();
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    public void stopRenewal() {
        if (renewalTask != null) {
            renewalTask.cancel(false);
        }
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
        }
    }

    /**
     * Release the leadership lock, but only if we still own it.
     */
    public void release() {
        if (!released.compareAndSet(false, true)) return;
        leader.set(false);
        stopRenewal();
        String lockValue = instanceId + ":" + fencingToken.get();
        redisTemplate.execute(RELEASE_SCRIPT, Collections.singletonList(lockKey), lockValue);
        log.info("Released leadership lock (instance={}, epoch={})", instanceId, fencingToken.get());
    }

    // ── Fencing token validation ────────────────────────────────

    /**
     * Check if this instance is still the valid leader by verifying the
     * lock value in Redis matches our instanceId:epoch.
     *
     * This is the core fencing mechanism — call this before each batch
     * write to detect if another leader has taken over (e.g. after a GC
     * pause caused the lock to expire).
     *
     * @return true if we are still the valid leader with the correct epoch
     */
    public boolean isValidLeader() {
        if (!leader.get()) return false;
        String currentValue = redisTemplate.opsForValue().get(lockKey);
        String expected = instanceId + ":" + fencingToken.get();
        if (!expected.equals(currentValue)) {
            log.warn("Fencing check failed: expected={}, actual={} — stepping down",
                    expected, currentValue);
            leader.set(false);
            return false;
        }
        return true;
    }

    /**
     * Get the current fencing token (epoch). Higher epoch = more recent
     * leadership acquisition. Used for monitoring.
     */
    public long getFencingToken() {
        return fencingToken.get();
    }

    // ── Accessors ───────────────────────────────────────────────

    public boolean isLeader() {
        return leader.get();
    }

    public String getInstanceId() {
        return instanceId;
    }
}
