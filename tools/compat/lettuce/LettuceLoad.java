// A Lettuce cluster client for migrate_check.py: INCRs counters k:0 ..
// k:N-1 one after the other, round and round, until its stdin closes, then
// prints, for each counter written, "i acked doubt" (doubt: the INCR
// failed or timed out, it may or may not have been applied) and exits.
//
//   java -cp 'jars/*:classes' LettuceLoad HOST PORT N REFRESH
//
// REFRESH is how the client keeps its slot map: "default" (Lettuce 7's:
// every adaptive trigger, -MOVED and -ASK among them, at most one refresh
// in 5 s), "off" (no adaptive refresh: it follows -MOVED without updating
// its map, Lettuce 6's default) or "fast" (the triggers limited to one
// refresh a second, and a periodic refresh every 10 s). REFRESH@MS also
// caps the delay between reconnection attempts at MS milliseconds (Lettuce's
// default backs off up to 30 s). Progress goes to stderr once a second.

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import java.net.InetAddress;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.Delay;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class LettuceLoad {
    public static void main(String[] args) throws Exception {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        int n = Integer.parseInt(args[2]);
        String[] mode = args[3].split("@");
        String refresh = mode[0];
        ClientResources.Builder resources = ClientResources.builder();
        // -Dlettuce.nodnscache: names resolved by the JVM, uncached, instead of
        // netty's resolver, which caches each answer for its TTL.
        if (System.getProperty("lettuce.nodnscache") != null) {
            java.security.Security.setProperty("networkaddress.cache.ttl", "0");
            java.security.Security.setProperty("networkaddress.cache.negative.ttl", "0");
            resources.addressResolverGroup(io.netty.resolver.DefaultAddressResolverGroup.INSTANCE);
        }
        if (mode.length > 1) {
            Duration max = Duration.ofMillis(Long.parseLong(mode[1]));
            resources.reconnectDelay(Delay.exponential(Duration.ofMillis(10), max, 2, TimeUnit.MILLISECONDS));
        }

        ClusterTopologyRefreshOptions.Builder topology = ClusterTopologyRefreshOptions.builder();
        switch (refresh) {
            case "default":
                break;
            case "off":
                topology.disableAllAdaptiveRefreshTriggers();
                break;
            case "fast":
                topology.adaptiveRefreshTriggersTimeout(Duration.ofSeconds(1)).enablePeriodicRefresh(Duration.ofSeconds(10));
                break;
            default:
                throw new IllegalArgumentException("REFRESH is default, off or fast: " + refresh);
        }
        RedisURI seed = RedisURI.builder().withHost(host).withPort(port).withTimeout(Duration.ofSeconds(5)).build();
        RedisClusterClient client = RedisClusterClient.create(resources.build(), seed);
        client.setOptions(ClusterClientOptions.builder().topologyRefreshOptions(topology.build()).build());
        StatefulRedisClusterConnection<String, String> connection = client.connect();
        RedisAdvancedClusterCommands<String, String> commands = connection.sync();

        AtomicBoolean stop = new AtomicBoolean();
        Thread stdin = new Thread(() -> {
            try {
                while (System.in.read() >= 0) {
                }
            } catch (Exception e) {
            }
            stop.set(true);
        });
        stdin.setDaemon(true);
        stdin.start();

        long[] acked = new long[n];
        long[] doubt = new long[n];
        long ops = 0, errors = 0, last = System.nanoTime();
        String lastError = "";
        for (int i = 0; !stop.get(); i = (i + 1) % n) {
            try {
                commands.incr("k:" + i);
                acked[i]++;
            } catch (Exception e) {
                doubt[i]++;
                errors++;
                lastError = e.toString();
                int slot = SlotHash.getSlot("k:" + i);
                RedisClusterNode owner = connection.getPartitions().getPartitionBySlot(slot);
                String where = owner == null ? "no owner" : owner.getNodeId() + " at " + owner.getUri().getHost() + ":" + owner.getUri().getPort();
                String resolves;
                try {
                    resolves = owner == null ? "-" : InetAddress.getByName(owner.getUri().getHost()).getHostAddress();
                } catch (Exception r) {
                    resolves = r.toString();
                }
                System.err.println(Instant.now() + " lettuce " + args[3] + ": k:" + i + " failed: " + lastError
                        + " [slot " + slot + ", Lettuce's map: " + where + ", which resolves to " + resolves + "]");
            }
            ops++;
            if (System.nanoTime() - last > 1_000_000_000L) {
                System.err.println("lettuce " + refresh + ": " + ops + " ops, " + errors + " errors " + lastError);
                last = System.nanoTime();
            }
        }
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < n; i++) {
            if (acked[i] + doubt[i] > 0) {
                out.append(i).append(' ').append(acked[i]).append(' ').append(doubt[i]).append('\n');
            }
        }
        System.out.print(out);
        System.out.flush();
        connection.close();
        client.shutdown(Duration.ZERO, Duration.ofSeconds(2));
    }
}
