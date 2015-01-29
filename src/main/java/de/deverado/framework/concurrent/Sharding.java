package de.deverado.framework.concurrent;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import de.deverado.framework.core.Strings2;
import de.deverado.framework.core.Utils;

public class Sharding {

    public interface ShardingStrategyGeneric<T> {
        int shard(T o);

        public int getShardCount();
    }

    public static ShardingStrategyGeneric<Object> createHashCodeShardingStrategyGeneric(
            int shards) {
        final ShardingStrategy s = new ShardByArbitraryStrategy(shards);
        return new ShardingStrategyGeneric<Object>() {

            @Override
            public int getShardCount() {
                return s.getShardCount();
            }

            @Override
            public int shard(Object o) {
                return s.shard(o == null ? 0 : o.hashCode());
            }
        };
    }

    public interface ShardingStrategy {

        public int shard(CharSequence val);

        public int shard(int val);

        public int shard(byte[] val);

        public int getShardCount();
    }

    public final static ShardBy16384Strategy by16384Instance = new ShardBy16384Strategy();

    public static class ShardBy16384Strategy extends
            ShardingStrategyMurmur3Adapter {

        @Override
        public int shard(int val) {
            return Utils.hashTo16384(val);
        }

        @Override
        public int getShardCount() {
            return 16384;
        }

    }

    public final static ShardBy4096Strategy by4096Instance = new ShardBy4096Strategy();

    public static class ShardBy4096Strategy extends
            ShardingStrategyMurmur3Adapter {

        @Override
        public int shard(int val) {
            return Utils.hashTo4096(val);
        }

        @Override
        public int getShardCount() {
            return 4096;
        }

    }

    public final static ShardBy512Strategy by512Instance = new ShardBy512Strategy();

    public static class ShardBy512Strategy extends
            ShardingStrategyMurmur3Adapter {

        @Override
        public int shard(int val) {
            return Utils.hashTo512(val);
        }

        @Override
        public int getShardCount() {
            return 512;
        }

    }

    public final static ShardBy256Strategy by256Instance = new ShardBy256Strategy();

    public static class ShardBy256Strategy extends
            ShardingStrategyMurmur3Adapter {

        @Override
        public int shard(int val) {
            return Utils.hashTo256(val);
        }

        @Override
        public int getShardCount() {
            return 256;
        }

    }

    public final static ShardBy16Strategy by16Instance = new ShardBy16Strategy();

    public static class ShardBy16Strategy extends
            ShardingStrategyMurmur3Adapter {

        @Override
        public int shard(int val) {
            return Utils.hashTo16(val);
        }

        @Override
        public int getShardCount() {
            return 16;
        }

    }

    /**
     * Hashes and folds the input to nearest value and does a mod.
     */
    public static class ShardByArbitraryStrategy implements ShardingStrategy {

        private final ShardingStrategy internal;
        private final int modulus;

        /**
         * 
         * @param shards
         *            should be a power of 2, else strong bias possible. E.g. 10
         *            leads to 60 percent different between smallest and biggest
         *            bucket.
         */
        public ShardByArbitraryStrategy(int shards) {
            modulus = shards;
            if (shards < 1) {
                throw new IllegalArgumentException("Shard number < 1");
            }
            if (shards <= 16) {
                internal = by16Instance;
            } else if (shards <= 256) {
                internal = by256Instance;
            } else if (shards <= 512) {
                internal = by512Instance;
            } else if (shards <= 4096) {
                internal = by4096Instance;
            } else if (shards <= 16384) {
                internal = by16384Instance;
            } else {
                throw new IllegalArgumentException(
                        "Max 16384 shards currently.");
            }
        }

        @Override
        public int shard(byte[] val) {
            return internal.shard(val) % modulus;
        }

        @Override
        public int shard(CharSequence val) {
            return internal.shard(val) % modulus;
        }

        @Override
        public int shard(int val) {
            return internal.shard(val) % modulus;
        }

        @Override
        public int getShardCount() {
            return modulus;
        }

    }

    public static abstract class ShardingStrategyMurmur3Adapter implements
            ShardingStrategy {

        @Override
        public int shard(CharSequence val) {
            return shard(Utils.hash_murmur3(val));
        }

        @Override
        public int shard(byte[] val) {
            return Utils.hash_murmur3(val, 0, val.length);
        }

    }

    public static abstract class ShardingStrategyByteArrayAdapter implements
            ShardingStrategy {

        @Override
        public int shard(CharSequence val) {
            return shard(Strings2.toUTF8ByteArray(val));
        }

        @Override
        public int shard(int val) {
            byte[] bytes = new byte[4];
            bytes[0] = (byte) val;
            bytes[1] = (byte) (val >>> 8);
            bytes[2] = (byte) (val >>> 8);
            bytes[3] = (byte) (val >>> 8);

            return shard(bytes);
        }
    }

    /**
     * 
     * @deprecated uses .hashCode and Arrays.hashCode for bytes, better:
     *             {@link ShardingStrategyMurmur3Adapter}
     */
    @Deprecated
    public static abstract class ShardingStrategyIntAdapter implements
            ShardingStrategy {

        @Override
        public int shard(CharSequence val) {
            return shard(val.hashCode());
        }

        @Override
        public int shard(byte[] val) {
            return shard(Arrays.hashCode(val));
        }

    }

    /**
     * Uses sha hash of in encoded as hex for levels.
     * 
     * @param levels
     * @param in
     * @return
     */
    public static String[] shardToMultiLevelHexStrings(int levels, String in) {
        byte[] sha = DigestUtils.sha1(Strings2.toUTF8ByteArray(in));
        char[] hex = org.apache.commons.codec.binary.Hex.encodeHex(sha, true);
        String[] retval = new String[Math.max(0, levels)];
        for (int i = 0; i < levels; i++) {
            retval[i] = new String(hex, i * 2, 2);
        }
        return retval;
    }

    /**
     * Gets the longest multi level hex string path in the argument. Expects sth
     * like sthsth/aa/bb/DIR/FILE or sthsth/aa/bb/DIR , where DIR must not be a
     * length 2 hex string.
     * 
     * @param fileUriOrPath
     * @return relative or absolute urls - absolute if an absolute string is
     *         matched during extraction, e.g. /DIR for /DIR
     * @throws Exception
     */
    public static String extractMultiLevelHexStringStoragePath(
            String fileUriOrPath) throws Exception {
        String path = new URI(fileUriOrPath).getPath();
        Preconditions.checkArgument(!Strings2.isNullOrWhitespace(path),
                "fileUriOrPath has no path");
        File file = new File(path);
        List<String> elements = Lists.newArrayList();
        elements.add(file.getName());
        if (!isMultiLevelHexString(file.getParentFile().getName())) {

            file = file.getParentFile();
            elements.add(file.getName()); // files in dir <recId>
        }
        // search leveled:
        while (true) {
            file = file.getParentFile();
            if (file != null) {
                String name = file.getName();
                if (isMultiLevelHexString(name)) {
                    elements.add(name);
                    continue;
                }
            }
            break;
        }

        Collections.reverse(elements);
        return Joiner.on("/").join(elements);
    }

    public static boolean isMultiLevelHexString(String name) {
        if (name.length() == 2) {
            // hex?
            try {
                Integer.parseInt(name, 16);
                return true;
            } catch (NumberFormatException nfe) {
                // ign
            }
        }
        return false;
    }

}
