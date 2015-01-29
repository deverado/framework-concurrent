package de.deverado.framework.concurrent;/*
 * Copyright Georg Koester 2012-2015. All rights reserved.
 */

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;

import java.nio.ByteBuffer;

public class Hashing {

    public static int hash_djb2a(CharSequence s) {
        int hash = 5381;
        if (s == null) {
            return hash;
        }
        for (int i = 0; i < s.length(); i++) {

            hash = ((hash << 5) + hash) ^ (byte) s.charAt(i); /* hash * 33 ^ c */
        }
        return hash;

    }

    public static int hash_djb2a(byte[] s, int start, int len) {
        int hash = 5381;
        if (s == null) {
            return hash;
        }
        for (int i = 0; i < len; i++) {
            hash = ((hash << 5) + hash) ^ s[start + i]; /* hash * 33 ^ c */
        }
        return hash;
    }

    private static final HashFunction MURMUR3_32 = com.google.common.hash.Hashing.murmur3_32(5381);

    /**
     * Fast and very good distribution. Seed 5381 - constant. Hashes chars as
     * wide chars (two bytes hashed per char).
     *
     */
    public static int hash_murmur3(CharSequence s) {
        return MURMUR3_32.hashUnencodedChars(s).asInt();
    }

    /**
     * Fast and very good distribution. Seed 5381 - constant.
     *
     */
    public static int hash_murmur3(byte[] s, int start, int len) {
        return MURMUR3_32.hashBytes(s, start, len).asInt();
    }

    /**
     * XORs the two 32 bit halfs of an or to make an int.
     *
     */
    public static int foldLongToInt(long l) {
        long val = (l & 0xFFFFFFFF) ^ ((l >> 32) & 0xFFFFFFFF);
        return (int) (val & 0xFFFFFFFF);
    }

    /**
     * Uses murmur3 folded to 12 bits .
     *
     */
    public static int hashTo4096(byte[] in) {
        int val = hash_murmur3(in, 0, in.length);
        return hashTo4096(val);
    }

    /**
     * Uses murmur3 folded to 16 minus 2 dropped bits .
     *
     */
    public static int hashTo16384(byte[] in) {
        int val = hash_murmur3(in, 0, in.length);
        return hashTo16384(val);
    }

    /**
     * Uses murmur3 folded to 16 bits.
     *
     */
    public static int hashTo65536(byte[] in) {
        int val = hash_murmur3(in, 0, in.length);
        return hashTo65535(val);
    }

    // private static void half(byte[] digest, int l) {
    // for (int i = 0; i < l / 2; i++) {
    // digest[i] = (byte) (digest[i] ^ digest[l - 1 - i]);
    // }
    // }

    /**
     * Folds in to 16 bits. Should only be used if <code>in</code> is
     * distributed rather evenly.
     *
     */
    public static int hashTo65535(int in) {
        int val = (in >>> 16) ^ (in);
        return val & 65535;
    }

    /**
     * Folds in to 16 minus 2 dropped bits. Should only be used if
     * <code>in</code> is distributed rather evenly.
     *
     */
    public static int hashTo16384(int in) {
        int val = (in >>> 16) ^ (in);
        return val & 16383;
    }

    /**
     * Folds in to 16, preserves first 4 bits setting them to 0, folds to 8 and
     * adds preserved bits. Should only be used if <code>in</code> is
     * distributed rather evenly.
     *
     */
    public static int hashTo4096(int in) {
        int val = (in >>> 16) ^ (in);
        int firstFour = val & 15;
        // val without first four:
        val = val & (~15);
        val = ((val >>> 8) ^ val) & 255;

        // use saved first four as bits 9 - 12:
        val = val | (firstFour << 8);

        return val & 4095;
    }

    /**
     * Reduces an integer to a value between 0 and 512 excl. Tries to use higher
     * order bits, too. Little tricky because 32 bits don't fit easily into 9
     * bits.
     *
     */
    public static int hashTo512(int in) {
        int val = (in >>> 16) ^ (in);
        int firstBit = val & 1;
        // val without first bit:
        val = val & (~1);
        val = ((val >>> 8) ^ val) & 255;

        // old method:
        // (with normal String.hashCode this couldn't even always hold stddev of
        // 30.)
        // int folded = 0;
        // // choose one bit of the first half depending on the second half
        // // (lower bits usually, or at least with strings, more diverse)
        // folded = (in >> ((in >> 16) & 15)) & 1;
        // val = val | (folded << 8);

        // use saved first bit as the ninth:
        val = val | (firstBit << 8);

        return val & 511;

    }

    /**
     * bucket size 256 or 8 bits.
     *
     */
    public static int hashTo256(int in) {
        int val = (in >>> 16) ^ (in);
        val = ((val >>> 8) ^ val);
        return val & 255;
    }

    /**
     * bucket size 16 or 4 bits.
     *
     */
    public static int hashTo16(int in) {
        int val = (in >>> 16) ^ (in);
        val = ((val >>> 8) ^ val);
        val = ((val >>> 4) ^ val);
        return val & 15;
    }

    public static int extract32Bits(HashCode code) {
        if (code.bits() < 32) {
            throw new IllegalArgumentException("Not enough bits");
        } else if (code.bits() < 64) {
            return code.asInt();
        } else if (code.bits() < 128) {
            ByteBuffer wrapped = ByteBuffer.wrap(code.asBytes());
            return wrapped.getInt() ^ wrapped.getInt();
        } else {
            ByteBuffer wrapped = ByteBuffer.wrap(code.asBytes());
            return wrapped.getInt() ^ wrapped.getInt() ^ wrapped.getInt() ^ wrapped.getInt();
        }
    }

    public static long extract64Bits(HashCode code) {
        if (code.bits() < 64) {
            throw new IllegalArgumentException("Not enough bits");
        } else if (code.bits() < 128) {
            return code.asLong();
        } else {
            ByteBuffer wrapped = ByteBuffer.wrap(code.asBytes());
            return wrapped.getLong() ^ wrapped.getLong();
        }
    }
}
