package de.deverado.framework.concurrent.consistenthashing;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FixedShardCountPrefixShardIdGenerator implements Iterable<String> {

    private String prefix;
    private int shardCount;

    public static FixedShardCountPrefixShardIdGenerator create(String prefix, int shardCount) {
        FixedShardCountPrefixShardIdGenerator result = new FixedShardCountPrefixShardIdGenerator();
        result.prefix = prefix;
        result.shardCount = shardCount;
        return result;
    }

    public List<String> asList() {
        List<String> result = new ArrayList<>(shardCount);
        Iterables.addAll(result, this);
        return result;
    }

    @Override
    public Iterator<String> iterator() {
        final String realPrefix = prefix + (prefix.endsWith("-") ? "" : "-");
        final StringBuilder builder = new StringBuilder(realPrefix.length() + 6);
        builder.append(realPrefix);

        return new Iterator<String>() {
            int next = 0;

            @Override
            public boolean hasNext() {
                return next < shardCount;
            }

            @Override
            public String next() {
                builder.setLength(realPrefix.length());
                builder.append(next);
                next++;
                return builder.toString();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
