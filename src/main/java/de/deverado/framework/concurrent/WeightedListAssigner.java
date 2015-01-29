package de.deverado.framework.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class WeightedListAssigner {

    public static class AssignmentFailedException extends Exception {

        private static final long serialVersionUID = 4136862168972729239L;

        public AssignmentFailedException() {
        }

        public AssignmentFailedException(String message) {
            super(message);
        }

        public AssignmentFailedException(Throwable cause) {
            super(cause);
        }

        public AssignmentFailedException(String message, Throwable cause) {
            super(message, cause);
        }

    }

    public interface WeightedListFactory {
        boolean validate(String val);

        /**
         * Should return quickly and schedule background work in different
         * thread as this is called with the client worker thread.
         * 
         * @param val
         */
        void reportDeadNode(String val);

        Map<String, Long> getRawWeightedList() throws Exception;

        void reportAssignmentDifficult(int replicationFactory, int numberTries);

    }

    private static final Logger log = LoggerFactory
            .getLogger(WeightedListAssigner.class);

    private Random random = new Random();
    private final LoadingCache<String, TreeMap<Long, String>> weightedListCache;
    private final Cache<String, Boolean> deadNodeCache;
    private final Lock deathReportingLock = new ReentrantLock();
    private final WeightedListFactory factory;

    public WeightedListAssigner(WeightedListFactory factory) {
        Preconditions.checkArgument(factory != null);
        this.factory = factory;
        weightedListCache = CacheBuilder.newBuilder().initialCapacity(1)
                .concurrencyLevel(1).expireAfterAccess(30, TimeUnit.SECONDS)
                .build(new CacheLoader<String, TreeMap<Long, String>>() {
                    @Override
                    public TreeMap<Long, String> load(String key)
                            throws Exception {
                        // key ignored in this application
                        return createArrangedWeightedList();
                    }
                });

        deadNodeCache = CacheBuilder.newBuilder()
                .expireAfterWrite(20, TimeUnit.SECONDS).build();
    }

    protected TreeMap<Long, String> createArrangedWeightedList()
            throws Exception {
        Map<String, Long> freeList = factory.getRawWeightedList();

        long sum = 0;
        for (Map.Entry<String, Long> entry : freeList.entrySet()) {
            sum += entry.getValue().longValue();
        }
        log.debug("Got weighted list, weightsumgb={}, weightsum={}",
                sum / 1024.0 / 1024 / 1024, sum);
        TreeMap<Long, String> vals = new TreeMap<Long, String>();
        long sum2 = 0;
        for (Map.Entry<String, Long> entry : freeList.entrySet()) {
            long partOfCurrent = entry.getValue().longValue();
            vals.put(sum2 + partOfCurrent, entry.getKey());
            sum2 += partOfCurrent;
        }
        return vals;
    }

    /**
     * Leads to recreation of weighted list from Factory on next access.
     */
    public void refresh() {
        weightedListCache.invalidateAll();
    }

    public List<String> assign(int replication, boolean validateAssignment)
            throws Exception {
        ArrayList<String> retval = new ArrayList<String>(3);
        int tries = 0;
        while (retval.size() < replication && tries++ < 200) {
            String anAssignment = assign(validateAssignment);
            if (!retval.contains(anAssignment)) {
                retval.add(anAssignment);
            }
        }
        if (tries > 50) {
            log.debug("Assignment difficult "
                    + "for replicationFactor={}, neededTries={} ", replication,
                    tries);
            factory.reportAssignmentDifficult(replication, tries);
        }
        if (retval.size() < replication) {
            if (retval.size() > 0) {
                throw new AssignmentFailedException(
                        "Replication factor request cannot be satisfied");
            } else {
                throw new AssignmentFailedException(
                        "Not assignable to any slot");
            }
        }
        return retval;
    }

    public String assign(boolean validateAssignment) throws Exception {
        TreeMap<Long, String> freeList = weightedListCache.get("IGN_KEY");
        if (freeList == null || freeList.size() < 1) {
            throw new AssignmentFailedException("No free slots");
        }

        int tries = 0;
        Entry<Long, String> entry;
        do {
            // only dice a couple of times to honor the weighted list
            // assumption. of course if a big node in middle of small
            // nodes fails, it's possible that the small nodes don't get
            // selected and the assignment fails although there would
            // still be space. But that is ok - it leads the system to
            // fail partly instead of slamming the smaller nodes right away
            long diced = (long) (random.nextDouble() * freeList.lastKey()
                    .longValue());
            entry = freeList.ceilingEntry(diced);

            if (!potentiallyValidation(validateAssignment, entry)) {
                entry = null;
            }

        } while (entry == null && tries++ < 10);
        if (entry == null) {
            throw new AssignmentFailedException(
                    "Couldn't find a (validated) assignable slot");
        }
        return entry.getValue();
    }

    protected boolean potentiallyValidation(boolean validateAssignment,
            Entry<Long, String> entry) throws Exception {
        if (validateAssignment) {
            boolean retval = false;

            if (isDeadNodeCached(entry.getValue())) {
                retval = false;
            } else {
                boolean validated = false;
                try {
                    validated = factory.validate(entry.getValue());
                } catch (Exception e) {
                    log.debug("Validation of {} failed: {}", entry.getValue(),
                            e.toString());
                }

                if (validated) {
                    retval = true;
                } else {
                    retval = false;

                    cacheDeathAndPotentiallyReport(entry);
                }
            }
            return retval;
        } else {
            return true;
        }
    }

    protected void cacheDeathAndPotentiallyReport(Entry<Long, String> entry)
            throws InterruptedException {
        boolean needsReporting = false;
        deathReportingLock.lockInterruptibly();
        try {
            if (isDeadNodeCached(entry.getValue())) {
                // already reported
            } else {
                deadNodeCache.put(entry.getValue(), true);
                needsReporting = true;
            }
        } finally {
            deathReportingLock.unlock();
        }

        if (needsReporting) {
            try {
                factory.reportDeadNode(entry.getValue());
            } catch (Exception e) {
                log.warn("Exception while reporting dead node: {}",
                        entry.getValue(), e);
            }
        }
    }

    private boolean isDeadNodeCached(String nodeId) {
        return null != deadNodeCache.getIfPresent(nodeId);
    }
}
