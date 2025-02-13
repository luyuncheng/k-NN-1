/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.lucene.index;

public class KNNMergeHelper {

    private KNNMergeHelper() {}
    public static boolean isMergeAborted() {
        Thread mergeThread = Thread.currentThread();
        if (mergeThread instanceof ConcurrentMergeScheduler.MergeThread) {
            return ((ConcurrentMergeScheduler.MergeThread) mergeThread).merge.isAborted();
        }
        return false;
    }
}
