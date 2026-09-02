/*
 * Copyright © 2026 Lolaf.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lolaf.primos.witness;

import java.util.Locale;

/**
 * Checks on an application name that only the witness server needs.
 */
final class ApplicationNames {

    /**
     * Names shorter than this are not compared for near misses. Over three characters an edit
     * distance of one is a strong signal; at two it is noise, because half the short names in any
     * estate are one edit apart.
     */
    private static final int MIN_LENGTH_FOR_NEAR_MISS = 4;

    private ApplicationNames() {
    }

    /**
     * Whether a name can be a single directory name, which the state layout requires.
     *
     * <p>The wire permits any printable ASCII, separators included, and under
     * {@link ConfigSource#CLUSTER} the name arrives from the network rather than from a filename. Without this check a datagram naming
     * itself {@code ../../etc} would choose where its state was written.
     */
    static boolean isUsableAsDirectory(String name) {
        if (name == null || name.isEmpty() || ".".equals(name) || "..".equals(name)) {
            return false;
        }
        return name.indexOf('/') < 0 && name.indexOf('\\') < 0 && name.indexOf(':') < 0;
    }

    /**
     * The served name a new one is suspiciously close to, or null.
     *
     * <p>This is aimed at one specific disaster. Nodes configured {@code orders} and {@code ordres}
     * drop each other's datagrams for a mismatched name, so each sees the other as unreachable and
     * asks the witness instead. A permissive witness auto-creates <em>two</em> buckets and grants a
     * vote from each — two leaders out of one typo, with the cross-talk protection hiding the
     * cause. Nothing else in the system can see that, because from every other vantage point the
     * two clusters are simply unrelated.
     */
    static String nearMiss(String name, Iterable<String> served) {
        if (name.length() < MIN_LENGTH_FOR_NEAR_MISS) {
            return null;
        }
        String candidate = name.toLowerCase(Locale.ROOT);
        for (String existing : served) {
            if (existing.equals(name) || existing.length() < MIN_LENGTH_FOR_NEAR_MISS) {
                continue;
            }
            String other = existing.toLowerCase(Locale.ROOT);
            if (other.equals(candidate) || editDistanceAtMostTwo(candidate, other)) {
                return existing;
            }
        }
        return null;
    }

    /**
     * Levenshtein distance, answered only as "is it at most two".
     *
     * <p>Two rows rather than a full matrix, and an early exit once every cell in a row exceeds the
     * threshold — names are short, but this runs on the first sighting of a name from the network,
     * so it should not be something an attacker can make expensive by choosing long ones.
     */
    private static boolean editDistanceAtMostTwo(String a, String b) {
        if (Math.abs(a.length() - b.length()) > 2) {
            return false;
        }
        int[] previous = new int[b.length() + 1];
        int[] current = new int[b.length() + 1];
        for (int j = 0; j <= b.length(); j++) {
            previous[j] = j;
        }
        for (int i = 1; i <= a.length(); i++) {
            current[0] = i;
            int best = current[0];
            for (int j = 1; j <= b.length(); j++) {
                int cost = a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1;
                current[j] = Math.min(Math.min(current[j - 1] + 1, previous[j] + 1),
                        previous[j - 1] + cost);
                best = Math.min(best, current[j]);
            }
            if (best > 2) {
                return false;
            }
            int[] swap = previous;
            previous = current;
            current = swap;
        }
        return previous[b.length()] <= 2;
    }
}
