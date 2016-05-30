package com.electrit.bloomfilter;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

@SuppressWarnings("WeakerAccess")
public class BloomFilterNameLookup implements NameLookup {

    private static final int MIN_PREFIX_LENGTH = 4;

    private final BloomFilter<CharSequence> bloomFilter;

    public BloomFilterNameLookup(String filterFilePath) throws IOException {
        bloomFilter = BloomFilter.readFrom(new FileInputStream(filterFilePath), Funnels.stringFunnel(Charsets.UTF_8));
    }

    public BloomFilterNameLookup(int expectedInsertions, double fpp) {
        bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), expectedInsertions, fpp);
    }

    public void save(String filterFilePath) throws IOException {
        OutputStream out = new FileOutputStream(filterFilePath);
        bloomFilter.writeTo(out);
        out.close();
    }

    public int add(String name) {
        int addedPrefixes = 0;
        name = name + '.'; // '.' is to mark a whole word
        for (int i = 0; i < name.length() - MIN_PREFIX_LENGTH + 1; i++)
            if (bloomFilter.put(name.substring(0, MIN_PREFIX_LENGTH + i)))
                addedPrefixes++;

        return addedPrefixes;
    }

    @Override
    public List<String> lookup(String prefix, int max) {
        if (!bloomFilter.mightContain(prefix))
            return Collections.emptyList();

        if (max <= 0)
            return Collections.singletonList(prefix);

        List<String> result = new ArrayList<>();
        String word = prefix + '.';
        if (bloomFilter.mightContain(word))
            result.add(word);

        Map<String, List<String>> nextPrefixResults = new HashMap<>();
        for (char c = 'a'; c <= 'z'; c++) {
            String nextPrefix = prefix + c;
            List<String> nextPrefixResult = lookup(nextPrefix, max - 1);
            if (!nextPrefixResult.isEmpty())
                nextPrefixResults.put(nextPrefix, nextPrefixResult);
        }

        int nominalSize = Math.max(result.size() + nextPrefixResults.size(), max);

        while (!nextPrefixResults.isEmpty()) {
            Map.Entry<String, List<String>> minNextPrefixEntry = null;
            for (Map.Entry<String, List<String>> entry : nextPrefixResults.entrySet())
                if (minNextPrefixEntry == null || entry.getValue().size() < minNextPrefixEntry.getValue().size())
                    minNextPrefixEntry = entry;

            if (minNextPrefixEntry != null) {
                if (result.size() + nextPrefixResults.size() - 1 + minNextPrefixEntry.getValue().size() <= nominalSize) {
                    result.addAll(minNextPrefixEntry.getValue());
                    nextPrefixResults.remove(minNextPrefixEntry.getKey());
                } else {
                    result.addAll(nextPrefixResults.keySet());
                    break;
                }
            }
        }

        return result;
    }

}
