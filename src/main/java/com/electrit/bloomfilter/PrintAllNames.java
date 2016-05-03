package com.electrit.bloomfilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

public class PrintAllNames {

    private final static Logger logger = LoggerFactory.getLogger(PrintAllNames.class);
    private static final String FILTER_FILE_PATH = "bloom_filter.bin";
    private static BloomFilterNameLookup nameLookup;

    public PrintAllNames() throws IOException {
        logger.debug("Loading filter data from file...");
        nameLookup = new BloomFilterNameLookup(FILTER_FILE_PATH);
        logger.debug("Filter is ready.");
    }

    public int printAllNames(String prefix, PrintStream stream) {
        int words = 0;
        for (String result : nameLookup.lookup(prefix, 10)) {
            if (result.endsWith(".")) {
                words++;
                stream.println(result.substring(0, result.length() - 1));
            } else {
                words += printAllNames(result, stream);
            }
        }
        return words;
    }

    public static void main(String[] args) throws IOException {
        PrintAllNames printAllNames = new PrintAllNames();
        PrintStream printStream = new PrintStream(new File("bloom_names.txt"));
        long start = System.currentTimeMillis();
        for (char c1 = 'a'; c1 <= 'z'; c1++) {
            for (char c2 = 'a'; c2 <= 'z'; c2++) {
                int count = 0;
                for (char c3 = 'a'; c3 <= 'z'; c3++) {
                    for (char c4 = 'a'; c4 <= 'z'; c4++) {
                        count += printAllNames.printAllNames("" + c1 + c2 + c3 + c4, printStream);
                    }
                }
                logger.debug("{}, count={}", "" + c1 + c2, count);

            }
        }
        logger.debug("total time: {} sec.", (System.currentTimeMillis() - start) / 1000);
    }

}
