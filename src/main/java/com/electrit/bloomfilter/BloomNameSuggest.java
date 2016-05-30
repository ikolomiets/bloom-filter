package com.electrit.bloomfilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;

public class BloomNameSuggest {

    private final static Logger logger = LoggerFactory.getLogger(BloomNameSuggest.class);
    private static final String FILTER_FILE_PATH = "bloom_filter.bin";

    private static BloomFilterNameLookup createBloomFilterNameLookup() throws IOException {
        BloomFilterNameLookup nameLookup;
        boolean updateFilterFile = false;
        //noinspection ConstantConditions
        if (updateFilterFile) {
            //PrintStream printStream = new PrintStream(new File("longer_lastnames.txt"));

            nameLookup = new BloomFilterNameLookup(20000000, 0.01);
            BufferedReader lnbr = new BufferedReader(new FileReader("lastnames.txt"));
            String name;
            int counter = 0;
            int prefixes = 0;
            while ((name = lnbr.readLine()) != null) {

                if (name.length() > 3) {
                    //printStream.println(name);
                    
                    prefixes += nameLookup.add(name);
                    if (++counter % 100000 == 0) {
                        logger.debug("Added {} names", counter);
                    }
                }

            }
            lnbr.close();

            nameLookup.save(FILTER_FILE_PATH);

            logger.debug("Read {} names", counter);
            logger.debug("Generated {} prefixes", prefixes);
        } else {
            logger.debug("Loading filter data from file...");
            nameLookup = new BloomFilterNameLookup("bloom_filter.bin");
            logger.debug("Filter is ready.");
        }
        return nameLookup;
    }

    public static void main(String[] args) throws IOException {
        NameLookup nameLookup = createBloomFilterNameLookup();

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        String line;
        while ((line = br.readLine()) != null) {
            if (line.length() > 3) {
                long start = System.currentTimeMillis();
                List<String> suggestions = nameLookup.lookup(line, 10);
                long elapsed = System.currentTimeMillis() - start;
                logger.debug("Found {} suggestions in {}ms", suggestions.size(), elapsed);
                for (String suggestion : suggestions) {
                    System.out.println("> " + suggestion);
                }
            } else {
                System.out.println("> " + line + " is too short");
            }
        }
    }

}
