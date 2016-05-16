package com.electrit.bloomfilter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class DbNameLookup implements NameLookup {

    private final PreparedStatement psSelect;

    public DbNameLookup(Connection connection) throws SQLException {
        psSelect = connection.prepareStatement("SELECT NAME FROM LASTNAMES WHERE NAME LIKE ?");
    }

    @Override
    public List<String> lookup(String prefix, int max) {
        List<String> results = new ArrayList<>();
        Map<String, List<String>> prefixWords = new HashMap<>();

        try {
            psSelect.setString(1, prefix + "%");
            ResultSet resultSet = psSelect.executeQuery();

            char prefixExtension = 'a';
            List<String> extendedPrefixWords = null;
            while (resultSet.next()) {
                String name = resultSet.getString(1);
                if (name.equals(prefix)) {
                    results.add(name + ".");
                } else {
                    String extendedPrefix = prefix + prefixExtension;
                    while (prefixExtension <= 'z' && !name.startsWith(extendedPrefix)) {
                        extendedPrefix = prefix + ++prefixExtension;
                        extendedPrefixWords = null;
                    }

                    if (extendedPrefixWords == null) {
                        extendedPrefixWords = new ArrayList<>();
                        prefixWords.put(extendedPrefix, extendedPrefixWords);
                    }
                    
                    extendedPrefixWords.add(name + ".");
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException("Unable to execute db query for " + prefix, e);
        }

        Iterator<Map.Entry<String, List<String>>> prefixWordsIterator = prefixWords.entrySet().iterator();
        while (prefixWordsIterator.hasNext()) {
            Map.Entry<String, List<String>> next = prefixWordsIterator.next();
            if (next.getValue().size() == 1) {
                results.add(next.getValue().get(0));
                prefixWordsIterator.remove();
            }
        }

        // todo optimize prefixWords
        
        int count = 0;
        for (Map.Entry<String, List<String>> entry : prefixWords.entrySet()) {
            System.out.println("XXX " + entry);
            count += entry.getValue().size();
        }
        System.out.println("total=" + count);

        return results;
    }

}
