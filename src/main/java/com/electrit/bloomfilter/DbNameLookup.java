package com.electrit.bloomfilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DbNameLookup implements NameLookup {

    private final static Logger logger = LoggerFactory.getLogger(DbNameLookup.class);

    private final PreparedStatement psSelect;

    public DbNameLookup(Connection connection) throws SQLException {
        psSelect = connection.prepareStatement("SELECT NAME FROM LASTNAMES WHERE NAME LIKE ? ORDER BY NAME",
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
    }

    private final static class PrefixWords {
        private final String prefix;
        private final List<PrefixWords> children = new ArrayList<>();

        public PrefixWords(String prefix) {
            this.prefix = prefix;
        }

        public int size() {
            int size = 1;
            for (PrefixWords child : children)
                size += child.size();
            return size;
        }

        public int words() {
            int words = prefix.endsWith(".") ? 1 : 0;
            for (PrefixWords child : children)
                words += child.words();
            return words;
        }

        public List<String> getWordsAndPrefixes(int max) {
            List<String> result = new ArrayList<>();
            if (prefix.endsWith(".")) {
                result.add(prefix);
            }

            Iterator<PrefixWords> iterator = children.iterator();
            while (iterator.hasNext()) {
                PrefixWords next = iterator.next();
                if (next.prefix.endsWith(".") && next.children.isEmpty()) {
                    result.add(next.prefix);
                    iterator.remove();
                }
            }

            while (result.size() + children.size() < max && !children.isEmpty()) {
                PrefixWords minChild = null;
                List<String> minChildWordsAndPrefixes = null;
                for (PrefixWords child : children) {
                    List<String> childWordsAndPrefixes = child.getWordsAndPrefixes(max - result.size() - children.size());
                    if (minChild == null || childWordsAndPrefixes.size() < minChildWordsAndPrefixes.size()) {
                        minChild = child;
                        minChildWordsAndPrefixes = childWordsAndPrefixes;
                    }
                }

                if (minChildWordsAndPrefixes != null && (result.size() + minChildWordsAndPrefixes.size() + children.size() - 1) <= max) {
                    children.remove(minChild);
                    result.addAll(minChildWordsAndPrefixes);
                }
            }

            for (PrefixWords child : children)
                if (child.prefix.endsWith("."))
                    result.add(child.prefix.substring(0, child.prefix.length() - 1));
                else
                    result.add(child.prefix);

            return result;
        }

        private PrefixWords pullOnlyChildUp() {
            if (!prefix.endsWith(".") && children.size() == 1) {
                PrefixWords onlyChild = children.get(0);
                PrefixWords next = onlyChild.pullOnlyChildUp();
                return next != null ? next : onlyChild;
            }
            return null;
        }

        public void compact() {
            for (int i = 0, childrenSize = children.size(); i < childrenSize; i++) {
                PrefixWords child = children.get(i);
                child.compact();
                PrefixWords onlyChild = child.pullOnlyChildUp();
                if (onlyChild != null) {
                    children.set(i, onlyChild);
                }
            }
        }

        @Override
        public String toString() {
            return toString("");
        }

        private String toString(String indent) {
            StringBuilder sb = new StringBuilder(indent).append(prefix);
            String nextIndent = indent + ".";
            for (PrefixWords child : children) {
                sb.append("\n").append(indent).append(child.toString(nextIndent));
            }
            return sb.toString();
        }
    }

    @Override
    public List<String> lookup(String prefix, int max) {
        PrefixWords prefixWords;
        try {
            psSelect.setString(1, prefix + "%");
            prefixWords = groupByAbc(psSelect.executeQuery(), prefix);
        } catch (SQLException e) {
            throw new RuntimeException("Unable to execute db query for " + prefix, e);
        }

        if (prefixWords == null)
            return new ArrayList<>();

        prefixWords.compact();
        logger.debug("prefixWords size={}, words={}:\n{}", prefixWords.size(), prefixWords.words(), prefixWords);

        return prefixWords.getWordsAndPrefixes(max);
    }

    private static PrefixWords groupByAbc(ResultSet resultSet, String prefix) throws SQLException {
        if (!resultSet.next())
            return null;

        String name = resultSet.getString(1);
        PrefixWords result;
        if (name.equals(prefix)) {
            result = new PrefixWords(prefix + ".");
            name = null;
        } else {
            result = new PrefixWords(prefix);
        }

        for (char extension = 'a'; extension <= 'z'; extension++) {
            if (name == null) {
                if (!resultSet.next())
                    break;
                name = resultSet.getString(1);
            }

            if (name.startsWith(prefix)) {
                String extendedPrefix = prefix + extension;
                if (name.startsWith(extendedPrefix)) {
                    resultSet.previous();
                    PrefixWords extPrefixWords = groupByAbc(resultSet, extendedPrefix);
                    if (extPrefixWords != null)
                        result.children.add(extPrefixWords);
                    name = null;
                }
            } else {
                resultSet.previous();
                break;
            }
        }

        return result;
    }
    
}
