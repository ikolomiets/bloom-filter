package com.electrit.bloomfilter;

import java.util.List;

public interface NameLookup {

    List<String> lookup(String prefix, int max);
    
}
