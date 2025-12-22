package com.collocations;
 
 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import java.io.FileInputStream; 
import java.io.InputStreamReader; 
import java.nio.charset.StandardCharsets; 
 
 /**
  * Stopwords loader and checker.
  */
public class Stopwords {
        final Set<String> en = new HashSet<>();
        final Set<String> he = new HashSet<>();

        void loadFromCache(Mapper.Context ctx) throws IOException {
            URI[] files = ctx.getCacheFiles();
            if (files == null) return;
            for (URI u : files) {
                String name = new Path(u.getPath()).getName().toLowerCase();
                Set<String> target;
                if (name.equals("eng-stopwords.txt")) target = en;
                else if (name.equals("heb-stopwords.txt")) target = he;
                else continue;
                String localName = (u.getFragment() != null) ? u.getFragment() : new Path(u.getPath()).getName();
                try (BufferedReader br = new BufferedReader(
        new InputStreamReader(new FileInputStream(localName), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        line = line.replace("\uFEFF", ""); 
                        line = line.trim().toLowerCase();
                        if (!line.isEmpty() && !line.startsWith("#")) target.add(line);
                    }
                }
            }
        }

        boolean isStop(String lang, String w) {
            if (w == null || w.isEmpty()) return true;
            return Constants.LANG_HE.equals(lang) ? he.contains(w) : en.contains(w);
        }
    }
