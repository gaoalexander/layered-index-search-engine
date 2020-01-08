package wse_project;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

class HeapKV implements Comparable<HeapKV> {
    private int key;
    private int value;
    HeapKV(int score, int docID) {
        this.key = score;
        this.value = docID; }
    public int getKey() { return this.key; }
    public int getValue() { return this.value; }
    public int compareTo(HeapKV other) { return this.getKey() - other.getKey(); }
}

class SearchNode_MultiTier {
    ArrayList<Integer> docs_per_term;
    ArrayList<ArrayList<Integer>> freqLists;
    ArrayList<ArrayList<Integer>> docIDLists;

    FileWriter queryLogOutStream;
    File queryLog;

    SearchNode_MultiTier() throws IOException {
        freqLists = new ArrayList<ArrayList<Integer>>();
        docIDLists = new ArrayList<ArrayList<Integer>>();
        docs_per_term = new ArrayList<Integer>();

        String queryLogPath = "data/3_log/query_log.txt";
        this.queryLog = new File(queryLogPath);
        this.queryLogOutStream = new FileWriter(queryLog, true);
    }

    public int intersection(Integer num_lists) {
        int num_intersect = 0;
        HashMap<Integer, Integer> counter = new  HashMap<Integer, Integer>();

        for (ArrayList<Integer> docIDList : docIDLists) {
            for (int i = 0; i < docIDList.size(); ++i) {
                if (counter.get(docIDList.get(i)) == null) counter.put(docIDList.get(i), 1);
                else counter.replace(docIDList.get(i), counter.get(docIDList.get(i)) + 1); } }

        for (Map.Entry each : counter.entrySet()) {
            if (each.getValue() == num_lists) ++num_intersect; }

        return num_intersect;
    }

    public void loadTier1(String[] query_terms, HashMap<String, Term> lexiconMapTier1, String invertedIndexPathTier1) {
        Integer offset, size;
        Term term_cur;

        for (String t : query_terms) {
            if (t.length() == 0) continue;
            termList list_cur = new termList();
            term_cur = lexiconMapTier1.get(t);
            if (term_cur != null) {
                offset = term_cur.getOffset();
                size = term_cur.getSize();
                list_cur.loadList(invertedIndexPathTier1, offset, size); }
            docIDLists.add(list_cur.docIDs);
            freqLists.add(list_cur.freqs);
            this.docs_per_term.add(list_cur.docIDs.size());
        }
    }

    public void fallThrough(String[] query_terms, HashMap<String, Term> lexiconMapTier2, String invertedIndexPathTier2) {
        Integer offset, size;
        Term term_cur;

        for (int i = 0; i < query_terms.length; ++i) {
            if (query_terms[i].length() == 0) continue;
            termList list_cur = new termList();
            term_cur = lexiconMapTier2.get(query_terms[i]);
            if (term_cur != null) {
                offset = term_cur.getOffset();
                size = term_cur.getSize();
                list_cur.loadList(invertedIndexPathTier2, offset, size);
            }
            docIDLists.get(i).addAll(list_cur.docIDs);
            freqLists.get(i).addAll(list_cur.freqs);
            this.docs_per_term.set(i, docs_per_term.get(i) + list_cur.docIDs.size());
        }
    }

    public ArrayList<Integer> thresholdAlgo(int k, String query, HashMap<String, Term> lexiconMapTier1, HashMap<String, Term> lexiconMapTier2, String invertedIndexPathTier1, String invertedIndexPathTier2) throws IOException {
        /*
        :param k:  target num results to be returned
        :param docs_per_term:
        :param termLists:  axis 0 := term-index, axis 1 := i-th element in term's list
        :return results:  list of top-k scoring results
        */

        String[] query_terms = query.split(" ");

        this.loadTier1(query_terms, lexiconMapTier1, invertedIndexPathTier1);

        int num_intersect_tier1 = this.intersection(query_terms.length);

        System.out.println("* " + num_intersect_tier1 + " documents in Tier 1 contain all query terms. *");

        if (num_intersect_tier1 <= k / 10 && num_intersect_tier1 > 0) {
            System.out.println("( FALLING THROUGH TO TIER 2. )");
            this.fallThrough(query_terms, lexiconMapTier2, invertedIndexPathTier2);
//            this.queryLogOutStream.write("2 " + query + "\n");
        } else {
            System.out.println("( EXECUTING QUERY IN TIER 1 ONLY. )");
//            this.queryLogOutStream.write("1 " + query + "\n");
        }

        int threshold = 0;
        int n_terms = query_terms.length;
        int min_list_size = Collections.min(docs_per_term);

        ArrayList<Integer> results = new ArrayList<Integer>();
        ArrayList<HashMap<Integer, Integer>> termMaps = new ArrayList<HashMap<Integer, Integer>>();
        HashMap<Integer, Integer> seen_value = new HashMap<Integer, Integer>();
        HashMap<Integer, Integer> seen_num = new HashMap<Integer, Integer>();

        PriorityQueue<HeapKV> minHeap = new PriorityQueue<HeapKV>();
        HeapKV kth = new HeapKV(0, -1);

        //Initialize Term HashMaps
        for (int i = 0; i < n_terms; ++i) {
            HashMap<Integer, Integer> termMap = new HashMap<Integer, Integer>();
            for (int j = 0; j < docIDLists.get(i).size(); ++j)
                termMap.put(docIDLists.get(i).get(j), freqLists.get(i).get(j));
            termMaps.add(termMap); }

        int i = 0;
        while (threshold >= kth.getKey() && i < min_list_size) {
            threshold = 0;
            for (int j = 0; j < n_terms; ++j) {
                int docID = docIDLists.get(j).get(i);
                int freq = freqLists.get(j).get(i);

                if (!seen_value.containsKey(docID)) {
                    for (int l = 0; l < n_terms; ++l) {
                        Integer freq_in_list = termMaps.get(l).get(docID);
                        if (freq_in_list != null) {
                            if (seen_value.get(docID) == null) {
                                seen_value.put(docID, freq_in_list);
                                seen_num.put(docID, 1);
                            } else {
                                seen_value.replace(docID, seen_value.get(docID) + freq_in_list);
                                seen_num.replace(docID, seen_num.get(docID) + 1);
                            }
                        }
                    }
                    int cur = seen_value.get(docID);

                    if (minHeap.size() < k)
                        minHeap.add(new HeapKV(cur, docID));
                    else if (minHeap.size() == k && cur > minHeap.peek().getKey()) {
                        minHeap.add(new HeapKV(cur, docID));
                        minHeap.poll(); }
                }
                threshold += freq;
            }
            kth = minHeap.peek();
            ++i;
        }

        int m = 0;
        while (m < k && minHeap.peek() != null) {
            results.add(minHeap.poll().getValue());
            ++m;
        }
        Collections.reverse(results);
//        if (results.size() == k) this.queryLogOutStream.write(query + "\n");      // FOR QUERY PREPROCESSING ONLY - DELIBERATELY COMMENTED OUT
        this.queryLogOutStream.close();
        return results;
    }

    public static void main(String[] args) throws IOException { return; }
}

class SearchNode_SingleTier {
    ArrayList<Integer> docs_per_term;
    ArrayList<ArrayList<Integer>> freqLists;
    ArrayList<ArrayList<Integer>> docIDLists;

    SearchNode_SingleTier() {
        this.freqLists = new ArrayList<ArrayList<Integer>>();
        this.docIDLists = new ArrayList<ArrayList<Integer>>();
        this.docs_per_term = new ArrayList<Integer>();
    }

    public void loadIndex(String[] query_terms, HashMap<String, Term> lexiconMapSingleTier, String invertedIndexPathSingleTier) {
        Integer offset, size;
        Term term_cur;

        for (String t : query_terms) {
            if (t.length() == 0) continue;
            termList list_cur = new termList();
            term_cur = lexiconMapSingleTier.get(t);
            if (term_cur != null) {
                offset = term_cur.getOffset();
                size = term_cur.getSize();
                list_cur.loadList(invertedIndexPathSingleTier, offset, size);
            }
            docIDLists.add(list_cur.docIDs);
            freqLists.add(list_cur.freqs);
            this.docs_per_term.add(list_cur.docIDs.size());
        }
    }

    public ArrayList<Integer> thresholdAlgo(int k, String query, HashMap<String, Term> lexiconMapSingleTier, String invertedIndexPathSingleTier) {
        /*
        :param k:  target num results to be returned
        :param docs_per_term:
        :param termLists:  axis 0 := term-index, axis 1 := i-th element in term's list
        :return results:  list of top-k scoring results
        */

        String[] query_terms = query.split(" ");

        this.loadIndex(query_terms, lexiconMapSingleTier, invertedIndexPathSingleTier);

        int threshold = 0;
        int n_terms = query_terms.length;
        int min_list_size = Collections.min(docs_per_term);

        ArrayList<Integer> results = new ArrayList<Integer>();
        ArrayList<HashMap<Integer, Integer>> termMaps = new ArrayList<HashMap<Integer, Integer>>();
        HashMap<Integer, Integer> seen_value = new HashMap<Integer, Integer>();
        HashMap<Integer, Integer> seen_num = new HashMap<Integer, Integer>();

        PriorityQueue<HeapKV> minHeap = new PriorityQueue<HeapKV>();
        HeapKV kth = new HeapKV(0, -1);

        //Initialize Term HashMaps
        for (int i = 0; i < n_terms; ++i) {
            HashMap<Integer, Integer> termMap = new HashMap<Integer, Integer>();
            for (int j = 0; j < docIDLists.get(i).size(); ++j)
                termMap.put(docIDLists.get(i).get(j), freqLists.get(i).get(j));
            termMaps.add(termMap); }

        int i = 0;
        while (threshold >= kth.getKey() && i < min_list_size) {
            threshold = 0;
            for (int j = 0; j < n_terms; ++j) {
                int docID = docIDLists.get(j).get(i);
                int freq = freqLists.get(j).get(i);

                if (!seen_value.containsKey(docID)) {
                    for (int l = 0; l < n_terms; ++l) {
                        Integer freq_in_list = termMaps.get(l).get(docID);
                        if (freq_in_list != null) {
                            if (seen_value.get(docID) == null) {
                                seen_value.put(docID, freq_in_list);
                                seen_num.put(docID, 1);
                            } else {
                                seen_value.replace(docID, seen_value.get(docID) + freq_in_list);
                                seen_num.replace(docID, seen_num.get(docID) + 1); } } }
                    int cur = seen_value.get(docID);
                    if (minHeap.size() < k)
                        minHeap.add(new HeapKV(cur, docID));
                    else if (minHeap.size() == k && cur > minHeap.peek().getKey()) {
                        minHeap.add(new HeapKV(cur, docID));
                        minHeap.poll(); } }
                threshold += freq; }
            kth = minHeap.peek();
            ++i; }
        int m = 0;
        while (m < k && minHeap.peek() != null) {
            results.add(minHeap.poll().getValue());
            ++m; }
        Collections.reverse(results);
        return results;
    }

    public static void main(String[] args) throws IOException { return; }
}