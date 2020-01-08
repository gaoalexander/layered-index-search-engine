package wse_project;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

// stores the postings that are generated from the terms in the queries
class Term {
    private Integer offset;
    private Integer size;
    private Integer count;

    Term(Integer offset, Integer size, Integer count) {
        this.offset = offset;
        this.size = size;
        this.count = count;
    }
    Integer getOffset() {
        return offset;
    }
    Integer getSize() {
        return size;
    }
    Integer getCount() { return count; }
}

// Stores all the documentIds and frequencies for the specific word in the query
class termList {
    ArrayList<Integer> docIDs;
    ArrayList<Integer> freqs;
    private Integer index; // current pointer to the list

    termList() {
        docIDs = new ArrayList();
        freqs = new ArrayList();
        index = -1;
    }

    public String[] decodeVarByteCompression(String varByteEncoding) {
        String[] decodings = new String[varByteEncoding.length() / 8];
        int j = 0;
        for (int i = 0; i < decodings.length; i++) {
            decodings[i] = varByteEncoding.substring(j, j + 8);
            j += 8;
        }
        return decodings;
    }

    public Integer nextGEQ(Integer k) {
        if (index > -1 && k != null) {
            for (int i = index; i < docIDs.size(); i++) {
                if (docIDs.get(i) >= k)
                    return docIDs.get(i);
            }
        }
        return null;
    }

    public Integer getDocIDsSize() {
        return docIDs.size();
    }

    public Integer getFreq() {
        return (index > -1) ? freqs.get(index) : null;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    // decodes the varbyte compression and populates the docIDs and frequencies list.
    public void loadList(String fileName, Integer offset, Integer size) {
        try {
            RandomAccessFile invertedIndexFile = new RandomAccessFile(fileName, "r");
            invertedIndexFile.seek(offset);
            byte[] byteArray = new byte[size];
            invertedIndexFile.read(byteArray);
            String varByteEncoding = new String(byteArray);
            String[] bytes = decodeVarByteCompression(varByteEncoding);
            StringBuffer sb = new StringBuffer();
            ArrayList<Integer> varByteDecodingList = new ArrayList();

            for (String byteString : bytes) {
                sb.append(byteString.substring(1));
                if (byteString.charAt(0) == '1') {
                    varByteDecodingList.add(Integer.parseInt(sb.toString(), 2));
                    sb = new StringBuffer();
                }
            }
            int i = 0;
            for (Integer num : varByteDecodingList) {
                if (i % 2 == 0) freqs.add(num);
                else docIDs.add(num);
                ++i;
            }
            if (docIDs.size() > 0) index++;
            invertedIndexFile.close();
        } catch (IOException e) {
            System.out.println("Unable to read content from file");
        }
    }
}

// output format of the final result that should be displayed to the user.
class SearchResult implements Comparable<SearchResult> {
    private String url;
    private Integer documentId;
    private Double score;
    private ArrayList<Integer> wordsFrequenciesList;

    SearchResult(String url, Integer documentId, Double score) {
        this.url = url;
        this.documentId = documentId;
        this.score = score;
        wordsFrequenciesList = new ArrayList();
    }

    public String getUrl() {
        return url;
    }

    public Double getScore() {
        return score;
    }

    public Integer getDocumentId() {
        return documentId;
    }

    public ArrayList<Integer> getWordsFrequenciesList() {
        return wordsFrequenciesList;
    }

    public void setWordsFrequenciesList(ArrayList<Integer> wordsFrequenciesList) {
        this.wordsFrequenciesList = wordsFrequenciesList;
    }

    @Override
    public int compareTo(SearchResult sr) {
        if (this.score > sr.score) return 1;
        else if (this.score < sr.score) return -1;
        else return 0;
    }
}

// used to store the url elements from url_doc_mapping.gz file
class URLMapping {
    private String url;
    private Integer totalTermsCount;

    URLMapping(String url, Integer totalTermsCount) {
        this.url = url;
        this.totalTermsCount = totalTermsCount;
    }

    public String getUrl() { return url; }

    public Integer getTotalTermsCount() { return totalTermsCount; }
}

class Query_MultiTier {
    private HashMap<String, Term> lexiconMapTier1;
    private HashMap<String, Term> lexiconMapTier2;
    private HashMap<Integer, URLMapping> docIDToUrlMap;
    private Integer totalResults, totalDocumentsTerms;
    private String invertedIndexPathTier1, invertedIndexPathTier2;

    Query_MultiTier(Integer totalResults, String invertedIndexPathTier1, String invertedIndexPathTier2) {
        this.lexiconMapTier1 = new HashMap();
        this.lexiconMapTier2 = new HashMap();
        this.docIDToUrlMap = new HashMap();
        this.totalResults = totalResults;
        this.invertedIndexPathTier1 = invertedIndexPathTier1;
        this.invertedIndexPathTier2 = invertedIndexPathTier2;
        this.totalDocumentsTerms = 0;
    }

    public HashMap<String, Term> getLexiconMapTier1(){ return lexiconMapTier1; }

    public HashMap<String, Term> getLexiconMapTier2(){ return lexiconMapTier2; }

    public void buildLexicon(String fileName, HashMap<String, Term> tier) {
        try {
            GZIPInputStream lexiconFile = new GZIPInputStream(new FileInputStream(fileName));
            BufferedReader br = new BufferedReader(new InputStreamReader(lexiconFile));
            String currentTerm = null;
            while ((currentTerm = br.readLine()) != null) {
                String[] lexiconValues = currentTerm.split(" ");
                if (lexiconValues.length == 4) {
                    String term = lexiconValues[0];
                    Integer offset = Integer.parseInt(lexiconValues[1]) - 1;
                    Integer size = Integer.parseInt(lexiconValues[2]);
                    Integer count = Integer.parseInt(lexiconValues[3]);
                    tier.put(term, new Term(offset, size, count));
                }
            }
            lexiconFile.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Unable to read content from file");
        }
    }

    public void buildDocIDsToUrlMapping(String fileName) {
        try {
            GZIPInputStream lexiconFile = new GZIPInputStream(new FileInputStream(fileName));
            BufferedReader br = new BufferedReader(new InputStreamReader(lexiconFile));
            String currentTerm = null;
            while ((currentTerm = br.readLine()) != null) {
                String[] docIDsToUrlMappingValues = currentTerm.split(" ");
                if (docIDsToUrlMappingValues.length == 3) {
                    Integer docId = Integer.parseInt(docIDsToUrlMappingValues[0]);
                    String url = docIDsToUrlMappingValues[1];
                    Integer totalTermsCount = Integer.parseInt(docIDsToUrlMappingValues[2]);
                    try {
                        docIDToUrlMap.put(docId, new URLMapping(url, totalTermsCount));
                    } catch (Exception e) {
                        System.out.println("Exception caught" + e);
                    }
                }
            }
            lexiconFile.close();
        } catch (IOException e) { System.out.println("Unable to read content from file"); }
    }

    public ArrayList<Integer> getSearchResults_MultiTier(String query, Integer k) throws IOException {
        ArrayList<ArrayList<Integer>> docIDLists = new ArrayList();
        ArrayList<ArrayList<Integer>> freqLists = new ArrayList();
        SearchNode_MultiTier searchnode_multi = new SearchNode_MultiTier();

        return searchnode_multi.thresholdAlgo(k, query, this.lexiconMapTier1, this.lexiconMapTier2, this.invertedIndexPathTier1, this.invertedIndexPathTier2);
    }

    public static void main(String[] args) throws IOException { return; }
}

class Query_SingleTier {
    private HashMap<String, Term> lexiconMapSingleTier;
    private HashMap<Integer, URLMapping> docIDToUrlMap;
    private Integer totalResults, totalDocumentsTerms;
    private String invertedIndexPathSingleTier;

    Query_SingleTier(Integer totalResults, String invertedIndexPathSingleTier) {
        this.lexiconMapSingleTier = new HashMap();
        this.docIDToUrlMap = new HashMap();
        this.totalResults = totalResults;
        this.invertedIndexPathSingleTier = invertedIndexPathSingleTier;
        this.totalDocumentsTerms = 0;
    }
    public HashMap<String, Term> getLexiconMapSingleTier(){
        return lexiconMapSingleTier;
    }

    public void buildLexicon(String fileName, HashMap<String, Term> lexicon) {
        try {
            GZIPInputStream lexiconFile = new GZIPInputStream(new FileInputStream(fileName));
            BufferedReader br = new BufferedReader(new InputStreamReader(lexiconFile));
            String currentTerm = null;
            while ((currentTerm = br.readLine()) != null) {
                String[] lexiconValues = currentTerm.split(" ");
                if (lexiconValues.length == 4) {
                    String term = lexiconValues[0];
                    Integer offset = Integer.parseInt(lexiconValues[1]) - 1;
                    Integer size = Integer.parseInt(lexiconValues[2]);
                    Integer count = Integer.parseInt(lexiconValues[3]);
                    lexicon.put(term, new Term(offset, size, count));
                }
            }
            lexiconFile.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Unable to read content from file");
        }
    }

    public void buildDocIDsToUrlMapping(String fileName) {
        try {
            GZIPInputStream lexiconFile = new GZIPInputStream(new FileInputStream(fileName));
            BufferedReader br = new BufferedReader(new InputStreamReader(lexiconFile));
            String currentTerm = null;
            while ((currentTerm = br.readLine()) != null) {
                String[] docIDsToUrlMappingValues = currentTerm.split(" ");
                if (docIDsToUrlMappingValues.length == 3) {
                    Integer docId = Integer.parseInt(docIDsToUrlMappingValues[0]);
                    String url = docIDsToUrlMappingValues[1];
                    Integer totalTermsCount = Integer.parseInt(docIDsToUrlMappingValues[2]);
                    try {
                        docIDToUrlMap.put(docId, new URLMapping(url, totalTermsCount));
                    } catch (Exception e) {
                        System.out.println("Exception caught" + e);
                    }
                }
            }
            lexiconFile.close();
        } catch (IOException e) {
            System.out.println("Unable to read content from file");
        }

    }

    public ArrayList<Integer> getSearchResults_SingleTier(String query, Integer k) {
        ArrayList<ArrayList<Integer>> docIDLists = new ArrayList();
        ArrayList<ArrayList<Integer>> freqLists = new ArrayList();

        SearchNode_SingleTier searchnode_single = new SearchNode_SingleTier();
        return searchnode_single.thresholdAlgo(k, query, lexiconMapSingleTier, invertedIndexPathSingleTier);
    }

    public static void main(String[] args) throws IOException { return; }
}

class MainQueryProgram {

    MainQueryProgram() { }

    public float average_precision(ArrayList<Integer> singleTierResults, ArrayList<Integer> multiTierResults) {
        float ap = 0.0f;
        int divisor = 0, num_true_pos = 0;
        HashSet<Integer> set = new HashSet<Integer>();
        for (Integer i : singleTierResults) { set.add(i); }

        for (int i = 0; i < multiTierResults.size(); ++i) {
            if (set.contains(multiTierResults.get(i))) {
                ++num_true_pos;
                ++divisor;
                ap += (float)num_true_pos/(i + 1);}}
        ap /= divisor;
        ap *= ((float)multiTierResults.size() / (float)singleTierResults.size());
        return Float.isNaN(ap) ? 0.5f : ap;
    }

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("\n------TIER 1 SPLIT PERCENTAGE------");
        System.out.println("[10] - more computational cost saving, lower MAP");
        System.out.println("[20] - slightly less computational cost saving, greater MAP");
        System.out.print("Enter [10] to use 10% Tier 1 split, or [20] to use 20% Tier 1 split: ");

        String tier_1_split_str = br.readLine();

        // DECLARE PATHS
        String dataIndexDir = "data/2_index/";
        String urlDocMappingPath = dataIndexDir + "url_doc_mapping.gz";
//        String queriesPath = "data/1_intermediate/queries/queries.gz";
        String queriesPath = "data/1_intermediate/queries/queries_3terms.txt";

//        String queriesPath = "data/3_log/dnu_50_3terms_2/tier1_queries.txt";
        String lexiconTier1Path = dataIndexDir + "lexiconTier1_" + tier_1_split_str + ".gz";
        String lexiconTier2Path = dataIndexDir + "lexiconTier2_" + tier_1_split_str + ".gz";
        String indexTier1Path = dataIndexDir + "invertedIndexTier1_" + tier_1_split_str;
        String indexTier2Path = dataIndexDir + "invertedIndexTier2_" + tier_1_split_str;
        String lexiconSingleTierPath = dataIndexDir + "lexiconSingleTier.gz";
        String indexSingleTierPath = dataIndexDir + "invertedIndexSingleTier";

        MainQueryProgram query = new MainQueryProgram();

        System.out.print("Enter target # of results to be returned: ");
        String records = br.readLine();
        System.out.print("Enter [1] to execute batch query, or [0] to manually enter queries: ");
        String batchQueryFlag = br.readLine();

        // INITIALIZE QUERY NODES
        Query_MultiTier query_multi = new Query_MultiTier(Integer.parseInt(records), indexTier1Path, indexTier2Path);
        query_multi.buildLexicon(lexiconTier1Path, query_multi.getLexiconMapTier1());
        query_multi.buildLexicon(lexiconTier2Path, query_multi.getLexiconMapTier2());
        query_multi.buildDocIDsToUrlMapping(urlDocMappingPath);

        Query_SingleTier query_single = new Query_SingleTier(Integer.parseInt(records), indexSingleTierPath);
        query_single.buildLexicon(lexiconSingleTierPath, query_single.getLexiconMapSingleTier());
        query_single.buildDocIDsToUrlMapping(urlDocMappingPath);

//        GZIPInputStream queries = new GZIPInputStream(new FileInputStream(queriesPath));
        FileInputStream queries = new FileInputStream(queriesPath);
        BufferedReader queries_br = new BufferedReader(new InputStreamReader(queries));

        long startTime;
        float mean_average_precision = 0.0f;
        float multitier_time = 0.0f;
        float singletier_time = 0.0f;
        String current_query = null;

        if (batchQueryFlag.equals("1")) {
            // BATCH EXECUTE QUERIES, MEASURE M.A.P.
            // COMPARE QUERY TIME FOR SINGLE VS. MULTI-TIER
            System.out.print("Enter # of queries to execute in batch (up to 60000): ");
            String num_queries = br.readLine();

            int i = 0;
            while ((current_query = queries_br.readLine()) != null && i < Integer.parseInt(num_queries)) {
                if (i % 10 == 0) System.out.println("# of queries executed: " + i + "\n");
                System.out.println("QUERY: " + current_query);
                startTime = System.currentTimeMillis();
                ArrayList<Integer> results_multi = query_multi.getSearchResults_MultiTier(current_query, Integer.parseInt(records));
                multitier_time += ((System.currentTimeMillis() - startTime) / 1000.0);
                System.out.println("Mutli-Tier Query Time = " + (System.currentTimeMillis() - startTime) / 1000.0 + " s");
                System.out.println("\t| # Results Returned: " + results_multi.size());

                startTime = System.currentTimeMillis();
                ArrayList<Integer> results_single = query_single.getSearchResults_SingleTier(current_query, Integer.parseInt(records));
                singletier_time += ((System.currentTimeMillis() - startTime) / 1000.0);
                System.out.println("Single-Tier Query Time = " + (System.currentTimeMillis() - startTime) / 1000.0 + " s");
                System.out.println("\t| # Results Returned: " + results_single.size() + "\n");
                System.out.println("Average Precision (AP): " + query.average_precision(results_single, results_multi) + "\n\n");

                mean_average_precision += query.average_precision(results_single, results_multi);
                ++i;
            }
            mean_average_precision /= i;
            System.out.println("Tier 1 Split Percentage: " + tier_1_split_str);
            System.out.println("Target # of results returned per query: " + records);
            System.out.println("Number of queries executed: " + i);
            System.out.println("Mean Average Precision: " + mean_average_precision);
            System.out.println("Multi-Tier/Single-Tier Query Execution Time Ratio : " + multitier_time/singletier_time);
        } else if (batchQueryFlag.equals("0")) {
            while (true) {
                System.out.print("Enter query or enter exit to quit : ");
                String input = br.readLine();
                if (input.equals("exit")) { break; }

                startTime = System.currentTimeMillis();
                ArrayList<Integer> results_multi = query_multi.getSearchResults_MultiTier(input, Integer.parseInt(records));
                System.out.println("Mutli-Tier Query Time = " + (System.currentTimeMillis() - startTime) / 1000.0 + " s");
                System.out.println("\t| # Results Returned: " + results_multi.size());

                startTime = System.currentTimeMillis();
                ArrayList<Integer> results_single = query_single.getSearchResults_SingleTier(input, Integer.parseInt(records));
                System.out.println("Single-Tier Query Time = " + (System.currentTimeMillis() - startTime) / 1000.0 + " s");
                System.out.println("\t| # Results Returned: " + results_single.size() + "\n");

//                for (Integer p : results_multi) System.out.println(p);
//                for (Integer m : results_single) System.out.println(m);

                System.out.println("Average Precision (AP): " + query.average_precision(results_single, results_multi) + "\n\n");
            }
        } else {
            System.out.println("Invalid response.");
        }
        return;
    }
}