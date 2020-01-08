package wse_project;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.lang.Float;

class FrequencyDocIdsMapping implements Comparable<FrequencyDocIdsMapping>{
    private int frequency;
    private Long class_docID;

    FrequencyDocIdsMapping(Integer frequency, Long docID, float k1_param, float b_param, float len_doc_avg, Integer len_doc, Integer total_num_docs, Integer docs_t) {
        this.frequency = (int)(10000 * this.bm25_contribution(frequency, k1_param, b_param, len_doc_avg, len_doc, total_num_docs, docs_t));
        if (this.frequency < 0) this.frequency = 0;
        this.class_docID = docID;
    }

    public float bm25_contribution(Integer frequency, float k1_param, float b_param, float len_doc_avg, Integer len_doc, Integer total_num_docs, Integer docs_t) {
        float k_param = k1_param * ((1.0f - b_param) + b_param * ((float)len_doc / (float)len_doc_avg));
        return (float)Math.log(((float)total_num_docs - (float)docs_t + 0.5f) / ((float)docs_t + 0.5f)) * ((((float)k1_param + 1.0f) * (float)frequency) / ((float)k_param + (float)frequency));
    }

    public int getFrequency(){
        return this.frequency;
    }

    public Long getDocId(){
        return this.class_docID;
    }

    @Override
    public int compareTo(FrequencyDocIdsMapping frequencyDocIdsMapping) {
        return frequencyDocIdsMapping.getFrequency() - this.getFrequency(); //Float.compare(frequencyDocIdsMapping.getFrequency(), this.getFrequency()); //
    }
}

class InvertedIndex {
    private GZIPOutputStream lexiconTier1;
    private GZIPOutputStream lexiconTier2;
    private FileOutputStream invertedIndexTier1;
    private FileOutputStream invertedIndexTier2;
    private GZIPInputStream sortedTermsFile;
    private Double tier1Percentage;
    private Integer total_num_docs;
    private Integer len_doc;
    private Integer docs_t;
    private float len_doc_avg;
    private float k1_param, b_param;

    private HashMap<Integer, Integer> docIDToWordCountMap;
    private HashMap<String, Integer> termToDocCountMap;

    InvertedIndex(String sortedTermsFilePath, String lexiconTier1FilePath, String lexiconTier2FilePath, String invertedIndexTier1Path, String invertedIndexTier2Path, Double tier1Percentage) {
        this.lexiconTier1 = createGzipFile(lexiconTier1FilePath);
        this.lexiconTier2 = createGzipFile(lexiconTier2FilePath);
        this.invertedIndexTier1 = createFile(invertedIndexTier1Path);
        this.invertedIndexTier2 = createFile(invertedIndexTier2Path);
        this.sortedTermsFile = openTermsFile(sortedTermsFilePath);
        this.tier1Percentage = tier1Percentage;
        this.docIDToWordCountMap = new HashMap();
        this.termToDocCountMap = new HashMap();

        this.k1_param = 1.2f;
        this.b_param = 0.75f;
        this.len_doc_avg = 0;
        this.total_num_docs = 0;
        this.docs_t = 0;
        this.len_doc = 0;
    }

    private FileOutputStream createFile(String fileName) {
        try {
            return new FileOutputStream(fileName);
        } catch (FileNotFoundException e) {
            System.out.println("Unable to read file");
        }
        return null;
    }

    private GZIPInputStream openTermsFile(String fileName) {
        try {
            return new GZIPInputStream(new FileInputStream(fileName));
        } catch (IOException e) {
            System.out.println("Unable to create " + fileName);
        }
        return null;
    }

    public void buildTermToDocCountMap(String fileName) {
        try {
            GZIPInputStream sortedPostingsFile = new GZIPInputStream(new FileInputStream(fileName));
            BufferedReader br = new BufferedReader(new InputStreamReader(sortedPostingsFile));
            String row = null;
            String prev_term = "";

            while ((row = br.readLine()) != null) {
                String[] row_entries = row.split(" ");
                String cur_term = row_entries[0].toLowerCase();

                if (cur_term.equals(prev_term)) {
                    termToDocCountMap.replace(cur_term, termToDocCountMap.get(cur_term) + 1);
                } else termToDocCountMap.put(cur_term, 1);
                prev_term = cur_term;
            }
            sortedPostingsFile.close();
        } catch (IOException e) { System.out.println("Unable to read content from file"); }
    }

    public void buildDocIDToWordCountMap(String fileName) {
        try {
            GZIPInputStream lexiconFile = new GZIPInputStream(new FileInputStream(fileName));
            BufferedReader br = new BufferedReader(new InputStreamReader(lexiconFile));
            String currentTerm = null;

            while ((currentTerm = br.readLine()) != null) {
                String[] docIDsToUrlMappingValues = currentTerm.split(" ");
                if (docIDsToUrlMappingValues.length == 3) {
                    Integer docId = Integer.parseInt(docIDsToUrlMappingValues[0]);
                    Integer totalTermsCount = Integer.parseInt(docIDsToUrlMappingValues[2]);
                    this.len_doc_avg += totalTermsCount;
                    ++this.total_num_docs;
                    try {
                        docIDToWordCountMap.put(docId, totalTermsCount);
                    } catch (Exception e) {
                        System.out.println("Exception caught" + e);
                    }
                }
            }
            this.len_doc_avg /= this.total_num_docs;
            System.out.println("Total num docs: " + this.total_num_docs);
            System.out.println("Avg Doc Len: " + this.len_doc_avg);
            lexiconFile.close();
        } catch (IOException e) { System.out.println("Unable to read content from file"); }
    }

    private GZIPOutputStream createGzipFile(String fileName) {
        try {
            return new GZIPOutputStream(new FileOutputStream(fileName));
        } catch (IOException e) {
            System.out.println("Unable to create " + fileName);
        }
        return null;
    }

    public Boolean ifLexiconAndInvertedIndexDocumentCreated() {
        return lexiconTier1 != null && lexiconTier2 != null && invertedIndexTier1 != null && invertedIndexTier2 != null && sortedTermsFile != null;
    }

    public String findVarByte(Long number) {
        String binaryValue = Long.toBinaryString(number);
        StringBuffer sb = new StringBuffer();
        int counter = 7;
        Boolean lastBit = false;
        for (int i = binaryValue.length() - 1; i >= 0; i--) {
            Character bit = binaryValue.charAt(i);
            if (counter == 0) {
                String lastBitValue = lastBit ? "0" : "1";
                lastBit = true;
                sb.append(lastBitValue + bit);
                counter = 6;
            } else {
                sb.append(bit);
                counter--;
            }
        }
        while (counter > 0) {
            sb.append("0");
            counter--;
        }
        if (number <= 127) {
            sb.append("1");
        } else {
            sb.append("0");
        }
        return sb.reverse().toString();
    }

    public void createIndex() {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(sortedTermsFile));
            String previousTerm = null, currentTerm = null;
            ArrayList<FrequencyDocIdsMapping> frequencyDocIdsMappingList = new ArrayList();
            Integer totalBytesTier1 = 0, totalBytesTier2 = 0;
            while ((currentTerm = br.readLine()) != null) {
                String[] posting = currentTerm.split(" ");
//                System.out.println("DEBUG: " + currentTerm);
//                System.out.println("DEBUG: " + posting[1]);

                this.len_doc = docIDToWordCountMap.get(Integer.parseInt(posting[1]));
                if (posting[0].equals(previousTerm) || previousTerm == null) {
//                    System.out.println("DEBUG2: sunburst");
//                    System.out.println("DEBUG2: " + termToDocCountMap.get("sunburst"));
                    this.docs_t = termToDocCountMap.get(posting[0].toLowerCase());
                    frequencyDocIdsMappingList.add(new FrequencyDocIdsMapping(Integer.parseInt(posting[2]), Long.parseLong(posting[1]), k1_param, b_param, len_doc_avg, len_doc, total_num_docs, docs_t));
                } else {
                    Collections.sort(frequencyDocIdsMappingList);
                    Integer tier1Length = Double.valueOf(Math.ceil(frequencyDocIdsMappingList.size() * tier1Percentage/100.0)).intValue();
                    StringBuffer totalBytesDocIdsFreqs = new StringBuffer();
                    for(int i=0; i< tier1Length;i++){
                        totalBytesDocIdsFreqs.append(findVarByte((Long.valueOf(frequencyDocIdsMappingList.get(i).getFrequency()))));
                        totalBytesDocIdsFreqs.append(findVarByte(frequencyDocIdsMappingList.get(i).getDocId()));
                    }
                    byte[] bytesForTerm = totalBytesDocIdsFreqs.toString().getBytes();
                    Integer totalBytesForTerm = bytesForTerm.length;
                    invertedIndexTier1.write(bytesForTerm);
                    lexiconTier1.write((previousTerm + " " + (totalBytesTier1 + 1) + " " + totalBytesForTerm + " "
                            + tier1Length + " \n").getBytes());
                    totalBytesTier1 += totalBytesForTerm;
                    totalBytesDocIdsFreqs = new StringBuffer();
                    for(int i = tier1Length; i< frequencyDocIdsMappingList.size();i++){
                        totalBytesDocIdsFreqs.append(findVarByte((Long.valueOf(frequencyDocIdsMappingList.get(i).getFrequency()))));
                        totalBytesDocIdsFreqs.append(findVarByte(frequencyDocIdsMappingList.get(i).getDocId()));
                    }
                    bytesForTerm = totalBytesDocIdsFreqs.toString().getBytes();
                    totalBytesForTerm = bytesForTerm.length;
                    invertedIndexTier2.write(bytesForTerm);
                    lexiconTier2.write((previousTerm + " " + (totalBytesTier2 + 1) + " " + totalBytesForTerm + " "
                            + (frequencyDocIdsMappingList.size() - tier1Length) + " \n").getBytes());
                    totalBytesTier2 += totalBytesForTerm;
                    frequencyDocIdsMappingList.clear();
                    this.docs_t = termToDocCountMap.get(posting[0].toLowerCase());
                    frequencyDocIdsMappingList.add(new FrequencyDocIdsMapping(Integer.parseInt(posting[2]), Long.parseLong(posting[1]), k1_param, b_param, len_doc_avg, len_doc, total_num_docs, docs_t));
                }
                previousTerm = posting[0];
            }
            sortedTermsFile.close();
            invertedIndexTier1.close();
            invertedIndexTier2.close();
            lexiconTier1.finish();
            lexiconTier1.close();
            lexiconTier2.finish();
            lexiconTier2.close();
        } catch (IOException e) {
            System.out.println("Error while reading the input" + e);
        }
    }

    public static void main(String[] args) {
        // :tier_1_split:   defines percentage of term lists to be included in Tier 1
        double tier_1_split = 20.0;
        String tier_1_split_str = "20";

        String dataIntermediateDir = "data/1_intermediate/", dataIndexDir = "data/2_index/";
        String sortedTermsFilePath = dataIntermediateDir + "postings/sorted.gz";
        String lexiconTier1FilePath = dataIndexDir + "lexiconTier1_" + tier_1_split_str + ".gz";
        String lexiconTier2FilePath = dataIndexDir + "lexiconTier2_" + tier_1_split_str + ".gz";
        String invertedIndexTier1Path = dataIndexDir + "invertedIndexTier1_" + tier_1_split_str;
        String invertedIndexTier2Path = dataIndexDir + "invertedIndexTier2_" + tier_1_split_str;
        String urlDocMappingPath = dataIndexDir + "url_doc_mapping.gz";

        long startTime = System.currentTimeMillis();
        InvertedIndex index = new InvertedIndex(sortedTermsFilePath, lexiconTier1FilePath, lexiconTier2FilePath, invertedIndexTier1Path, invertedIndexTier2Path, tier_1_split);
        index.buildDocIDToWordCountMap(urlDocMappingPath);
        index.buildTermToDocCountMap(sortedTermsFilePath);
        if (index.ifLexiconAndInvertedIndexDocumentCreated()) index.createIndex();
        System.out.println("Total time =" + (System.currentTimeMillis() - startTime) / 60000.0);
    }

}