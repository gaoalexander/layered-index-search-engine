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

class InvertedIndexSingleTier {
    private GZIPOutputStream lexiconSingleTier;
    private FileOutputStream invertedIndexSingleTier;
    private GZIPInputStream sortedTermsFile;

    private Integer total_num_docs;
    private Integer len_doc;
    private Integer docs_t;
    private float len_doc_avg;
    private float k1_param, b_param;

    private HashMap<Integer, Integer> docIDToWordCountMap;
    private HashMap<String, Integer> termToDocCountMap;

    InvertedIndexSingleTier(String sortedTermsFilePath, String lexiconSingleTierFilePath, String invertedIndexSingleTierPath) {
        this.lexiconSingleTier = createGzipFile(lexiconSingleTierFilePath);
        this.invertedIndexSingleTier = createFile(invertedIndexSingleTierPath);
        this.sortedTermsFile = openTermsFile(sortedTermsFilePath);

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

            System.out.println("buildDocIdsToWordCount mapping size = " + docIDToWordCountMap.size());
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
        return lexiconSingleTier != null && invertedIndexSingleTier != null && sortedTermsFile != null;
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
            Integer totalBytesSingleTier = 0;
            while ((currentTerm = br.readLine()) != null) {
                String[] posting = currentTerm.split(" ");
                this.len_doc = docIDToWordCountMap.get(Integer.parseInt(posting[1]));

                if (posting[0].equals(previousTerm) || previousTerm == null) {
                    this.docs_t = termToDocCountMap.get(posting[0].toLowerCase());
                    frequencyDocIdsMappingList.add(new FrequencyDocIdsMapping(Integer.parseInt(posting[2]), Long.parseLong(posting[1]), k1_param, b_param, len_doc_avg, len_doc, total_num_docs, docs_t));
                } else {
                    Collections.sort(frequencyDocIdsMappingList);
                    Integer singleTierLength = Double.valueOf(Math.ceil(frequencyDocIdsMappingList.size())).intValue();
                    StringBuffer totalBytesDocIdsFreqs = new StringBuffer();
                    for(int i=0; i< singleTierLength;i++){
                        totalBytesDocIdsFreqs.append(findVarByte((Long.valueOf(frequencyDocIdsMappingList.get(i).getFrequency()))));
                        totalBytesDocIdsFreqs.append(findVarByte(frequencyDocIdsMappingList.get(i).getDocId()));
                    }
                    byte[] bytesForTerm = totalBytesDocIdsFreqs.toString().getBytes();
                    Integer totalBytesForTerm = bytesForTerm.length;
                    invertedIndexSingleTier.write(bytesForTerm);
                    lexiconSingleTier.write((previousTerm + " " + (totalBytesSingleTier + 1) + " " + totalBytesForTerm + " "
                            + singleTierLength + " \n").getBytes());
                    totalBytesSingleTier += totalBytesForTerm;
                    totalBytesDocIdsFreqs = new StringBuffer();
                    frequencyDocIdsMappingList.clear();
                    this.docs_t = termToDocCountMap.get(posting[0].toLowerCase());
                    frequencyDocIdsMappingList.add(new FrequencyDocIdsMapping(Integer.parseInt(posting[2]), Long.parseLong(posting[1]), k1_param, b_param, len_doc_avg, len_doc, total_num_docs, docs_t));
                }
                previousTerm = posting[0];
            }
            sortedTermsFile.close();
            invertedIndexSingleTier.close();
            lexiconSingleTier.finish();
            lexiconSingleTier.close();
        } catch (IOException e) {
            System.out.println("Error while reading the input" + e);
        }
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        InvertedIndexSingleTier index = new InvertedIndexSingleTier("data/1_intermediate/postings/sorted.gz", "data/2_index/lexiconSingleTier.gz", "data/2_index/invertedIndexSingleTier");

        index.buildDocIDToWordCountMap("data/2_index/url_doc_mapping.gz");
        index.buildTermToDocCountMap("data/1_intermediate/postings/sorted.gz");

        if (index.ifLexiconAndInvertedIndexDocumentCreated())
            index.createIndex();
        System.out.println("Total time =" + (System.currentTimeMillis() - startTime) / 60000.0);
    }

}