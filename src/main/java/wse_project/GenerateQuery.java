package wse_project;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.HashSet;

public class GenerateQuery {
    private String sortedFilePath;
    private GZIPOutputStream queries;
    private String queriesPath;
    private HashSet<String> stopWordsSet = new HashSet();

    GenerateQuery(String sortedFilePath, String queriesPath){
        this.sortedFilePath = sortedFilePath;
        this.queriesPath = queriesPath;
        this.queries = createGzipFile(this.queriesPath);

        ArrayList<String> stopWords = new ArrayList(Arrays.asList("i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"));
        for (String stopword : stopWords) { stopWordsSet.add(stopword); }
    }

    private GZIPOutputStream createGzipFile(String fileName) {
        try {
            return new GZIPOutputStream(new FileOutputStream(fileName));
        } catch (IOException e) {
            System.out.println("Unable to create " + fileName);
        }
        return null;
    }

    public String getTermBasedOnProbability(ArrayList<Double> termProbabilities, ArrayList<String> termList, Double randProb1){
        int left = 0, right = termProbabilities.size()-1;
        while(left<=right){
            int mid = (left+right)/2;
            Double midProbability = termProbabilities.get(mid);
            if(right-left<=1){
                return termList.get(right);
            } else if(midProbability > randProb1){
                right = mid-1;
            } else if(midProbability < randProb1){
                left = mid+1;
            } else{
                return termList.get(mid);
            }
        }
        return null;
    }

    public boolean isStopWord(String q) { return stopWordsSet.contains(q); }

    public void generateQueries(){
        try {
            GZIPInputStream sortedTermsFile = new GZIPInputStream(new FileInputStream(this.sortedFilePath));
            BufferedReader br = new BufferedReader(new InputStreamReader(sortedTermsFile));
            HashMap<String, Integer> termFrequencyMap = new HashMap();
            String currentTerm = null;
            Integer totalFrequencyOfWords = 0;
            while ((currentTerm = br.readLine()) != null) {
                String[] termValues = currentTerm.split(" ");
                if (termValues.length == 3) {
                    String term = termValues[0];
                    Integer frequency = Integer.parseInt(termValues[2]);
                    Integer previousTermValue = (termFrequencyMap.containsKey(term))? termFrequencyMap.get(term):0;
                    termFrequencyMap.put(term, previousTermValue+frequency);
                    totalFrequencyOfWords+=frequency;
                }
            }
            ArrayList<Double> termProbabilities = new ArrayList();
            ArrayList<String> termList = new ArrayList();

            Double totalTermFrequenciesCoverage = 0.0;
            for(String key: termFrequencyMap.keySet()){
                Double probabilityOfTerm = termFrequencyMap.get(key) * 1.0 / totalFrequencyOfWords;
                totalTermFrequenciesCoverage+=probabilityOfTerm;
                termProbabilities.add(totalTermFrequenciesCoverage);
                termList.add(key);
            }

            Random r = new Random();
            int num_queries = 1000000;
            HashSet<String> queryBank = new HashSet();
            for(int i=0; i < num_queries; ++i){
                Double randProb1, randProb2;
                String term1 = "", term2 = "", term3 = "";
                String random_query = "";
                String random_query_perm1 = "";
                String random_query_perm2 = "";
                String random_query_perm3 = "";
                String random_query_perm4 = "";
                String random_query_perm5 = "";

                while (random_query == "" || queryBank.contains(random_query) || queryBank.contains(random_query_perm1) ||
                        queryBank.contains(random_query_perm2) || queryBank.contains(random_query_perm3) ||
                        queryBank.contains(random_query_perm4) || queryBank.contains(random_query_perm5)) {
                    term1 = getTermBasedOnProbability(termProbabilities, termList, r.nextDouble());
                    term2 = getTermBasedOnProbability(termProbabilities, termList, r.nextDouble());
                    term3 = getTermBasedOnProbability(termProbabilities, termList, r.nextDouble());

                    while (isStopWord(term1)) {
                        term1 = getTermBasedOnProbability(termProbabilities, termList, r.nextDouble());
                    }
                    while (isStopWord(term2) || term1.equals(term2)) {
                        term2 = getTermBasedOnProbability(termProbabilities, termList, r.nextDouble());
                    }
                    while (isStopWord(term3) || term3.equals(term2) || term3.equals(term1)) {
                        term3 = getTermBasedOnProbability(termProbabilities, termList, r.nextDouble());
                    }
                    random_query = term1 + " " + term2 + " " + term3 + "\n";
                    random_query_perm1 = term1 + " " + term3 + " " + term2 + "\n";
                    random_query_perm2 = term2 + " " + term1 + " " + term3 + "\n";
                    random_query_perm3 = term2 + " " + term3 + " " + term1 + "\n";
                    random_query_perm4 = term3 + " " + term1 + " " + term2 + "\n";
                    random_query_perm5 = term3 + " " + term2 + " " + term1 + "\n";
                }
                queryBank.add(random_query);

                queries.write(random_query.getBytes());
                System.out.println(i);
            }
            sortedTermsFile.close();
            queries.finish();
            queries.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Unable to read content from file");
        }
    }

    public static void main(String[] args){
        GenerateQuery gq = new GenerateQuery("data/1_intermediate/postings/sorted.gz", "data/1_intermediate/queries/queries.gz");
        gq.generateQueries();
    }
}
