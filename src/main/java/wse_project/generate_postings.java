package wse_project;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;



class GeneratePostings {
    private String jsonDatasetPath;
    private GZIPOutputStream urlToDocMappingFile;
    private File postingsDirectory;

    GeneratePostings(String jsonDatasetPath, String postingsOutputPath) {
        this.jsonDatasetPath = jsonDatasetPath;
        this.urlToDocMappingFile = createUrlToDocMapping();
        this.postingsDirectory = createOutputDirectory(postingsOutputPath);
    }

    private GZIPOutputStream createUrlToDocMapping() {
        try {
            return new GZIPOutputStream(new FileOutputStream("data/2_index/url_doc_mapping.gz"));
        } catch (IOException e) {
            System.out.println("Unable to create URL to Doc Mapping file");
        }
        return null;
    }

    public Boolean ifDirectoryAndMappingDocumentCreated() {
        return postingsDirectory != null && urlToDocMappingFile != null;
    }

    private File createOutputDirectory(String postingsPath) {
        File file = new File(postingsPath);
        if (!file.exists()) {
            if (file.mkdir()) {
                System.out.println("Directory is created!");
            } else {
                System.out.println("Failed to create directory!");
            }
        }

        return file;
    }

    private Boolean isCorrectWord(String word) {
        return ((word != null) && (word.length() > 2) && (word.matches("^[a-zA-Z0-9]*$")));
    }

    private Map<String, Integer> findWordsCount(String[] words) {
        Map<String, Integer> wordsCount = new HashMap<String, Integer>();
        for (String word : words) {
            word = word.toLowerCase();
            if (!isCorrectWord(word)) {
                continue;
            }
            Integer count = wordsCount.getOrDefault(word, 0);
            wordsCount.put(word, count + 1);
        }
        return wordsCount;
    }

    private Boolean isPageValid(Map<String, Integer> wordsCount, int totalWords) {
        int totalWordsAfterParsing = 0;
        for (Integer count : wordsCount.values()) {
            totalWordsAfterParsing += count;
        }

        return (float) totalWordsAfterParsing / totalWords > 0.1;
    }

    public void createPostings() {
        final JSONParser jsonParser = new JSONParser();
        long totalUrls = 0;
        try(FileReader reader = new FileReader(jsonDatasetPath)){
            Object obj = jsonParser.parse(reader);
            JSONArray recipeList = (JSONArray) obj;
            String postingsFileName = "data/1_intermediate/postings/postings.gz";
            GZIPOutputStream gzos = new GZIPOutputStream(
                    new FileOutputStream(postingsFileName));

            for (Object o : recipeList) {
                JSONObject jsonObject = (JSONObject) o;
                String title = (String) jsonObject.get("title");
                JSONArray ingredientsArray = (JSONArray) jsonObject.get("ingredients");
                ArrayList<String> ingredients = new ArrayList();
                for(Object ingredient: ingredientsArray){
                    ingredients.add((String) ingredient);
                }
                JSONArray instructionsArray = (JSONArray) jsonObject.get("instructions");
                ArrayList<String> instructions = new ArrayList();
                for(Object instruction: instructionsArray){
                    instructions.add((String) instruction);
                }
                String url = (String) jsonObject.get("url");
                StringBuffer sb = new StringBuffer();
                sb.append(title+" ");
                for(String ingredient: ingredients){
                    sb.append(ingredient+" ");
                }
                for(String instruction: instructions){
                    sb.append(instruction+" ");
                }
                String content = new String(sb.toString());
                String[] words = content.split(" ");

                Map<String, Integer> wordsCount = findWordsCount(words);

                if (!isPageValid(wordsCount, words.length)) {
                    continue;
                }
                totalUrls++;
                for (String word : wordsCount.keySet()) {
                    String posting = word + " " + totalUrls + " " + wordsCount.get(word) + "\n";
                    gzos.write(posting.getBytes());
                }

                urlToDocMappingFile.write((totalUrls + " " + url + " " + words.length+"\n").getBytes());
            }

            gzos.finish();
            gzos.close();
            urlToDocMappingFile.close();

        } catch(FileNotFoundException e){
            e.printStackTrace();
        } catch (IOException io){
            io.printStackTrace();
        } catch (ParseException pe){
            pe.printStackTrace();
        }
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        GeneratePostings gp = new GeneratePostings("data/0_crawl/crawled_formatted_data.json", "data/1_intermediate/postings");
        if (gp.ifDirectoryAndMappingDocumentCreated())
            gp.createPostings();
        System.out.println("Total time =" + (System.currentTimeMillis() - startTime) / 60000.0);
    }
}