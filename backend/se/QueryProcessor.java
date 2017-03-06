package backend.se;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import com.google.common.collect.Maps;

/**
 * This class starts up the search engine and deals with queries input from users.
 */
public class QueryProcessor {
    /**
     * Path of the data.
     */
    private final static String PATH = Path.PATH;

    /**
     * Average of documents length in a collection.
     */
    private static float dAvg;

    /**
     * A List containing docs and their url and length, docID as key, url and doc length as value.
     */
    private static List<String> urlTable = new ArrayList<>();

    /**
     * Size of lexicon.
     */
    private static int lexicon_SIZE = 22641384;

    /**
     * An array containing terms in lexicon in order.
     */
    private static String[] lexiconArray = new String[lexicon_SIZE];

    /**
     * An array containing offset of each term's index in inverted_index file.
     */
    private static long[] offsetArray = new long[lexicon_SIZE];

    /**
     * An instance of LRUCache to manage cache.
     */
    private static LRUCache indexCache = new LRUCache(128 * 1048576);

    /**
     * A ordered set containing top 10 urls and their BM25 scores, ordered by BM25 scores.
     */
    private static TreeSet<Entry<String, Float>> heap =
            new TreeSet<>(Map.Entry.comparingByValue(Comparator.reverseOrder()));

    static {
        String bootstrapTime;
        long t1 = System.currentTimeMillis();
        startUp();
        long t2 = System.currentTimeMillis();
        bootstrapTime = String.format("Bootstrap in %.3f seconds", (float)(t2 - t1)/1000);
        System.out.println(bootstrapTime);

        warmUp();
        long t3 = System.currentTimeMillis();
        bootstrapTime = String.format("Warmed up in %.3f seconds", (float)(t3 - t2)/1000);
        System.out.println(bootstrapTime);
        System.out.println(indexCache.size);
    }

    /**
     * Start up the search engine server. Reading lexicon and url table into memory.
     * Computing the doc average length.
     */
    private static void startUp(){
        File lexiconFile = new File(PATH + "output/lexicon_for_binary");
        File urlTableFile= new File(PATH + "output/url_table");
        try(
                BufferedReader urlTableFIS = new BufferedReader(new FileReader(urlTableFile))
        ){
            String thisLine;
            long accum = 0L;
            urlTable.add("");
            while((thisLine = urlTableFIS.readLine()) != null){
                String[] l = thisLine.split(" ", 2);
                if(l.length != 2) continue;
                urlTable.add(l[1]);
                accum += Long.parseLong(l[1].split(" ")[1]);
            }
            dAvg = (float) accum / urlTable.size();
            System.out.println("urlTable finished");
            urlTableFIS.close();

        }catch (Exception e){
            e.printStackTrace();
        }

        try(BufferedReader lexiconFIS = new BufferedReader(new FileReader(lexiconFile))){
            String thisLine;
            long off = 0L;
            String[] ll;
            int i = 0;
            while((thisLine = lexiconFIS.readLine()) != null){
                ll = thisLine.split(" ", 3);
                if(ll.length != 3) continue;
                off = Long.parseLong(ll[1]);
                lexiconArray[i] = ll[0];
                offsetArray[i] = off;
                i++;
            }
            System.out.println("Lexicon finished" + off);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * Warms up the search engine with cache by reading queries from query file
     * and opens the Indices of terms, then stores them into a Hash map.
     */
    private static void warmUp(){
        String PATH = "/Users/Li/Downloads/";
        File queries = new File(PATH + "top100.txt");
        try(BufferedReader br = new BufferedReader(new FileReader(queries))) {

            String thisLine;
            while ((thisLine = br.readLine()) != null) {
                String[] s = thisLine.split("\t");
                String query = s[s.length - 1].toLowerCase();
                System.out.print(query + indexCache.size + " ");
                if(indexCache.containsKey(query)) continue;

                Index lp = openList(query, -1);
                if(lp == null) continue;
                dynamicCache(lp);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * Fetches the inverted index of a term according to their offset and length.
     * @param dataFile The file containing the whole inverted index.
     * @param off The offset of the beginning of this term's inverted index.
     * @param len The length of this term's inverted index.
     * @return A ByteBuffer containing the inverted index of this term.
     */
    private static ByteBuffer fetchPage(File dataFile, long off, int len) {
        ByteBuffer buf = ByteBuffer.allocate(len);
        try{
            RandomAccessFile raf = new RandomAccessFile(dataFile, "rw");
            raf.seek(off);
            for (int i = 0; i < len; i++) {
                buf.put(raf.readByte());
            }
            raf.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
        //cacheSize += buf.position();
        return buf;
    }

    /**
     * Opens the inverted list of specific word.
     * @param word The specific word to be opened.
     * @return The inverted list of specific word.
     */
    private static Index openList(String word, int index){

        if(indexCache.containsKey(word)) return indexCache.get(word);

        File invIndexFile= new File(PATH + "output/inverted_in_binary");

        int length;
        long offset;
        ByteBuffer invIndex;
        if(index == -1) index = Arrays.binarySearch(lexiconArray, word);
        if(index >= 0 ){
            offset = offsetArray[index];
            length = (int) (offsetArray[index+1] - offset);
            invIndex = fetchPage(invIndexFile, offset, length);
            return new Index(invIndex, word);
        }
        return null;
    }

    /**
     * Reset an Index to initial state and cache it.
     * @param lp The Index to be cached.
     */
    private static void dynamicCache(Index lp){
        lp.reset();
        indexCache.set(lp.getTerm(), lp);
    }

    /**
     * Finds the next posting in list lp that docID greater or equal to did, and returns it if exists.
     * @param lp The posting list to be looked up.
     * @param did The specific docID to search against.
     * @return The next posting in list lp that docID greater or equal to did, and returns it if exists.
     */
    private static int nextGEQ(Index lp, int did){
        int[] lastDocID = lp.getLastDocID();
        while(lastDocID[lp.getPosition()] < did && lp.skipNext());

        List<Integer> temp = lp.decodeCurrDocID();
        //int i=0;
        //while(i < temp.size() && temp.get(i) < did) i++;
        int ngeq = Collections.binarySearch(temp, did);
        //if(ngeq >= 0) return temp.get(ngeq);
        if(ngeq < 0) ngeq = -1 - ngeq;
        return (ngeq == temp.size()) ? 0 : temp.get(ngeq);
    }

    /**
     * Returns the frequency of a term in a specific doc.
     * @param lp The Index of specific term.
     * @param did A docId.
     * @return The frequency of a term in a specific doc.
     */
    private static int getFreqDT(Index lp, int did){
        List<Integer> docID = lp.getDecodedDocID(did);
        List<Integer> freq = lp.decodeCurrFreq();
        return freq.get(docID.indexOf(did));
    }

    /**
     * Compute BM25 score for a doc.
     * @param ft An array containing frequencies of each term in this doc.
     * @param fdt An array containing frequencies of each term in the whole collection.
     * @param did The specific docID.
     * @return The BM25 score of this doc against specific query terms.
     */
    private static float computeBM25(int[] ft, int[] fdt, int did, int[] count){
        float k1 = 1.2f;
        float b = 0.75f;
        int d = Integer.parseInt(urlTable.get(did).split(" ")[1]);
        float K = k1 * ((1 - b) + b * d / dAvg);
        int N = urlTable.size();
        float bm25Score = 0f;
        for(int i =0; i<ft.length; i++){
            bm25Score += Math.log((N - ft[i] + 0.5) / (ft[i] + 0.5)) * count[i] * (k1 + 1) * fdt[i] / (K + fdt[i]);
        }
        return bm25Score;
    }

    /**
     * Updates the heap according to specific docID and its score.
     * If the did is already in this heap, which would only happen in disjunctive mode, accumulate the score.
     * Otherwise, insert this did and score into the heap if the heap is not full or the score is greater than the lowest score.
     * @param did The docID to be updated.
     * @param score The BM25 score of corresponding doc.
     */
    private static void updateHeap(int did, float score){
        String url = urlTable.get(did).split(" ")[0];
        for (Map.Entry<String, Float> u: heap
             ) {
            if(u.getKey().equals(url)) score += u.getValue();
        }
        if(heap.size() < 10){
            heap.add(Maps.immutableEntry(url, score));
        }else{
            float last = heap.last().getValue();
            if(last < score){
                heap.pollLast();
                heap.add(Maps.immutableEntry(url, score));
            }
        }
    }

    /**
     * Processes conjunctive query. Open indices of all terms in the query,
     * Looping through the shortest inverted list, find all intersection docIDs in other terms inverted list.
     * For the intersection docIDs, compute their BM25 scores and update the heap.
     * @param queryTerms The query terms to be processed.
     */
    private static void conjunctProcess(String[] queryTerms, HashMap<String, Integer> count){

        int termNum = queryTerms.length;
        Index[] lp = new Index[termNum];

        for(int i = 0; i< termNum; i++) {
            long t1 = System.currentTimeMillis();
            if(indexCache.containsKey(queryTerms[i])) {
                lp[i] = indexCache.get(queryTerms[i]);
            }
            int j = Arrays.binarySearch(lexiconArray, queryTerms[i]) ;
            if(j < 0){
                System.out.println("No web page contains term: " + queryTerms[i]);
                return;
            }else if(j>=0){
                lp[i] = openList(queryTerms[i], j);
            }
            long t2 = System.currentTimeMillis();
            System.out.print(queryTerms[i] + (t2 - t1) + " ");
        }

        Arrays.sort(lp, (Index o1, Index o2) -> (o1.getNumOfBlocks() - o2.getNumOfBlocks()));
        int[] lastDocID = lp[0].getLastDocID();
        int did = 1;
        while(did <= lastDocID[lastDocID.length - 1]){
            did = nextGEQ(lp[0], did);
            int d = did - 1;
            for(int i =1; i< termNum && (d = nextGEQ(lp[i], did)) == did; i++);
            if(d > did) did = d;
            else if(d == did){
                int[] fdt = new int[termNum];
                int[] ft = new int[termNum];
                int[] countArray = new int[termNum];
                for(int i=0; i<termNum; i++) fdt[i] = getFreqDT(lp[i], did);
                for(int i=0; i<termNum; i++) ft[i] = lp[i].getFT();
                for(int i=0; i<termNum; i++) countArray[i] = count.get(lp[i].getTerm());
                float score = computeBM25(ft, fdt, did, countArray);

                updateHeap(did, score);

                did++;
            }else if(d < did) break;
        }
        for(int i = 0; i< termNum; i++) { dynamicCache(lp[i]); }
    }

    /**
     * Processes disjunctive query. Opens indices of all terms in the query,
     * and compute BM25 scores for each doc containing one or more of the terms, then update the heap.
     * @param queryTerms The query terms to be processed.
     */
    private static void disjunctProcess(String[] queryTerms, HashMap<String, Integer> count){
        int counter = 0;
        for(String term: queryTerms){
            int i = Arrays.binarySearch(lexiconArray, term);
            if(i < 0){
                counter += 1;
                if(counter == queryTerms.length){
                    System.out.println("No web page contains any of these terms");
                    return;
                }
                continue;
            }
            long t1 = System.currentTimeMillis();
            Index lp = openList(term, i);
            long t2 = System.currentTimeMillis();
            System.out.print(term + (t2-t1) + " ");
            List<Integer> docID;
            do{
                try {
                    docID = lp.decodeCurrDocID();
                    if (docID != null) {
                        lp.decodeCurrFreq();
                        for (int did : docID) {
                            int[] fdt = new int[]{getFreqDT(lp, did)};
                            int[] ft = new int[]{lp.getFT()};
                            int[] countArray = new int[]{count.get(lp.getTerm())};
                            float score = computeBM25(ft, fdt, did, countArray);
                            updateHeap(did, score);
                        }
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            }while(lp.skipNext());
            dynamicCache(lp);
        }
    }

    /**
     * Processes the query request.
     * @param query Query sentence from users.
     * @param mode Flag indicating disjunctive(or) or conjunctive(and) query.
     * @return Query results in String format.
     */
    public static List<String> search(String query, String mode){
        heap.clear();
        String[] q = query.toLowerCase().split(" ");
        String[] queryTerms = new HashSet<>(Arrays.asList(q)).toArray(new String[0]);
        HashMap<String, Integer> count = new HashMap<>();
        for(String term: q){
            if(count.containsKey(term)) count.put(term, count.get(term) + 1);
            else count.put(term, 1);
        }

        long t3 = System.currentTimeMillis();
        if(mode.equals("or")){
            disjunctProcess(queryTerms, count);
        }
        if(mode.equals("and")){
            if(queryTerms.length == 1){
                disjunctProcess(queryTerms,count);
            }else {
                conjunctProcess(queryTerms, count);
            }
        }
        long t4 = System.currentTimeMillis();
        System.out.println("Query processed in " + (t4 - t3) + " milliseconds");
        if(heap.isEmpty()) System.out.println("No result!");
        List<String> results = new ArrayList<>();
        while (!heap.isEmpty()) {
            Map.Entry entry = heap.pollFirst();
            String res = entry.getKey() + " " + entry.getValue();
            results.add(res);
            System.out.println(res);
        }
        return results;
    }

    public static void main(String args[]){
        Scanner scanner = new Scanner(System.in);
        do {
            System.out.println("Query: ");
            String query = scanner.nextLine();
            String mode = query.substring(query.lastIndexOf(' '));
            String newQuery = query.substring(0, query.lastIndexOf(' '));
            search(newQuery.trim(), mode.trim());
        }while(true);
    }
}
