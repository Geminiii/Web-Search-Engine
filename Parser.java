package backend.se;

import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;
import java.io.*;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class is for parsing the WET files.
 */
class Parser {

    /**
     * Initial docID set to 1.
     */
    private static int docID = 1;

    /**
     * Returns true if this record's WARC-Type is conversion. false otherwise.
     * @param record A WarcRecord.
     * @return true if this record's WARC-Type is conversion. false otherwise.
     */
    private static boolean isConversion(WarcRecord record){
        return record.getHeader("WARC-Type").value.equals("conversion");
    }

    /**
     * Returns the content length of this WarcRecord.
     * @param record A WarcRecord.
     * @return The content length of this WarcRecord.
     * @throws NumberFormatException if it occurs.
     */
    private static int getContentLength(WarcRecord record) throws NumberFormatException {
        String contentLength = record.getHeader("Content-Length").value;
        if(contentLength == null || contentLength.equals("")){
            return 0;
        }
        return Integer.parseInt(contentLength);
    }

    /**
     * Returns the url of this WarcRecord.
     * @param record A WarcRecord.
     * @return The url of this WarcRecord.
     */
    private static String getURL(WarcRecord record){

        return record.getHeader("WARC-Target-URI").value;
    }

    /**
     * Parses a WarcRecord. It sorts all distinct words in this record alphabetically.
     * Store them in a String in the posting format of (word1 docID freq1 "\n" word2 docID freq2...).
     * @param record A WarcRecord.
     * @param docID The id of this doc
     * @return A String containing all postings of this Warc Record.
     */
    private static String parseRecord(WarcRecord record, int docID){
        TreeMap<String, Integer> tm = new TreeMap<>();
        StringBuilder builder = new StringBuilder();

        String thisLine;
        Pattern pattern = Pattern.compile("\\w+");

        try(
                InputStream payLoadIS = record.getPayloadContent();
                BufferedReader payLoadBR = new BufferedReader(new InputStreamReader(payLoadIS))
        ){
            while ((thisLine = payLoadBR.readLine()) != null) {
                Matcher matcher = pattern.matcher(thisLine.toLowerCase());
                while (matcher.find()) {
                    int freq = 0;
                    String word = matcher.group();
                    if(tm.containsKey(word)){
                        freq = tm.get(word);
                    }
                    tm.put(word, freq + 1 );
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        for(Map.Entry<String,Integer> entry : tm.entrySet()){
            String key = entry.getKey();
            String value = entry.getValue().toString() + "\n";
            builder.append(key).append(" ").append(docID).append(" ").append(value);
        }

        return builder.toString();

    }

    /**
     * Parses a single WET file. Write the result posting to an intermediate posting file.
     * Update the url_table file recording docID and url.
     * @param f A WET file to be parsed.
     * @param urlTableWriter A Buffered PrintWriter to update url table.
     */
    static void parseWET(File f, PrintWriter urlTableWriter){
        String PATH = Path.PATH;
        try(
                FileInputStream is = new FileInputStream(f);
                WarcReader reader = WarcReaderFactory.getReader(is);
                FileOutputStream postingOS = new FileOutputStream( PATH + "postings/" + f.getName().replace(".warc.wet.gz", "_posting"));
                PrintWriter postingWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(postingOS)))
        ){
            int  curID = docID;
            WarcRecord record;
            int contentLength;
            while((record = reader.getNextRecord()) != null){
                if( isConversion(record) && (contentLength = getContentLength(record)) != 0 ){
                    String url = getURL(record);
                    String postings = parseRecord(record, docID);
                    //Write to posting file
                    postingWriter.print(postings.trim() + "\n");
                    //Write to url_table
                    urlTableWriter.println(docID +" "+ url +" "+contentLength);
                    docID++;
                }
            }
            System.out.println((docID - curID) + " records was parsed. Accumulated records parsed: " + docID);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
