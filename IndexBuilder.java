package backend.se;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


/**
 * This class is used to build the inverted index for the web pages collection.
 */
public class IndexBuilder {

    /**
     * Path to data files.
     */
    private final static String PATH = Path.PATH;

    /**
     * Set the docID number of each block to 128.
     */
    final static int BLOCK_SIZE = 128;

    /**
     * Computes and returns the gap between docIDs in the docID list.
     * @param docIDList A specific docID list.
     * @return The list containing gaps between docIDs in the docID list.
     */
    private static List<Integer> toDifference(List<Integer> docIDList){
        int last = -1;
        int num;
        List<Integer> diff = new ArrayList<>();
        for (int i = 0; i < docIDList.size(); i++) {
            num = docIDList.get(i);
            if (i == 0) {
                diff.add(num);
            } else {
                diff.add(num - last);
            }
            last = num;
        }
        return diff;
    }

    private static ByteBuffer block(List<Integer> docIDList, List<Integer> freqList) {
        int size = docIDList.size();
        int offset = 0;
        int numOfBlocks = (int) Math.ceil((double) size / BLOCK_SIZE );
        List<Integer> lastDocID = new ArrayList<>(numOfBlocks);
        List<Integer> blockSize = new ArrayList<>(numOfBlocks);
        List<Integer> diffList = toDifference(docIDList);
        ByteBuffer buf = ByteBuffer.allocate(2 * docIDList.size() * (Integer.SIZE / Byte.SIZE) + 64);
        while(offset < size){
            int tail;
            if((size - offset) < BLOCK_SIZE){
                tail = size;
            }else{
                tail = offset + BLOCK_SIZE;
            }
            byte[] diffListB = VarBytesCoder.encode(diffList.subList(offset, tail));
            byte[] freqListB = VarBytesCoder.encode(freqList.subList(offset, tail));
            blockSize.add(diffListB.length + freqListB.length);
            lastDocID.add(docIDList.get(tail - 1));
            buf.put(diffListB).put(freqListB);
            offset += BLOCK_SIZE;
        }
        buf.flip();
        ByteBuffer block = ByteBuffer.allocate((2 * blockSize.size() + 1) * (Integer.SIZE / Byte.SIZE) + buf.limit());
        block.putInt(numOfBlocks);
        lastDocID.forEach(block::putInt);
        blockSize.forEach(block::putInt);

        block.put(buf);

        return block;
    }

    /**
     * Convert the docID list and corresponding frequency list into bytes block list with meta data ahead of it.
     * Each block contains 128 docIds and 128 frequency, which are all Var-Bytes encoded.
     * Metadata includes number of docs, an array of last docID of each block, an array of size of each blocks'
     * docID part and frequency part.
     * @param docIDList The docID list to be converted.
     * @param freqList The frequency list corresponding to the docID list.
     * @return The ByteBuffer containing the meta data and the blocks.
     */
    private static ByteBuffer newblock(List<Integer> docIDList, List<Integer> freqList) {
        int size = docIDList.size();
        int offset = 0;
        int numOfBlocks = (int) Math.ceil((double) size / BLOCK_SIZE );
        List<Integer> lastDocID = new ArrayList<>(numOfBlocks);
        List<Integer> blockSize = new ArrayList<>(2 * numOfBlocks);
        List<Integer> diffList = toDifference(docIDList);
        ByteBuffer buf = ByteBuffer.allocate(2 * docIDList.size() * (Integer.SIZE / Byte.SIZE) + 64);
        while(offset < size){
            int tail;
            if((size - offset) < BLOCK_SIZE){
                tail = size;
            }else{
                tail = offset + BLOCK_SIZE;
            }
            byte[] diffListB = VarBytesCoder.encode(diffList.subList(offset, tail));
            byte[] freqListB = VarBytesCoder.encode(freqList.subList(offset, tail));
            blockSize.add(diffListB.length);
            blockSize.add(freqListB.length);
            lastDocID.add(docIDList.get(tail - 1));
            buf.put(diffListB).put(freqListB);
            offset += BLOCK_SIZE;
        }
        buf.flip();
        ByteBuffer block = ByteBuffer.allocate((3 * numOfBlocks + 1) * (Integer.SIZE / Byte.SIZE) + buf.limit());
        block.putInt(size);
        lastDocID.forEach(block::putInt);
        blockSize.forEach(block::putInt);

        block.put(buf);

        return block;
    }

    /**
     * Builds the index and corresponding lexicon, and writes them to files.
     * @param br A buffered reader of a posting file.
     */
    private static void buildIndexInBinary(BufferedReader br){
        StringBuilder lexicon = new StringBuilder();
        int len = -1;
        long off = 0;
        try(FileOutputStream fos = new FileOutputStream(new File(PATH +"output/inverted_in_binary"));
            FileChannel wChannel = fos.getChannel();
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(PATH +"output/"+ "lexicon_for_binary")));
        ){
            ByteBuffer buf = ByteBuffer.allocateDirect(1024*1024*16);

            String thisLine;
            String lastWord = "";
            int currDocID, freq;
            List<Integer> docIDList = new ArrayList<>();//
            List<Integer> freqList = new ArrayList<>();//

            while ((thisLine = br.readLine()) != null) {
                String[] posting = thisLine.split(" ");
                if(posting.length < 3 || posting[0].length() > 20 || posting[0].contains("_")) continue;

                try {
                    if (!posting[0].equals(lastWord)) {
                        if(docIDList.size() != 0) {
                            buf = newblock(docIDList, freqList);
                            len = buf.limit();
                        }
                        if(len > 0){
                            lexicon = new StringBuilder();
                            lexicon.append(lastWord).append(" ").append(off).append(" ").append(len);
                            off += len;
                            out.println(lexicon);
                        }
                        buf.flip();
                        wChannel.write(buf);
                        buf.clear();
                        docIDList = new ArrayList<>();//
                        freqList = new ArrayList<>();//
                    }
                    lastWord = posting[0];
                    currDocID = Integer.parseInt(posting[1]);
                    freq = Integer.parseInt(posting[2]);
                    docIDList.add(currDocID); //
                    freqList.add(freq); //
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * Call Unix sort process to sort the posting and then pip the result to buildIndexInBinary function.
     * @throws IOException If IOException occurs.
     */
    private static void sort() throws IOException{
        String[] cmd = {"/bin/sh", "-c", "sort -k1,1 -k2n,2 "+ PATH + "postings/" + "*posting* | sort -m "};
        Process p = Runtime.getRuntime().exec(cmd);
        try(
                BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
                BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()))
        ){
            buildIndexInBinary(stdInput);
            p.waitFor();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public static void main(String args[]) throws IOException{
        /*
        File folder = new File(PATH);
        File postingFolder = new File(PATH + "postings");
        boolean postingSuccess = postingFolder.mkdir();
        File outputFolder = new File(PATH + "output");
        boolean outputSuccess = outputFolder.mkdir();
        if(postingSuccess && outputSuccess) System.out.println("Directories 'postings' & 'output' have been created.");

        try(
            FileOutputStream urlTableOS = new FileOutputStream(PATH + "output/url_table");
            PrintWriter urlTableWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(urlTableOS)))
        ){
            File[] files = folder.listFiles( (File pathname) -> pathname.getName().contains(".warc.wet.gz") );
            if(files != null) {
                for (File wetFile : files) {
                    Parser.parseWET(wetFile, urlTableWriter);
                }
            }
        }catch (NullPointerException e){
            e.printStackTrace();
        }*/

        sort();


    }
}
