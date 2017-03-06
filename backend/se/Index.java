package backend.se;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * This class represents an index of an term, including metadata and blocks
 */
class Index {

    /**
     * Term of this Index.
     */
    private String term;
    /**
     * Number of documents containing this term
     */
    private int ft;
    /**
     * Number of blocks in this Index
     */
    private int numOfBlocks;
    /**
     * Array including the last docID of each block
     */
    private int[] lastDocID;
    /**
     * Array including the block size in bytes
     */
    private int[] blockSize;
    /**
     * Array containing all block
     */
    private byte[] payload;
    /**
     * A index of block indicating what is the current block
     */
    private int position;
    /**
     * A byte pointer pointing to the beginning of current block in the payload
     */
    private int blockPointer;
    /**
     * A byte pointer pointing to the beginning the frequency list of current block in the payload
     */
    private int freqPointer;
    /**
     * A map containing already decoded docID list, block index as key, docID list as value
     */
    private HashMap<Integer, List<Integer>> decodedDocID;
    /**
     * A map containing already decoded frequency list, block index as key, docID list as value
     */
    private HashMap<Integer, List<Integer>> decodedFreq;

    /**
     * Constructs the Index instance according to the ByteBuffer.
     * @param buf a ByteBuffer corresponding to content in the inverted index of a term.
     * @param word The word this Index belong to.
     */
    Index(ByteBuffer buf, String word){
        term = word;
        int BLOCK_SIZE = IndexBuilder.BLOCK_SIZE;
        buf.position(0);
        ft = buf.getInt();
        numOfBlocks = (int) Math.ceil((double) ft / BLOCK_SIZE );
        lastDocID = new int[numOfBlocks];
        blockSize = new int[2 * numOfBlocks];
        for(int i = 0; i<lastDocID.length; i++){
            lastDocID[i] = buf.getInt();
        }
        for(int i = 0; i<blockSize.length; i++){
            blockSize[i] = buf.getInt();
        }
        payload = new byte[buf.limit() - buf.position()];
        buf.mark();
        buf.get(payload, 0, buf.limit() - buf.position());
        buf.reset();
        blockPointer = 0;
        freqPointer = blockPointer + blockSize[0];
        position = 0;
        decodedDocID = new HashMap<>();
        decodedFreq = new HashMap<>();
    }


    String getTerm(){
        return term;
    }

    int getNumOfBlocks(){
        return numOfBlocks;
    }

    int[] getLastDocID(){
        return lastDocID;
    }

    int getFT(){
        return ft;
    }

    int getPosition(){
        return position;
    }

    /**
     * Computes and returns the size in bytes of this Index
     * @return
     */
    int size(){
        return term.getBytes().length +
                4 * (5 + blockSize.length + lastDocID.length) + 96;
    }

    /**
     * This function check if it is possible to skip to next block.
     * If yes, skips to next block, updating blockPointer, freqPointer and position to next block.
     * If no, skips to first block and return false.
     * @return True if skipped to next block.
     */
    boolean skipNext(){
        if(position + 1 < numOfBlocks ){
            blockPointer += blockSize[2 * position] + blockSize[2 * position + 1];
            freqPointer = blockPointer + blockSize[2 * position + 2];
            position += 1;
            return true;
        }else{
            position = 0;
            return false;
        }
    }

    /**
     * Returns the original docID corresponding to the diff list.
     * @param diff A list containing difference between docIDs.
     * @param base Last docID of last block or 0 if this block is the first one.
     * @return The original list represented by the difference list.
     */
    private List<Integer> unDiff(List<Integer> diff, int base){
        List<Integer> docID = new ArrayList<>();
        for(int i = 0;i < diff.size(); i++){
            if(i ==0) {
                docID.add(i, base + diff.get(i));
            }else{
                docID.add(i, docID.get(i - 1) + diff.get(i));
            }
        }
        return docID;
    }

    /**
     * Decodes the docID of current block, and puts the docID into the decodedDocID container.
     * If already decoded, return from decodedDocID container directly.
     * Updates the freqPointer which indicates where the freq list begins in this block.
     * @return The docID list of block the current pointers pointing to.
     */
    List<Integer> decodeCurrDocID() {
        if(decodedDocID.containsKey(position)){
            return decodedDocID.get(position);
        }

        byte[] currBlock = Arrays.copyOfRange(payload, blockPointer, blockPointer + blockSize[2 * position]);
        List<Integer> diff = VarBytesCoder.decode(currBlock);
        List<Integer> docID;
        if(position == 0){
            docID = unDiff(diff, 0);
        }else{
            docID = unDiff(diff, lastDocID[position - 1]);
        }
        if(docID != null) decodedDocID.put(position, docID);

        return docID;
    }

    /**
     * Decodes freq list of current block, and puts the docID into the decodedDocID container.
     * If already decoded, return from decodedFreq container directly.
     * @return A list containing frequencies in this block.
     */
    List<Integer> decodeCurrFreq(){
        if(decodedFreq.containsKey(position)){
            return decodedFreq.get(position);
        }

        byte[] currBlock = Arrays.copyOfRange(payload, freqPointer, freqPointer + blockSize[2 * position + 1]);
        List<Integer> freq = VarBytesCoder.decode(currBlock);
        decodedFreq.put(position, freq);
        return freq;
    }

    /**
     * Returns decoded docID list of the block including did, if exists.
     * @param did The doc id which is contained in the wanted docID list.
     * @return Decoded docID list of the block including did, if exists.
     */
    List<Integer> getDecodedDocID(int did){
        int i = 0;
        while(i < numOfBlocks && did > lastDocID[i]){
            i++;
        }
        return decodedDocID.get(i);
    }

    /**
     * Reset this Index. Move all pointers to original position,
     * and clear the HashMap containing decoded
     */
    void reset(){
        blockPointer = 0;
        freqPointer = blockPointer + blockSize[0];
        position = 0;
        decodedDocID = new HashMap<>();
        decodedFreq = new HashMap<>();
    }
}
