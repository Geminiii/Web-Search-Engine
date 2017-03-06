package backend.se;

import java.util.LinkedHashMap;
import java.util.Iterator;

/**
 * This class implements the LRU cache.
 */
class LRUCache {
    /**
     * The current size of this LRUCache instance.
     */
    int size;

    /**
     * Tha maximum size of this LRUCache instance.
     */
    private int maxSize;

    /**
     * The LinkedHashMap containing the cache.
     */
    private LinkedHashMap<String, Index> cache;

    /**
     * Constructor of the LRUCache instance.
     * @param capacity The capacity of this cache.
     */
    LRUCache(int capacity){
        this.size = 0;
        this.maxSize = capacity;
        this.cache = new LinkedHashMap<>();
    }

    /**
     * Checks whether this Cache contains this word's Index or not.
     * @param word The term to be checked.
     * @return true if it contains, false if not.
     */
    boolean containsKey(String word){
        return this.cache.containsKey(word);
    }

    /**
     * Returns the cache Index of a word.
     * @param word The word to get Index of.
     * @return The index of word if exists, or null if not.
     */
    Index get(String word){
        Index i = this.cache.get(word);
        if(i != null) this.set(word, i);
        return i;
    }

    /**
     * Puts the Index of the word into cache.
     * @param word The word to be cached.
     * @param i The index to be cached.
     */
    void set(String word, Index i){
        if(this.cache.containsKey(word)){
            this.size -= this.cache.get(word).size();
            this.cache.remove(word);
        }else if(this.size >= this.maxSize){
            Iterator<String> it = this.cache.keySet().iterator();
            Index tobeRemove = this.cache.get(it.next());
            this.size -= tobeRemove.size();
            it.remove();
        }
        cache.put(word, i);
        this.size += i.size();
    }
}
