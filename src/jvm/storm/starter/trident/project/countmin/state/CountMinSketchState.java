/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package storm.starter.trident.project.countmin.state;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import java.util.Arrays;
import java.util.Random;


import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;

import java.util.Collections;

import java.util.Comparator;
import java.util.PriorityQueue;

import storm.starter.trident.project.countmin.state.TweetWord;

import java.io.Serializable;

import java.io.*;

//import com.clearspring.analytics.stream.membership.Filter;
//import Filter;

/**
 * Count-Min Sketch datastructure.
 * An Improved Data Stream Summary: The Count-Min Sketch and its Applications
 * http://www.eecs.harvard.edu/~michaelm/CS222/countmin.pdf
 * Modified by Preetham MS. Originally by https://github.com/addthis/stream-lib/
 * @author: Preetham MS (pmahish@ncsu.edu)
 */
public class CountMinSketchState implements State, Serializable {

    public static final long PRIME_MODULUS = (1L << 31) - 1;

    int depth;
    int width;
    long[][] table;
    long[] hashA;
    long size;
    double eps;
    double confidence;
    public static int topk_size = 10;

    class WordComparator implements Comparator<TweetWord> {

        public int compare(TweetWord a, TweetWord b) {
            if(a.count > b.count)
                return 1;
            else
                return -1;
        }
    }

    PriorityQueue<TweetWord> queue = new PriorityQueue<TweetWord>(10, new WordComparator());

    CountMinSketchState() {
        
    }

    public CountMinSketchState(int depth, int width, int seed, int topk_size) {
        this.depth = depth;
        this.width = width;
        this.eps = 2.0 / width;
        this.confidence = 1 - 1 / Math.pow(2, depth);
        this.topk_size = topk_size;
        initTablesWith(depth, width, seed);
    }

    public CountMinSketchState(double epsOfTotalCount, double confidence, int seed) {
        // 2/w = eps ; w = 2/eps
        // 1/2^depth <= 1-confidence ; depth >= -log2 (1-confidence)
        this.eps = epsOfTotalCount;
        this.confidence = confidence;
        this.width = (int) Math.ceil(2 / epsOfTotalCount);
        this.depth = (int) Math.ceil(-Math.log(1 - confidence) / Math.log(2));
        initTablesWith(depth, width, seed);
    }

    public CountMinSketchState(int depth, int width, int size, long[] hashA, long[][] table) {
        this.depth = depth;
        this.width = width;
        this.eps = 2.0 / width;
        this.confidence = 1 - 1 / Math.pow(2, depth);
        this.hashA = hashA;
        this.table = table;
        this.size = size;
    }

    private void initTablesWith(int depth, int width, int seed) {
        this.table = new long[depth][width];
        this.hashA = new long[depth];
        Random r = new Random(seed);
        // We're using a linear hash functions
        // of the form (a*x+b) mod p.
        // a,b are chosen independently for each hash function.
        // However we can set b = 0 as all it does is shift the results
        // without compromising their uniformity or independence with
        // the other hashes.
        for (int i = 0; i < depth; ++i) {
            hashA[i] = r.nextInt(Integer.MAX_VALUE);
        }
    }

    public double getRelativeError() {
        return eps;
    }

    public double getConfidence() {
        return confidence;
    }

    int hash(long item, int i) {
        long hash = hashA[i] * item;
        // A super fast way of computing x mod 2^p-1
        // See http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
        // page 149, right after Proposition 7.
        hash += hash >> 32;
        hash &= PRIME_MODULUS;
        // Doing "%" after (int) conversion is ~2x faster than %'ing longs.
        return ((int) hash) % width;
    }

    
    public void add(long item, long count) {
        if (count < 0) {
            // Actually for negative increments we'll need to use the median
            // instead of minimum, and accuracy will suffer somewhat.
            // Probably makes sense to add an "allow negative increments"
            // parameter to constructor.
            throw new IllegalArgumentException("Negative increments not implemented");
        }
        for (int i = 0; i < depth; ++i) {
            table[i][hash(item, i)] += count;
        }
        size += count;
    }

    
    public void add(String item, long count) {

        item = item.trim();
        if (count < 0) {
            // Actually for negative increments we'll need to use the median
            // instead of minimum, and accuracy will suffer somewhat.
            // Probably makes sense to add an "allow negative increments"
            // parameter to constructor.
            throw new IllegalArgumentException("Negative increments not implemented");
        }
        int[] buckets = Filter.getHashBuckets(item, depth, width);
        for (int i = 0; i < depth; ++i) {
            table[i][buckets[i]] += count;
        }
        size += count;
        //Maintaining the top K items in the PQueue
        if(item.length() > 0) {

            //Estimate the count
            long ct = estimateCount(item);
            TweetWord tw = new TweetWord(item, ct);
            tw.word = item;
            
            //If the PQueue already has the element, remove it
            if(queue.contains(tw)) {
                queue.remove(tw);
            }

            //Add the element to the PQueue
            queue.add(tw);

            //Remove all elements apart from the top K
            while(queue.size() > topk_size)
                queue.poll();
        }

    }

    
    public long size() {
        return size;
    }

    /**
     * The estimate is correct within 'epsilon' * (total item count),
     * with probability 'confidence'.
     */
    
    public long estimateCount(long item) {
        long res = Long.MAX_VALUE;
        for (int i = 0; i < depth; ++i) {
            res = Math.min(res, table[i][hash(item, i)]);
        }
        return res;
    }

    
    public long estimateCount(String item) {
        long res = Long.MAX_VALUE;
        int[] buckets = Filter.getHashBuckets(item, depth, width);
        for (int i = 0; i < depth; ++i) {
            res = Math.min(res, table[i][buckets[i]]);
        }
        return res;
    }

    /**
     * Merges count min sketches to produce a count min sketch for their combined streams
     *
     * @param estimators
     * @return merged estimator or null if no estimators were provided
     * @throws CMSMergeException if estimators are not mergeable (same depth, width and seed)
     */
    public static CountMinSketchState merge(CountMinSketchState... estimators) throws CMSMergeException {
        CountMinSketchState merged = null;
        if (estimators != null && estimators.length > 0) {
            int depth = estimators[0].depth;
            int width = estimators[0].width;
            long[] hashA = Arrays.copyOf(estimators[0].hashA, estimators[0].hashA.length);

            long[][] table = new long[depth][width];
            int size = 0;

            for (CountMinSketchState estimator : estimators) {
                if (estimator.depth != depth) {
                    throw new CMSMergeException("Cannot merge estimators of different depth");
                }
                if (estimator.width != width) {
                    throw new CMSMergeException("Cannot merge estimators of different width");
                }
                if (!Arrays.equals(estimator.hashA, hashA)) {
                    throw new CMSMergeException("Cannot merge estimators of different seed");
                }

                for(TweetWord tw : estimator.queue) {
                    if(merged.queue.contains(tw)) {
                        TweetWord merged_count = new TweetWord(tw.word, tw.count);
                        for(TweetWord temp : merged.queue) {
                            if(temp.equals(tw)) {
                                merged_count.count += temp.count;
                            }
                        }
                        merged.queue.remove(tw);
                        merged.queue.add(merged_count);
                    }
                }

                while(merged.queue.size() > topk_size)
                    merged.queue.poll();

                for (int i = 0; i < table.length; i++) {
                    for (int j = 0; j < table[i].length; j++) {
                        table[i][j] += estimator.table[i][j];
                    }
                }
                size += estimator.size;
            }

            merged = new CountMinSketchState(depth, width, size, hashA, table);
        }

        return merged;
    }

    /*
     * Made the sketch class serializable so that it can be converted to
     * a byte stream directly instead of field by field.
    */
    public static byte[] serialize(CountMinSketchState sketch) {

        ByteArrayOutputStream bos = null;
        ObjectOutputStream out = null;
        try {
            bos = new ByteArrayOutputStream();
            out = new ObjectOutputStream(bos);
            out.writeObject(sketch);
            return bos.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(bos != null) {
                try {
                    bos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        
        return null;
    }

    /*
     * Deserialize method for the sketch. Since the class is serializable
     * we can directly convert the data stream to an object.
    */
    public static CountMinSketchState deserialize(byte[] data) {

        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
 
        try{
            bis = new ByteArrayInputStream(data);
            ois = new ObjectInputStream(bis);
            CountMinSketchState sketch = (CountMinSketchState)ois.readObject();
            return  sketch;
        } catch(IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            if(bis != null) {
                try {
                    bis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(ois!= null) {
                try {
                    ois.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    @Override
    public void beginCommit(Long txid) {
        return;
    }

    @Override
    public void commit(Long txid) {
        return;
    }

    @SuppressWarnings("serial")
    protected static class CMSMergeException extends RuntimeException {
   // protected static class CMSMergeException extends FrequencyMergeException {

        public CMSMergeException(String message) {
            super(message);
        }
    }
}
