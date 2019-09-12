package simpledb;

import java.util.*;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int numberOfBuckets;
    private int min;
    private int max;
    private int widthOfBucket;
    private int[] bucketValues;
    private int totalNumberOfTuples; 

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.numberOfBuckets = buckets;
        this.min = min;
        this.max = max;
        this.widthOfBucket = (int) (Math.ceil((double) (max - min + 1) / buckets));
        this.bucketValues = new int[buckets];
        this.totalNumberOfTuples = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int bucketNumber = this.getBucketNumber(v);
        bucketValues[bucketNumber]++;
        totalNumberOfTuples++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        int bucketNumber = this.getBucketNumber(v);
        // Get the left and right endpoint of the bucket
        int leftEndpointOfBucket = min + (bucketNumber * widthOfBucket);
        int rightEndpointOfBucket = min + ((bucketNumber + 1) * widthOfBucket);
        // Get the height of the bucket
        int heightOfBucket = bucketValues[bucketNumber];
        switch (op) {
            case LIKE:
            case EQUALS:
                return getEqualsSelectivity(v, heightOfBucket, widthOfBucket);
            case GREATER_THAN:
                return getGreaterThanSelectivity(v, bucketNumber, heightOfBucket, widthOfBucket, rightEndpointOfBucket);
            case LESS_THAN:
                return getLessThanSelectivity(v, bucketNumber, heightOfBucket, widthOfBucket, leftEndpointOfBucket);
            case GREATER_THAN_OR_EQ:
                if (v <= min) {
                    return 1.0;
                }
                if (v > max) {
                    return 0.0;
                }
                double greaterThanOrEqualSelectivity = getGreaterThanSelectivity(v, bucketNumber, heightOfBucket, widthOfBucket, rightEndpointOfBucket);
                greaterThanOrEqualSelectivity += (((double) heightOfBucket / widthOfBucket) / totalNumberOfTuples);
                return greaterThanOrEqualSelectivity;
            case LESS_THAN_OR_EQ:
                if (v >= max) {
                    return 1.0;
                }
                if (v < min) {
                    return 0.0;
                }
                double lessThanOrEqualSelectivity = getLessThanSelectivity(v, bucketNumber, heightOfBucket, widthOfBucket, leftEndpointOfBucket);
                lessThanOrEqualSelectivity += (((double) heightOfBucket / widthOfBucket) / totalNumberOfTuples);
                return lessThanOrEqualSelectivity;
            case NOT_EQUALS:
                return 1.0 - getEqualsSelectivity(v, heightOfBucket, widthOfBucket);
            default:
                break;
        }
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }

    private int getBucketNumber(int v) {
        int distanceToMin = Math.abs(v - min); 
        int bucketNumber = distanceToMin / widthOfBucket;
        if (bucketNumber >= this.numberOfBuckets) {
            bucketNumber = this.numberOfBuckets - 1;
        }
        return bucketNumber;
    }

    private double getEqualsSelectivity(int v, int heightOfBucket, int widthOfBucket) {
        if (v < min || v > max) {
            return 0.0;
        }
        return ((double) heightOfBucket / widthOfBucket) / totalNumberOfTuples;
    }

    private double getGreaterThanSelectivity(int v, int bucketNumber, int heightOfBucket, int widthOfBucket, int rightEndpoint) {
        if (v < min) {
            return 1.0;
        }
        if (v > max) {
            return 0.0;
        }
        double fractionOfTotalTuples = (double) heightOfBucket / totalNumberOfTuples;
        double fractionOfBucketGreaterThanConstant = ((double) rightEndpoint - v) / widthOfBucket;
        double selectivity = fractionOfTotalTuples * fractionOfBucketGreaterThanConstant;
        for (int i = bucketNumber + 1; i < numberOfBuckets; i++) {
            int currentHeightOfBucket = bucketValues[i];
            selectivity += (double) currentHeightOfBucket / totalNumberOfTuples;
        }
        return selectivity;
    }

    private double getLessThanSelectivity(int v, int bucketNumber, int heightOfBucket, int widthOfBucket, int leftEndpoint) {
        if (v > max) {
            return 1.0;
        }
        if (v < min) {
            return 0.0;
        }
        double fractionOfTotalTuples = (double) heightOfBucket / totalNumberOfTuples;
        double fractionOfBucketLessThanConstant = ((double) v - leftEndpoint) / widthOfBucket;
        double selectivity = fractionOfTotalTuples * fractionOfBucketLessThanConstant;
        for (int i = bucketNumber - 1; i >= 0; i--) {
            int currentHeightOfBucket = bucketValues[i];
            selectivity += (double) currentHeightOfBucket / totalNumberOfTuples;
        }
        return selectivity;
    }
}
