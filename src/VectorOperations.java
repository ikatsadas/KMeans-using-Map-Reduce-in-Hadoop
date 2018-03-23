import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import java.util.ArrayList;

/**
 * Class to implement operations between N-dimensional points
 * @author Katsadas Ioannis
 * @author Michailidou Anna Valentini
 */
public class VectorOperations {

    /**
     * Function to find distance between two points
     * @param a float[] first point
     * @param b float[] second point
     * @return distance
     */
    public double distance(float[] a, float[] b) {
        double diff_square_sum = 0.0;//Distance
        //Euclidean Distance
        for (int i = 0; i < a.length; i++) {
            diff_square_sum += (a[i] - b[i]) * (a[i] - b[i]);
        }
        return Math.sqrt(diff_square_sum);
    }

    /**
     * Function finding a center among N points
     * @param cluster ArrayList of points
     * @param dimensions Number of dimensions
     * @return center arrray
     */
    public double[] center(ArrayList<WordInfoWritable> cluster, int dimensions) {
        double[] centers = new double[dimensions];//New center
        for (int i = 0; i < dimensions; i++) {
            centers[i] = 0;
        }
        int size = 0;//Number of points
        int num = 0;//Dimension counter
        IntArrayWritable array = new IntArrayWritable();

        //Iterate through points
        for (WordInfoWritable val : cluster) {

            array = val.getArray();//Get the position

            //Iterate through dimensions in position
            for (Writable writable : array.get()) {
                //Get value
                IntWritable intWritable = (IntWritable) writable;
                int value = intWritable.get();
                centers[num] = centers[num] + value;//Add value to the sum
                num++;
            }
            num = 0;//Set dimension to 0
            size++;
        }

        //Get the average
        for (int i = 0; i < dimensions; i++) {
            centers[i] = centers[i] / size;
        }
        return centers;

    }
}
