import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * Class to create a new type that implements Writable.
 * It holds an IntWritable.
 * @author Katsadas Ioannis 
 * @author Michailidou Anna Valentini 
 */
public class IntArrayWritable extends ArrayWritable {

    public IntArrayWritable() {
        super(IntWritable.class);
    }

    public IntArrayWritable(IntWritable[] values) {
        super(IntWritable.class, values);
        IntWritable[] ints = new IntWritable[values.length];
        for (int i = 0; i < values.length; i++) {
            ints[i] = new IntWritable(values[i].get());
        }
        set(ints);
    }

    
    public IntWritable[] g() {
        return (IntWritable[]) super.get();
    }

    @Override
    public String toString() {
        IntWritable[] values = g();
        String s = "[" + values[0].toString() + ", ";
        for (int i = 1; i < values.length - 1; i++) {
            s += values[i].toString() + ", ";
        }
        return s + values[values.length - 1].toString() + "]";
    }
}
