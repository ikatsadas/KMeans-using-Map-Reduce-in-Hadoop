import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Class to create a new type that implements Writable.
 * It holds a Text which is the word and an IntArrayWritable which is the position.
 * @author Katsadas Ioannis
 * @author Michailidou Anna Valentini
 */
public class WordInfoWritable implements Writable {

    private Text word;
    private IntArrayWritable array;

    //Constructor
    public WordInfoWritable() {
        this.word = new Text();
        this.array = new IntArrayWritable();
    }

    //Setter/Getters
    
    public void set(Text a, IntArrayWritable b) {
        this.word.set(a);
        this.array = b;
    }

    public Text getWord() {
        return word; }

    public IntArrayWritable getArray() {
        return array; }

    //Methods used from Hadoop 
    @Override
    public void readFields(DataInput in) throws IOException {
        word.readFields(in);
        array.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        word.write(out);
        array.write(out);
    }

}

