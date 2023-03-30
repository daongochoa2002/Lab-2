import java.nio.file.Paths;   
import java.nio.file.Files;
import java.nio.file.Path; 
public class Driver{
    public static void main(String[] args) throws Exception{
        FindAllNeighbour findAllNeighbour = new FindAllNeighbour();
        MinimumNeighbour minimumNeighbour = new MinimumNeighbour();
        CountDistinct countDistinct = new CountDistinct();

        // job1 find the relationship
        String[] job1Args = {args[0],args[1]};
        findAllNeighbour.main(job1Args);
        //job2 label the result
        String[] job2Args = {args[1],args[2]};
        minimumNeighbour.main(job2Args);

        String[] job3Args = {args[2],args[3]};
        countDistinct.main(job3Args);
        

    }
}