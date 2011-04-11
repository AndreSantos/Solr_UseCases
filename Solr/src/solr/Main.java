package solr;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.solr.client.solrj.SolrServerException;


public class Main {
    static String use_case, variable;
    static int scenario;
    public static void main(String[] args) throws FileNotFoundException, IOException, SolrServerException, Exception {
        if (args.length != 2) {
            System.out.println("usage: java -Xmx1536M -Xms1024M <use_case> <variable> <scenario>");
            return;
        }
        
        use_case = args[0];
        variable = args[1];
        scenario = Integer.valueOf(args[2]);

        if (use_case.contentEquals("insert")) {
            insert();
        }

    }

    private static void insert() throws FileNotFoundException, IOException, SolrServerException, Exception {
        String [] vec = new String[30];
        vec[0] = "1";                       // Batches
        vec[1] = "";                        // Registers
        vec[2] = "30";                      // Text Fields
        vec[3] = "20";                      //      Field Length
        vec[4] = "5";                       //      Int Fields
        vec[5] = "0";                       //      Float Length    (=0)
        vec[6] = "30";                      // Stored Length        (=All)

        switch (scenario) {                 // Not indexed fields
            case 1: vec[7] = "29"; break;
            case 2: vec[7] = "15"; break;
            default: vec[7] = "0";
        }

        vec[8] = "300";                     // Max RAM
        vec[9] = "25";                      // Merge factor
        vec[10] = "S";                      // Dictionary
        vec[11] = "2";                      // Mode

        Solr solr = new Solr(vec);
        int beg = 1000;
        int end = 150000;
        
        solr.start_insert(beg, end);
    }

}
