import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.solr.client.solrj.SolrServerException;
import solr.SolrInsert;
import solr.SolrSearch;


public class Main_Solr {
    static String use_case;
    static int scenario, variable;
    static int var;
    public static void main(String[] args) throws FileNotFoundException, IOException, SolrServerException, Exception {
        if (args.length < 3) {
            System.out.println("usage: java -Xmx1536M -Xms1024M -cp .:lib/*: Main_Solr <use_case> <variable> <scenario> [var1]");
            return;
        }
        
        use_case = args[0];
        variable = Integer.valueOf(args[1]);
        scenario = Integer.valueOf(args[2]);
        if (args.length > 3)
            var  = Integer.valueOf(args[3]);


        if (use_case.contentEquals("insert"))
            insert();
        if (use_case.contentEquals("search"))
            search();
    }

    private static void insert() throws FileNotFoundException, IOException, SolrServerException, Exception {
        String [] vec = new String[30];
        vec[0] = "1";                       // Batches
        vec[1] = "250";                     // Registers
        vec[2] = "20";                      // Text Fields
        vec[3] = "20";                      //      Field Length
        vec[4] = "5";                       //      Int Fields
        vec[5] = "0";                       //      Float Fields    (=0)
        vec[6] = "30";                      // Stored               (=All)

        switch (scenario) {                 // Not indexed fields
            case 1:  vec[7] = "19"; break;
            case 2:  vec[7] = "10"; break;
            default: vec[7] = "0";
        }

        vec[8] = "300";                     // Max RAM
        vec[9] = "25";                      // Merge factor
        vec[10] = "S";                      // Dictionary
        vec[11] = "2";                      // Mode

        SolrInsert solr = new SolrInsert(vec);

        int beg, end;
        switch (variable) {
            case -1:                    // Just insert
                beg = var;          // batches
                end = 5000;         // registers
                solr.just_insert(beg, end);
            break;
            case 0:                                 // Batches
                switch (scenario) {
                    case 1:  end = 2500; break;
                    case 2:  end = 2000; break;
                    default: end = 1500;
                }
                solr.insert_batches(end);
            break;
            case 1:                                 // Registers
                beg = 1000;
                end = 150000;
                solr.insert_registers(beg, end, scenario);
            break;
            case 2:                                 // Text Fields
                end = 990;      // Limite de colunas do Oracle = 1000
                solr.insert_text_fields(end, scenario);
            break;
            case 3:                                 // Int Fields
                end = 970;      // Limite de colunas do Oracle = 1000
                solr.insert_int_fields(end, scenario);
            break;
            case 4:                                 // Indexed Fields
                end = 990;      // Limite de colunas do Oracle = 1000
                solr.insert_indexed_fields(end);
            break;
        }
    }

    private static void search() throws FileNotFoundException, SolrServerException, IOException, Exception {
        String [] vec = new String[30];
        vec[0] = "500";          // Ops per sec
        vec[1] = "10";           // Num Threads
        vec[2] = "20";           // Num Words    - Deprecated
        vec[3] = "10";           // Seconds
        vec[4] = "10";           // Field Length
        vec[5] = "S";            // Dictionary

        switch (scenario) {                 // Not indexed fields
            case 1:  vec[6] = "19"; break;
            case 2:  vec[6] = "10"; break;
            default: vec[6] = "0";
        }

        vec[7] = "10";            // Resultados
        
        SolrSearch solr = new SolrSearch(vec);


        switch (variable) {
            case 0:
                int seconds = 10;
                solr.search_seconds(seconds);
                break;
            case 1:
                int threads = 200;
                solr.search_threads(threads);
                break;
            case 2:
                int ops = 2000;
                solr.search_opspersec(ops);
                break;
            case 3:
                int res = 250;
                solr.search_results(res);
                break;
        }
    }

}
