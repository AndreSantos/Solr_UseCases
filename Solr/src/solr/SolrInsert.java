package solr;

// Compile: javac -cp .:lib/ojdbc6.jar SolrInsert.java
// Run:     java -Xmx1536M -Xms1024M -cp .:lib/ojdbc6.jar SolrInsert 2000 1000 25 5 5 5 S 300 25 2 > LOG

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.String;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.index.TermVectorMapper;
import org.apache.lucene.search.IndexSearcher;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.getopt.luke.IndexInfo;


public class SolrInsert {
    static int total = 0;
    static int ind = 0;
    
    static float tam_medio = 0;
    static String [] palavras = new String[23000];
    static private long start;
    static private long end;
    static private long total_commit_time, total_add_time;

    static private int mode = 0;   // 0 - Human Readable / 1 - Time, Size

    static String dictionary_path, index_path, configuration_path;
    static int nbatches, nregisters, ntextfields;
    static int storedfields, notindexed;
    static int maxram, mergefactor;
    static int fieldlength;
    static int intfields, floatfields;

    // Server
    static CommonsHttpSolrServer server;
    static int variable;

    static boolean slow;
    static int MAX_TIME = 700000;
    
    public static void lerPalavras() {
        File file = new File(dictionary_path);
        FileInputStream fis = null;
        BufferedInputStream bis = null;
        DataInputStream dis = null;

        try {
            fis = new FileInputStream(file);

            // Here BufferedInputStream is added for fast reading.
            bis = new BufferedInputStream(fis);
            dis = new DataInputStream(bis);
            
            while (dis.available() != 0) {
                palavras[total++] = dis.readLine();
                tam_medio += palavras[total-1].length();
            }
            tam_medio /= total;
            
            // dispose all the resources after using them.
            fis.close();
            bis.close();
            dis.close();
        } catch (FileNotFoundException e) {
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void insertdata(CommonsHttpSolrServer server) throws SolrServerException, IOException, Exception {
        // Clean up existent index
        server.deleteByQuery("*:*");
        server.commit();
        // System.out.println("\n\nCleaned up SolrInsert index.\n");

        
        String str1 = "_tn"; //"_text_not_indexed";
        String str2 = "_ts"; //"_text_stored";

        for (int b = 1;b <= nbatches; b++) {
            int conta = 1;
            int text_data_size = 0;
            int non_text_data_size = 0;
            total_commit_time   = 0;
            total_add_time      = 0;
            
            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
            for (int k = 0;k < nregisters; k++) {
                SolrInputDocument doc = new SolrInputDocument();
                doc.addField("id", nregisters * b + k);

                // Text fields
                    // Not indexed
                for (int l = 0;l < notindexed; l++) {
                    String word = tune_string_by_size();
                    doc.addField(name(l) + str1, word);
                }

                    // Indexed
                for (int l = 0;l < ntextfields - notindexed; l++) {
                    String word = tune_string_by_size();
                    doc.addField(name(l) + str2, word);
                }
                text_data_size += fieldlength * ntextfields;

                // Int fields
                for (int l = 0;l < intfields; l++) {
                    int n = (int) (Math.random() * 9000.0f);
                    n += 1000;
                    doc.addField(name(l) + "_i", n);
                    non_text_data_size += String.valueOf(n).length();
                }
                
                // Float fields
                for (int l = 0;l < floatfields; l++) {
                    float n = (float) (Math.random() * 9000.0f);
                    n += 1000.0f;
                    doc.addField(name(l) + "_f", n);
                    non_text_data_size += String.valueOf(n).length();
                }
                
                docs.add( doc );
            }
            
            try {
                start = System.currentTimeMillis();
                server.add(docs);
                end = System.currentTimeMillis();
                total_add_time += end - start;

                start = System.currentTimeMillis();
                server.commit();
                end = System.currentTimeMillis();
                total_commit_time += end - start;
            }
            catch(Exception e) {
                conta = 0;
                System.out.println("Erro: " + e.getMessage());
                Thread.sleep(5000);
            }
            if (conta == 0) {
                b--;
                continue;
            }
            double solr_size = 0;
            if (b % 10 == 0 || variable > 0)
                solr_size = measure_index_size();
            String inc_s = String.valueOf(solr_size / 1024);
            inc_s = inc_s.replace(".", ",");
            if (total_add_time + total_commit_time > MAX_TIME)
                slow = true;
            if (mode == 2 && variable <= 0)
                System.out.println(b + ";" + (total_add_time + total_commit_time) + ";" + (text_data_size + non_text_data_size) + ";" + inc_s);
            if (mode == 2 && variable == 1)
                System.out.println(nregisters + ";" + (total_add_time + total_commit_time) + ";" + (text_data_size + non_text_data_size) + ";" + inc_s);
            if (mode == 2 && variable == 2)
                System.out.println(ntextfields + ";" + (total_add_time + total_commit_time) + ";" + (text_data_size + non_text_data_size) + ";" + inc_s);
            if (mode == 2 && variable == 3)
                System.out.println(intfields + ";" + (total_add_time + total_commit_time) + ";" + (text_data_size + non_text_data_size) + ";" + inc_s);
            if (mode == 2 && variable == 4)
                System.out.println((ntextfields - notindexed) + ";" + (total_add_time + total_commit_time) + ";" + (text_data_size + non_text_data_size) + ";" + inc_s);
        }
    }

    private static String tune_string_by_size() {
        ind = (ind + 1) % total;
        int len = palavras[ind].length();
        String word =  palavras[ind];
        while (len < fieldlength) {
            word += "-" + palavras[ind];
            len  += 1   + palavras[ind].length();
        }
        return word.substring(0, fieldlength);
    }

    int TIME = 5000000;
    
    // ============================== START FUNCTIONS ==================================
    public void just_insert(int b, int r) throws SolrServerException, IOException, Exception {
        CommonsHttpSolrServer server = new CommonsHttpSolrServer("http://localhost:8080/solr/");
        server.setConnectionTimeout(TIME);
        server.setMaxRetries(1);
        variable    = -1;
        nbatches    = b;
        nregisters  = r;

        //notindexed  = 0;
        //ntextfields  = 10;
        //fieldlength  = 10;

        insertdata(server);
    }
    public void insert_batches(int e) throws SolrServerException, IOException, Exception {
        CommonsHttpSolrServer server = new CommonsHttpSolrServer("http://localhost:8080/solr/");
        server.setConnectionTimeout(TIME);
        server.setMaxRetries(1);
        variable = 0;
        nbatches = e;
        nregisters = 1000;

        insertdata(server);
    }
    public void insert_registers(int b,int e, int sce) throws SolrServerException, IOException, Exception {
        CommonsHttpSolrServer server = new CommonsHttpSolrServer("http://localhost:8080/solr/");
        server.setConnectionTimeout(TIME);
        server.setMaxRetries(1);
        variable = 1;

        
        int con = 0;
        for (int k = b;k <= e;k += 500) {
            slow = false;
            nregisters = k;
            insertdata(server);
            if (slow == true) {
                con++;
                if (con >= 3)
                    break;
            }
            else
                con = 0;
        }
    }
    public void insert_text_fields(int e,int scenario) throws SolrServerException, IOException, Exception {
        CommonsHttpSolrServer server = new CommonsHttpSolrServer("http://localhost:8080/solr/");
        server.setConnectionTimeout(TIME);
        server.setMaxRetries(1);
        variable = 2;

        for (int k = 10;k <= e;k += 10) {
            ntextfields = k;
            switch (scenario) {
                case 1: notindexed = k - 1;     break;
                case 2: notindexed = k/2;       break;
                case 3: notindexed = 0;         break;
            }
            insertdata(server);
        }
    }
    public void insert_int_fields(int e,int scenario) throws SolrServerException, IOException, Exception {
        CommonsHttpSolrServer server = new CommonsHttpSolrServer("http://localhost:8080/solr/");
        server.setConnectionTimeout(TIME);
        server.setMaxRetries(1);
        variable = 3;

        for (int k = 10;k <= e;k += 10) {
            intfields = k;
            insertdata(server);
        }
    }
    public void insert_indexed_fields(int e) throws SolrServerException, IOException, Exception {
        CommonsHttpSolrServer server = new CommonsHttpSolrServer("http://localhost:8080/solr/");
        server.setConnectionTimeout(TIME);
        server.setMaxRetries(1);
        variable = 4;
        ntextfields = 990;
        notindexed = 990;
        nregisters = 250;

        for (int k = 10;k <= e;k += 10) {
            notindexed -= 10;
            insertdata(server);
        }
    }

    public SolrInsert(String [] args) throws FileNotFoundException, IOException {
        // Parameters
        nbatches      = Integer.parseInt(args[0]);
        nregisters    = Integer.parseInt(args[1]);
        ntextfields   = Integer.parseInt(args[2]);
        fieldlength   = Integer.parseInt(args[3]);
        intfields     = Integer.parseInt(args[4]);
        floatfields   = Integer.parseInt(args[5]);
        storedfields  = Integer.parseInt(args[6]);
        notindexed    = Integer.parseInt(args[7]);
        maxram        = Integer.parseInt(args[8]);
        mergefactor   = Integer.parseInt(args[9]);
        mode          = Integer.parseInt(args[11]);

        index_path          = "/opt/ptin/ach/apache-tomcat-6.0.32/bin/solr/data/index";
        configuration_path  = "/opt/ptin/ach/apache-tomcat-6.0.32/bin/solr/conf/solrconfig.xml";
        // Change solrconfig.xml
        if (mode != 2)
            change_config_file();

        // Dictionary
        dictionary_path = "dics/dic_" + args[10] + ".txt";
        lerPalavras();
    }

    private static String name(int l) {
        return String.valueOf((char) ('a' + (l / 23))) + String.valueOf((char) ('a' + (l % 23)));
    }

    private static double measure_index_size() throws Exception {
        IndexReader indexReader = IndexReader.open(index_path);
        IndexInfo indexinfo = new IndexInfo( indexReader, index_path);
        double size = indexinfo.getTotalFileSize();
        indexReader.close();
        // System.out.println("No terms: " + indexinfo.getNumTerms());
        return size;
    }

    private static void print_time(String beg, double time) {
        if (mode != 0)
            return;
        
        if (time > 1000 * 60) {
            int t = ((int)time) % 60000;
            System.out.printf("%s %dm %.2fs\n",beg, (int)(time / 60000), t / 1000.0);
        }
        else
        if (time > 1000)
            System.out.printf("%s %.2fs\n",beg, time / 1000);
        else
            System.out.printf("%s %.0fms\n",beg, time);
    }

    private static void print_size(String beg, double size) {
        if (mode != 0)
            return;
        
        if (size > 1024 * 1024)
            System.out.printf("%s %.2f Mb\n",beg, size / 1024 / 1024);
        else
            System.out.printf("%s %.2f Kb\n",beg, size / 1024);
    }


    private static void change_config_file() throws FileNotFoundException, IOException {
        BufferedReader input = new BufferedReader(new FileReader(configuration_path));
        List<String> contents = new ArrayList<String>();
        String line = null;
        while (( line = input.readLine()) != null)
            contents.add(line);
        input.close();
        
        BufferedWriter writer =  new BufferedWriter(new FileWriter(configuration_path));
        for (int k = 0;k < contents.size(); k++) {
            line = contents.get(k);
            if (line.contains("<mergeFactor>"))
                line = "    <mergeFactor>" + String.valueOf(mergefactor) + "</mergeFactor>";
            if (line.contains("<ramBufferSizeMB>"))
                line = "    <ramBufferSizeMB>" + String.valueOf(maxram) + "</ramBufferSizeMB>";
            writer.write(line + "\n");
        }
        writer.close();
    }

    private static void execute_command(String command) throws IOException, InterruptedException {
        Process ls_proc = Runtime.getRuntime().exec(command);
        ls_proc.waitFor();
        String ls_str;
        DataInputStream ls_in = new DataInputStream(ls_proc.getInputStream());
        try {
            while ((ls_str = ls_in.readLine()) != null)
                System.out.println(ls_str);
        }
        catch (IOException e) {  }
    }
}



                /*
                // Text fields
                // Stored
                    // Indexed
                for (int l = 0;l < storedfields; l++) {
                    int rand = (int) ( Math.random() * total );
                    doc.addField(name(l) + "_text_stored", palavras[rand]);
                    text_data_size += palavras[rand].length();
                }

                    // Not indexed
                for (int l = 0;l < notindexed; l++) {
                    int rand = (int) ( Math.random() * total );
                    doc.addField(name(l) + "_text_not_indexed", palavras[rand]);
                    text_data_size += palavras[rand].length();
                    non_indexed_data_size += palavras[rand].length();
                }


                // Not stored, But indexed
                for (int l = 0;l < ntextfields - storedfields - notindexed; l++) {
                    int rand = (int) ( Math.random() * total );
                    doc.addField(name(l) + "_text", palavras[rand]);
                    text_data_size += palavras[rand].length();
                    non_stored_data_size += palavras[rand].length();
                }
                */