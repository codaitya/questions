import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class ExternalSortTest {

    @Test
    public void testTransformAndSort() {
        ExternalSort externalSort = new ExternalSort( "test", "/tmp",10);
        List<String> lst = Arrays.asList("1436910236011,3,/item/32184"
                ,"1436910236012,2,/item/1","1436910236015,2,/item/2");
        List<String> ans = externalSort.transformAndSort(lst);
        Assert.assertEquals("2,/item/1", ans.get(0));
        Assert.assertEquals("2,/item/2", ans.get(1));
        Assert.assertEquals("3,/item/32184", ans.get(2));

    }

    @Test
    public void testFlushing()
            throws IOException {

        List<String> lst = Arrays.asList("2,/item/1","2,/item/2","3,/item/2");
        ExternalSort externalSort = new ExternalSort( "test", "/tmp",10);
        externalSort.flush (lst);
        try (BufferedReader br = new BufferedReader(new FileReader("/tmp/0.txt"))) {
            String str = br.readLine();
            Assert.assertEquals("2,/item/1", str);
            str = br.readLine();
            Assert.assertEquals("2,/item/2", str);
            str = br.readLine();
            Assert.assertEquals("3,/item/2", str);
        }
    }

    @Test
    public void testTemporaryFileCreation()
            throws IOException, InterruptedException {

        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter( "/tmp/input.txt"))) {
                bufferedWriter.write("1436910236012,2,/item/10");
                bufferedWriter.newLine();
                bufferedWriter.write("1436910236013,2,/item/1");
                bufferedWriter.newLine();
                bufferedWriter.write("1436910236014,3,/item/2");
        }

        ExternalSort externalSort = new ExternalSort( "/tmp/input.txt", "/tmp",2);
        externalSort.createTemporaryFiles();
        try (BufferedReader br = new BufferedReader(new FileReader("/tmp/0.txt"))) {
            String str = br.readLine();
            Assert.assertEquals("2,/item/1", str);
            str = br.readLine();
            Assert.assertEquals("2,/item/10", str);
        }
        try (BufferedReader br = new BufferedReader(new FileReader("/tmp/1.txt"))) {
            String str = br.readLine();
            Assert.assertEquals("3,/item/2", str);
        }
    }

    @Test
    public void testUniqueUsers()
            throws IOException, InterruptedException {

        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter( "/tmp/input.txt"))) {
            bufferedWriter.write("1436910236012,2,/item/10");
            bufferedWriter.newLine();
            bufferedWriter.write("1436910236013,2,/item/1");
            bufferedWriter.newLine();
            bufferedWriter.write("1436910236014,3,/item/2");
            bufferedWriter.newLine();
            bufferedWriter.write("1436910236014,4,/item/2");
            bufferedWriter.newLine();
            bufferedWriter.write("1436910236014,5,/item/1");
            bufferedWriter.newLine();
            bufferedWriter.write("1436910236014,5,/item/1");
        }

        ExternalSort externalSort = new ExternalSort( "/tmp/input.txt", "/tmp", 2);
        externalSort.writeUsersWhoVisitNDistinctPath(2);

        try (BufferedReader br = new BufferedReader(new FileReader("/tmp/out.txt"))) {
            String str = br.readLine();
            Assert.assertEquals("2", str);
        }
    }

    @Test
    //test on provided data
    public void testOnAccessLog()
            throws IOException, InterruptedException {
        int unique = 25;

        long t1 = System.currentTimeMillis();
        ExternalSort externalSort = new ExternalSort("./src/test/resources/access.log", "/tmp/mapr", 100);
        externalSort.writeUsersWhoVisitNDistinctPath(unique);

        List<String> generated = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader("/tmp/mapr/out.txt"))) {
            String str = null;
            while ( (str = br.readLine()) != null ) {
                generated.add(str);
            }
        }
        System.out.println("External sorting  Time taken(seconds) " +  (System.currentTimeMillis() - t1));

        //input at "https://static.imply.io/takehome/access.log.gz"
        t1 = System.currentTimeMillis();
        List<String> expected = findUnique("./src/test/resources/access.log", unique);
        System.out.println("BruteForce Time taken(seconds) " +  (System.currentTimeMillis() - t1));
        Assert.assertEquals(expected, generated);
    }

    private List<String> findUnique(java.lang.String path, int N)
            throws IOException {

        Map<String, Set<String>> mp  = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String str = null;
            while ( (str = br.readLine()) != null ) {
                String [] ar  = str.split(",");
                if (!mp.containsKey(ar[1])) {
                    mp.put(ar[1], new HashSet<>());
                }
                mp.get(ar[1]).add(ar[2]);
            }
        }
        List<String> ans = new ArrayList<>();
        for (String key : mp.keySet()) {
            if (mp.get(key).size() >= N) {
                ans.add(key);
            }
        }
        Collections.sort(ans);
        return ans;
    }

    @Test
    @Ignore
    //test on large data
    public void testOnLargeData()
            throws IOException, InterruptedException {
        int unique = 25;

        List<String> data = readFileToList("./src/test/resources/access.log");
        System.out.println(data.size());
        int k = 50;
        //write k times
        try (BufferedWriter br = new BufferedWriter(new FileWriter("/tmp/access_big.txt"))) {
            for (int i = 1; i <= k; i++) {
                System.out.println(i);
                for (int j = 0; j < data.size(); j++ ) {
                    br.write(data.get(j));
                    if ( i ==k && j == data.size() - 1) {
                        ;
                    }
                    else {
                        br.newLine();
                    }
                }
            }
        }

        long t1 = System.currentTimeMillis();
        ExternalSort externalSort = new ExternalSort("/tmp/access_big.txt", "/tmp/mapr", 10000000);
        externalSort.writeUsersWhoVisitNDistinctPath(unique);
        System.out.println("External Sort Time taken(seconds) " +  (System.currentTimeMillis() - t1));
    }

     private List<String> readFileToList(String path)
             throws IOException {
        List<String> contents = new ArrayList<>();
         try (BufferedReader br = new BufferedReader(new FileReader(path))) {
             String str = null;
             while ( (str = br.readLine()) != null ) {
                    contents.add(str);
                 }
         }
         return contents;
    }
}
