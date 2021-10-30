import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

public class ExternalSort {

    //no of records to write to each temporary file
    private int bufferSize;

    private String inputFilePath;

    private String workspacePath;

    private int tmpfileNumber;

    public ExternalSort(final String inputFilePath, final String workspacePath, final int bufferSize) {
        this.bufferSize = bufferSize;
        this.inputFilePath = inputFilePath;
        this.workspacePath = workspacePath;
    }
    public ExternalSort(String inputFilePath) {
        this.bufferSize = 10000000;
        this.inputFilePath = inputFilePath;
        this.workspacePath = "./";
    }
    public ExternalSort(final String inputFilePath, final String workspacePath) {
        this.bufferSize = 10000000;
        this.inputFilePath = inputFilePath;
        this.workspacePath = workspacePath;
    }

    private static class Holder {
        private BufferedReader bufferedReader;
        private String user;
        private String resource;

        private Holder(final BufferedReader bufferedReader, final String user, final String resource) {
            this.bufferedReader = bufferedReader;
            this.user = user;
            this.resource = resource;
        }

        private BufferedReader getBufferedReader() {
            return bufferedReader;
        }

        private String getUser() {
            return user;
        }

        private String getResource() {
            return resource;
        }
    }

    private Holder formOneHolder(BufferedReader bufferedReader, String str) {
        String [] top = str.split(",");
        String user = top[0];
        String resource = top[1];
        return new Holder(bufferedReader, user, resource );
    }

    private void initializeFromTempFiles(PriorityQueue<Holder> holders)
            throws IOException {
        for (int i = 0; i < tmpfileNumber; i++) {
            BufferedReader br = new BufferedReader(new FileReader(workspacePath + "/" + i + ".txt"));
            String str = br.readLine();
            Holder h = formOneHolder(br, str);
            holders.add(h);
        }
    }

    public void writeUsersWhoVisitNDistinctPath(int n)
            throws IOException {

        createTemporaryFiles();
        PriorityQueue<Holder> holders = new PriorityQueue<>((h1,h2)->{
            if (h1.getUser().equals(h2.getUser())) {
                return h1.getResource().compareTo(h2.getResource());
            }
            else {
                return h1.getUser().compareTo(h2.getUser());
            }

        });

        initializeFromTempFiles(holders);


        String prevUser = null;
        String prevResource= null;
        int uniqResourceCountcount  = 0;
        System.out.println("Will write output to " + workspacePath  + "out.txt");
        BufferedWriter out = new BufferedWriter(new FileWriter(workspacePath + "/" + "out.txt"));

        int c = 0;

        while (holders.size() > 0) {
            Holder h = holders.poll();
            if(! h.getUser().equals(prevUser)) {
                // we have a new user
                //check earlier and write if count greater than n
                if (prevUser != null) {
                    if (uniqResourceCountcount >= n) {
                        out.write(prevUser);
                        out.newLine();
                    }
                }
                uniqResourceCountcount = 1;
            }
            else  {
                //if resources are not equal
                if (!h.getResource().equals(prevResource)) {
                    uniqResourceCountcount ++;
                }
            }
            //update prev user and resource
            prevUser = h.user;
            prevResource = h.resource;

            //write next
            BufferedReader br= h.getBufferedReader();
            String str = br.readLine();
            if (str == null) {
                br.close();
            }
            else {
                Holder holder = formOneHolder(br, str);
                holders.add(holder);
            }
        }

        //write what remains
        if (uniqResourceCountcount >= n ) {
            out.write(prevUser);
        }
        out.close();

    }

    public void createTemporaryFiles()
            throws IOException {


        BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFilePath));
        List<String> holder =  new ArrayList<>();
        String str;
        while ( (str = bufferedReader.readLine()) != null) {
            holder.add(str);
            if (holder.size() == bufferSize) {
                List<String> sortedAndTransformed = transformAndSort(holder);
                flush(sortedAndTransformed);
                tmpfileNumber ++;
                holder.clear();
                System.out.println("Temp file " + tmpfileNumber + " created");
            }
        }
        if (holder.size() > 0 ) {
            List<String> sortedAndTransformed = transformAndSort(holder);
            flush(sortedAndTransformed);
            tmpfileNumber ++;
        }

    }

    protected List<String> transformAndSort(List<String> list) {
        // transform list to contain only userid and path
        List<Holder> transformed = new ArrayList<>();
        for (String str : list) {
            String [] ar = str.split(",");
            transformed.add(new Holder(null, ar[1], ar[2]));
        }
        transformed.sort((c1, c2) -> {
            if(c1.getUser().equals(c2.getUser())) {
                //if users are equal sort by resource
                return c1.getResource().compareTo(c2.getResource());
            }
            else {
                return c1.getUser().compareTo(c2.getUser());
            }
        });
        return transformed.stream().map(h -> h.getUser() + "," + h.getResource()).collect(Collectors.toList());
    }

    protected void flush(List<String> lst)
            throws IOException {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(workspacePath + "/" + tmpfileNumber + ".txt"))) {
            for (int i =0; i < lst.size(); i++) {
                bufferedWriter.write(lst.get(i));
                if (i != lst.size() - 1) {
                    bufferedWriter.newLine();
                }
            }
        }
    }

    public static void main(String[] args)
            throws IOException {
        if (args.length < 2 ) {
            throw new IllegalArgumentException("Please provide input file and unique visits");
        }
        else if (args.length == 2) {
            System.out.println("Provided input file path is " + args[0]);
            ExternalSort externalSort = new ExternalSort(args[0]);
            externalSort.writeUsersWhoVisitNDistinctPath(Integer.parseInt(args[1]));
        }
        else if (args.length == 3) {
            System.out.println("Provided input file path is " + args[0]);
            System.out.println("Provided workspace path is " + args[2]);
            ExternalSort externalSort = new ExternalSort(args[0], args[2]);
            externalSort.writeUsersWhoVisitNDistinctPath(Integer.parseInt(args[1]));
        }
        else if (args.length == 4) {
            System.out.println("Provided input file path is " + args[0]);
            System.out.println("Provided workspace path is " + args[2]);
            System.out.println("Num records per file " + args[3]);
            ExternalSort externalSort = new ExternalSort(args[0], args[2], Integer.parseInt(args[3]));
            externalSort.writeUsersWhoVisitNDistinctPath(Integer.parseInt(args[1]));
        }
        else {
            throw new IllegalArgumentException("Got " + args.length + " arguments " + "Max permissible is 4");
        }
    }

}
