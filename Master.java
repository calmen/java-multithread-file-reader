import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.*;

public class Master extends Thread {

    private ServerSocket serverSocketApp;
    private ServerSocket serverSocketWorker;

    private Socket clientSocketApp;
    private Socket clientSocketWorker0;
    private Socket clientSocketWorker1;
    private Socket clientSocketWorker2;

    private ObjectInputStream inputStreamApp;
    private ObjectInputStream inputStreamWorker0;
    private ObjectInputStream inputStreamWorker1;
    private ObjectInputStream inputStreamWorker2;

    private ObjectOutputStream outputStreamApp;
    private ObjectOutputStream outputStreamWorker0;
    private ObjectOutputStream outputStreamWorker1;
    private ObjectOutputStream outputStreamWorker2;

    public int input = 1;
    private static boolean ready = false;
    private String fileContentString;
    private Read_File file = new Read_File();

    private final int chunkSize = 5;

    private ArrayList<Worker> workers = new ArrayList<>();
    public int workerNumber = 3;

    List<List<String>> workerWpt = new ArrayList<>();
    List<String> workerEle = new ArrayList<>();
    List<String> workerTime = new ArrayList<>();

    Map<String, String> finalMap = Collections.synchronizedMap(new HashMap<>());

    double totalDistance = 0.0d;
    double totalElevation = 0.0d;
    double totalTime = 0.0d;
    double averageSpeed = 0.0d;
    String creator;

    public List<List<String>> statistics = new ArrayList<>();


    //****************** SERVER PART **************

    public void run() {

        try {
            openServerApp();
            openServerWorker();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Thread app = new Thread(() ->
        {
            while (true) {
                try {
                    waitForClientApp();
                    handleClientApp();
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        });

        Thread worker0 = new Thread(() ->
        {
            while (true) {
                try {
                    waitForClientWorker0();
                    handleClientWorker0();
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        });

        Thread worker1 = new Thread(() ->
        {
            while (true) {
                try {
                    waitForClientWorker1();
                    handleClientWorker1();
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        });

        Thread worker2 = new Thread(() ->
        {
            while (true) {
                try {
                    waitForClientWorker2();
                    handleClientWorker2();
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        });

        app.start();
        worker0.start();
        worker1.start();
        worker2.start();

    }


    private void openServerApp() throws IOException {
        serverSocketApp = new ServerSocket(1234);
        System.out.println("Server App started on port 1234");
    }

    private void openServerWorker() throws IOException {
        serverSocketWorker = new ServerSocket(5678);
        System.out.println("Server Worker started");
    }

    private void waitForClientApp() throws IOException {
        clientSocketApp = serverSocketApp.accept();
        System.out.println("Client connected from " + clientSocketApp.getInetAddress());
        outputStreamApp = new ObjectOutputStream(clientSocketApp.getOutputStream());
        inputStreamApp = new ObjectInputStream(clientSocketApp.getInputStream());
    }

    private void waitForClientWorker0() throws IOException {
        clientSocketWorker0 = serverSocketWorker.accept();
        System.out.println("Client connected from " + clientSocketWorker0.getInetAddress());
        outputStreamWorker0 = new ObjectOutputStream(clientSocketWorker0.getOutputStream());
        inputStreamWorker0 = new ObjectInputStream(clientSocketWorker0.getInputStream());
    }

    private void waitForClientWorker1() throws IOException {
        clientSocketWorker1 = serverSocketWorker.accept();
        System.out.println("Client connected from " + clientSocketWorker1.getInetAddress());
        outputStreamWorker1 = new ObjectOutputStream(clientSocketWorker1.getOutputStream());
        inputStreamWorker1 = new ObjectInputStream(clientSocketWorker1.getInputStream());
    }

    private void waitForClientWorker2() throws IOException {
        clientSocketWorker2 = serverSocketWorker.accept();
        System.out.println("Client connected from " + clientSocketWorker2.getInetAddress());
        outputStreamWorker2 = new ObjectOutputStream(clientSocketWorker2.getOutputStream());
        inputStreamWorker2 = new ObjectInputStream(clientSocketWorker2.getInputStream());
    }

    private void handleClientApp() throws IOException, ClassNotFoundException {
        try {
            while (input == 1) {
                // Read the data sent by the client
                byte[] fileContent = (byte[]) inputStreamApp.readObject();

                // Convert the byte array to string
                fileContentString = new String(fileContent);

                // Print the file content
                System.out.println("Received file content:");

                input = inputStreamApp.readInt();

                //inputStreamApp.reset();
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void handleClientWorker0() throws IOException, ClassNotFoundException {
        // Send the file variable to the client
        while (true) {
            while (!ready) {
                System.out.print("");
            }

            int start = (workerNumber - 3) * chunkSize;
            int size = workerWpt.size();
            boolean notFirst = false;

            while (start < size) {
                int end = Math.min(start + chunkSize, size);
                if (notFirst) {
                    outputStreamWorker0.writeObject(workerWpt.get(start - 1));
                    outputStreamWorker0.flush();
                    outputStreamWorker0.writeObject(workerEle.get(start - 1));
                    outputStreamWorker0.flush();
                    outputStreamWorker0.writeObject(workerTime.get(start - 1));
                    outputStreamWorker0.flush();
                } else {
                    outputStreamWorker0.writeObject(workerWpt.get(start));
                    outputStreamWorker0.flush();
                    outputStreamWorker0.writeObject(workerEle.get(start));
                    outputStreamWorker0.flush();
                    outputStreamWorker0.writeObject(workerTime.get(start));
                    outputStreamWorker0.flush();
                }

                for (int i = start; i < end; i++) {
                    outputStreamWorker0.writeObject(workerWpt.get(i));
                    outputStreamWorker0.flush();
                    outputStreamWorker0.writeObject(workerEle.get(i));
                    outputStreamWorker0.flush();
                    outputStreamWorker0.writeObject(workerTime.get(i));
                    outputStreamWorker0.flush();
                }

                start = end + (workerNumber - 1) * chunkSize;
                notFirst = true;
            }

            String message = "terminate";
            outputStreamWorker0.writeObject(message);
            outputStreamWorker0.flush();

            Object inputObject = inputStreamWorker0.readObject();
            System.out.println(inputObject.toString() + "I received nothing");
            HashMap<String, String> temp;
            temp = (HashMap<String, String>) inputObject;
            for (HashMap.Entry<String, String> entry : temp.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                finalMap.put(key, value);
            }
            temp.clear();
            ready = false;
        }
    }

    private void handleClientWorker1() throws IOException, ClassNotFoundException {
        // Send the file variable to the client
        while (true) {
            while (!ready) {
                System.out.print("");
            }

            int start = (workerNumber - 2) * chunkSize;
            int size = workerWpt.size();
            boolean notFirst = false;

            while (start < size) {
                int end = Math.min(start + chunkSize, size);
                //if (notFirst) {
                outputStreamWorker1.writeObject(workerWpt.get(start - 1));
                outputStreamWorker1.flush();
                outputStreamWorker1.writeObject(workerEle.get(start - 1));
                outputStreamWorker1.flush();
                outputStreamWorker1.writeObject(workerTime.get(start - 1));
                outputStreamWorker1.flush();
                //}
                for (int i = start; i < end; i++) {
                    outputStreamWorker1.writeObject(workerWpt.get(i));
                    outputStreamWorker1.flush();
                    outputStreamWorker1.writeObject(workerEle.get(i));
                    outputStreamWorker1.flush();
                    outputStreamWorker1.writeObject(workerTime.get(i));
                    outputStreamWorker1.flush();
                }

                start = end + (workerNumber - 1) * chunkSize;
                notFirst = true;

            }

            String message = "terminate";
            outputStreamWorker1.writeObject(message);
            outputStreamWorker1.flush();

            Object inputObject = inputStreamWorker1.readObject();
            System.out.println(inputObject.toString() + "I received nothing");
            HashMap<String, String> temp;
            temp = (HashMap<String, String>) inputObject;
            for (HashMap.Entry<String, String> entry : temp.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                finalMap.put(key, value);
            }
            temp.clear();
            ready = false;
        }
    }

    private void handleClientWorker2() throws IOException, ClassNotFoundException {
        // Send the file variable to the client
        while (true) {
            while (!ready) {
                System.out.print("");
            }

            int start = (workerNumber - 1) * chunkSize;
            int size = workerWpt.size();
            boolean notFirst = false;

            while (start < size) {
                int end = Math.min(start + chunkSize, size);
                //if (notFirst) {
                outputStreamWorker2.writeObject(workerWpt.get(start - 1));
                outputStreamWorker2.flush();
                outputStreamWorker2.writeObject(workerEle.get(start - 1));
                outputStreamWorker2.flush();
                outputStreamWorker2.writeObject(workerTime.get(start - 1));
                outputStreamWorker2.flush();
                //}
                for (int i = start; i < end; i++) {
                    outputStreamWorker2.writeObject(workerWpt.get(i));
                    outputStreamWorker2.flush();
                    outputStreamWorker2.writeObject(workerEle.get(i));
                    outputStreamWorker2.flush();
                    outputStreamWorker2.writeObject(workerTime.get(i));
                    outputStreamWorker2.flush();
                }

                start = end + (workerNumber - 1) * chunkSize;   //+ (workerNumber -1) * chunkSize;
                notFirst = true;

            }

            String message = "terminate";
            outputStreamWorker2.writeObject(message);
            outputStreamWorker2.flush();

            Object inputObject = inputStreamWorker2.readObject();
            System.out.println(inputObject.toString() + "I received nothing");
            HashMap<String, String> temp;
            temp = (HashMap<String, String>) inputObject;
            for (HashMap.Entry<String, String> entry : temp.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                finalMap.put(key, value);
            }
            temp.clear();
            ready = false;
        }
    }


    public void closeServers() {
        try {
            if (inputStreamApp != null) {
                inputStreamApp.close();
            }
            if (outputStreamApp != null) {
                outputStreamApp.close();
            }
            if (clientSocketApp != null) {
                clientSocketApp.close();
            }
            if (inputStreamWorker0 != null) {
                inputStreamWorker0.close();
            }
            if (outputStreamWorker0 != null) {
                outputStreamWorker0.close();
            }
            if (clientSocketWorker0 != null) {
                clientSocketWorker0.close();
            }
            if (inputStreamWorker1 != null) {
                inputStreamWorker1.close();
            }
            if (outputStreamWorker1 != null) {
                outputStreamWorker1.close();
            }
            if (clientSocketWorker1 != null) {
                clientSocketWorker1.close();
            }
            if (inputStreamWorker2 != null) {
                inputStreamWorker2.close();
            }
            if (outputStreamWorker2 != null) {
                outputStreamWorker2.close();
            }
            if (clientSocketWorker2 != null) {
                clientSocketWorker2.close();
            }
            if (serverSocketApp != null) {
                serverSocketApp.close();
            }
            if (serverSocketWorker != null) {
                serverSocketWorker.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Server closed");
    }


    //*************** EXTRACT DATA FROM GPX PART ************

    public void ReadData() {
        file.ReadFile(fileContentString);
        creator = file.getCreator();
    }

    public void Fragmentation() {

        if (file.wpt.size() % 2 == 0) {
            for (int i = 0; i < file.wpt.size(); i = i + 2) {
                List<String> temp = new ArrayList<>();
                temp.add(file.wpt.get(i));
                temp.add(file.wpt.get(i + 1));
                workerWpt.add(temp);
            }
        }

        workerEle.addAll(file.ele);
        workerTime.addAll(file.time);

        ready = true;
    }


    public void Reduction() {
        for (HashMap.Entry<String, String> entry : finalMap.entrySet()) {
            if (entry.getKey().startsWith("w")) {
                totalDistance = totalDistance + Double.parseDouble(entry.getValue());
            } else if (entry.getKey().startsWith("e")) {
                totalElevation = totalElevation + Double.parseDouble(entry.getValue());
            } else {
                totalTime = totalTime + Double.parseDouble(entry.getValue());
            }
        }
        averageSpeed = (totalDistance) / (totalTime/60);
        System.out.println("Total Distance: " + totalDistance);
        System.out.println("Total Elevation: " + totalElevation);
        System.out.println("Total Time: " + totalTime);
        System.out.println("Average Speed: " + averageSpeed);
        System.out.println("Creator: " + file.getCreator());

    }

    public void FillStatistics() {
        List<String> temp = new ArrayList<>();
        temp.add(creator);
        temp.add(String.valueOf(totalDistance));
        temp.add(String.valueOf(totalElevation));
        temp.add(String.valueOf(totalTime));
        temp.add(String.valueOf(averageSpeed));
        statistics.add(temp);
    }

    public double TotalD() {
        double d = 0.0d;
        double c = 0.0d;
        for (List<String> list : statistics) {
            if (list.get(0).equals(creator)) {
                c = c + 1.0d;
                d += Double.parseDouble(list.get(1));
            }
        }
        return d/c;
    }

    public double TotalE() {
        double e = 0.0d;
        double c = 0.0d;
        for (List<String> list : statistics) {
            if (list.get(0).equals(creator)) {
                c = c + 1.0d;
                e += Double.parseDouble(list.get(2));
            }
        }
        return e/c;
    }

    public double TotalT() {
        double t = 0.0d;
        double c = 0.0d;
        for (List<String> list : statistics) {
            if (list.get(0).equals(creator)) {
                c = c + 1.0d;
                t += Double.parseDouble(list.get(3));
            }
        }
        return t/c;
    }

    public double TotalS() {
        double s = 0.0d;
        double c = 0.0d;
        for (List<String> list : statistics) {
            if (list.get(0).equals(creator)) {
                c = c + 1.0d;
                s += Double.parseDouble(list.get(4));
            }
        }
        return s/c;
    }

    public double TotalDA() {
        double d = 0.0d;
        double c = 0.0d;
        for (List<String> list : statistics) {
                c = c + 1.0d;
                d += Double.parseDouble(list.get(1));
        }
        return d/c;
    }

    public double TotalEA() {
        double e = 0.0d;
        double c = 0.0d;
        for (List<String> list : statistics) {
                c = c + 1.0d;
                e += Double.parseDouble(list.get(2));
        }
        return e/c;
    }

    public double TotalTA() {
        double t = 0.0d;
        double c = 0.0d;
        for (List<String> list : statistics) {
                c = c + 1.0d;
                t += Double.parseDouble(list.get(3));
        }
        return t/c;
    }

    public double TotalSA() {
        double s = 0.0d;
        double c = 0.0d;
        for (List<String> list : statistics) {
                c = c + 1.0d;
                s += Double.parseDouble(list.get(4));
        }
        return s/c;
    }

    public void Results() throws IOException {

        outputStreamApp.writeObject(creator);
        outputStreamApp.flush();
        outputStreamApp.writeObject(totalDistance);
        outputStreamApp.flush();
        outputStreamApp.writeObject(totalElevation);
        outputStreamApp.flush();
        outputStreamApp.writeObject(totalTime);
        outputStreamApp.flush();
        outputStreamApp.writeObject(averageSpeed);
        outputStreamApp.flush();

        outputStreamApp.writeObject(TotalD());
        outputStreamApp.flush();
        outputStreamApp.writeObject(TotalE());
        outputStreamApp.flush();
        outputStreamApp.writeObject(TotalT());
        outputStreamApp.flush();
        outputStreamApp.writeObject(TotalS());
        outputStreamApp.flush();

        outputStreamApp.writeObject(TotalDA());
        outputStreamApp.flush();
        outputStreamApp.writeObject(TotalEA());
        outputStreamApp.flush();
        outputStreamApp.writeObject(TotalTA());
        outputStreamApp.flush();
        outputStreamApp.writeObject(TotalSA());
        outputStreamApp.flush();
    }

    public void ClearValues() {
        workerWpt.clear();
        workerEle.clear();
        workerTime.clear();
        finalMap.clear();
        totalDistance = 0.0d;
        totalElevation = 0.0d;
        totalTime = 0.0d;
        averageSpeed = 0.0d;
        creator = null;
        fileContentString = null;
        file.wpt.clear();
        file.ele.clear();
        file.time.clear();
    }

    public static void main(String[] args) throws IOException{

        Master master = new Master();
        Thread masterThread = new Thread(master);
        masterThread.start();


        while (master.input == 1) {

            while (master.fileContentString == null) {
                System.out.print("");
                if (master.input == 3)
                    System.exit(0);
            }

            master.ReadData();

            master.Fragmentation();
            System.out.println("test 1");

            while (master.finalMap.size() != master.workerNumber * 3) {
                System.out.print("");
            }
            System.out.println("test 2");
            master.Reduction();
            System.out.println(master.finalMap);

            master.FillStatistics();
            System.out.println(master.statistics);

            master.Results();

            master.ClearValues();

        }

        //master.closeServers();
        System.exit(0);
    }

}