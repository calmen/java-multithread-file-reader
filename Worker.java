import java.io.*;
import java.net.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Worker extends Thread {

    private ObjectOutputStream outputStream;
    private ObjectInputStream inputStream;
    private Socket requestSocket;

    private String host;
    private int port;
    private String name;

    List<List<String>> elements = new ArrayList<>();
    List<List<String>> elemWpt = new ArrayList<>();
    List<List<String>> elemEle = new ArrayList<>();
    List<List<String>> elemTime = new ArrayList<>();

    List<String> wptID = new ArrayList<>();
    List<String> eleID = new ArrayList<>();
    List<String> timeID = new ArrayList<>();

    LinkedHashMap<String, List<String>> map = new LinkedHashMap<>();
    LinkedHashMap<String, String> finalMap = new LinkedHashMap<>();

    public Worker(String host, int port, String name) {
        this.host = host;
        this.port = port;
        this.name = name;
    }

    public void run() {
        try {

            /* Create socket for contacting the server*/
            requestSocket = new Socket(host, port);

            /* Create the streams to send and receive data from server */
            outputStream = new ObjectOutputStream((requestSocket.getOutputStream()));
            inputStream = new ObjectInputStream(requestSocket.getInputStream());

            System.out.println(name);

            boolean terminated = false;
            while (true) {
            while (!terminated) {
                Object inputObject = inputStream.readObject();

                // Check if termination message is received
                if (inputObject instanceof String && inputObject.equals("terminate")) {
                    terminated = true;
                } else if (inputObject instanceof String) {
                    elements.add(Collections.singletonList((String) inputObject));

                } else {
                    // Do something with the received message here
                    elements.add((List<String>) inputObject);
                }
            }
            System.out.println("terminated");
            splitDataType();
            giveIDs();
            fillLinkedHashMap();
            map();

            System.out.println("I gave" + finalMap);
            outputStream.writeObject(finalMap);
            outputStream.flush();
            outputStream.reset();

            clearValues();
            terminated = false;
            }
        } catch (IOException | ClassNotFoundException | ParseException e) {
            e.printStackTrace();
        }
    }

    private void splitDataType() {
        for (int i = 0; i < elements.size(); i++) {
            if (elements.get(i).get(0).length() > 10) {
                if (elements.get(i).get(0).charAt(4) == '-') {
                    elemTime.add(elements.get(i));
                } else {
                    elemWpt.add(elements.get(i));
                }
            } else {
                elemEle.add(elements.get(i));
            }
        }
    }

    private void giveIDs() {
        char front = name.charAt(6);
        int counter = 100;
        for (int i = 0; i < elemWpt.size(); i++) {
            wptID.add("w" + front + counter);
            counter += 1;
        }
        counter = 100;
        for (int i = 0; i < elemEle.size(); i++) {
            eleID.add("e" + front + counter);
            counter += 1;
        }
        counter = 100;
        for (int i = 0; i < elemTime.size(); i++) {
            timeID.add("t" + front + counter);
            counter += 1;
        }
        System.out.println(wptID + " wptID");
        System.out.println(eleID + " eleID");
        System.out.println(timeID + " timeID");
    }

    private void fillLinkedHashMap() {
        for (int i = 0; i < elemWpt.size(); i++) {
            String key = wptID.get(i);
            List<String> value = elemWpt.get(i);
            map.put(key, value);
        }

        for (int i = 0; i < elemEle.size(); i++) {
            String key = eleID.get(i);
            List<String> value = elemEle.get(i);
            map.put(key, value);
        }

        for (int i = 0; i < elemTime.size(); i++) {
            String key = timeID.get(i);
            List<String> value = elemTime.get(i);
            map.put(key, value);
        }
        System.out.println(map);
    }

    private void map() throws ParseException {
        String postfix;

        double totalDistance = 0.0;
        double prevLat = Double.parseDouble(map.get("w" + name.charAt(6) + "100").get(0));
        double prevLon = Double.parseDouble(map.get("w" + name.charAt(6) + "100").get(1));

        double totalElevation = 0.0;
        double elevationGained = 0.0;
        double prevElevation = Double.parseDouble(map.get("e" + name.charAt(6) + "100").get(0));
        double currentElevation = 0.0;

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        Date startDate = dateFormat.parse(map.get("t" + name.charAt(6) + "100").get(0));
        double totalTime = 0.0;

        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            if (entry.getKey().startsWith("w") && Integer.parseInt(entry.getKey().substring(entry.getKey().length() - 2)) % 6 == 0) {
                postfix = entry.getKey().substring(5 - 3);
                prevLat = Double.parseDouble(map.get("w" + name.charAt(6) + postfix).get(0));
                prevLon = Double.parseDouble(map.get("w" + name.charAt(6) + postfix).get(1));
            } else if (entry.getKey().startsWith("w") && !entry.getKey().endsWith("00")) {
                double lat = Double.parseDouble(entry.getValue().get(0));
                double lon = Double.parseDouble(entry.getValue().get(1));
                double distance = haversine(prevLat, prevLon, lat, lon);
                totalDistance += distance;
                prevLat = lat;
                prevLon = lon;
            } else if (entry.getKey().startsWith("e") && Integer.parseInt(entry.getKey().substring(entry.getKey().length() - 2)) % 6 == 0) {
                postfix = entry.getKey().substring(5 - 3);
                prevElevation = Double.parseDouble(map.get("e" + name.charAt(6) + postfix).get(0));
            } else if (entry.getKey().startsWith("e") && !entry.getKey().endsWith("00")) {
                currentElevation = Double.parseDouble(entry.getValue().get(0));
                if (prevElevation < currentElevation) {
                    elevationGained = currentElevation - prevElevation;
                    totalElevation += elevationGained;
                }
                prevElevation = currentElevation;
            } else if (entry.getKey().startsWith("t") && Integer.parseInt(entry.getKey().substring(entry.getKey().length() - 2)) % 6 == 0) {
                postfix = entry.getKey().substring(5 - 3);
                startDate = dateFormat.parse(map.get("t" + name.charAt(6) + postfix).get(0));
            } else if (entry.getKey().startsWith("t") && !entry.getKey().endsWith("00")) {
                Date endDate = dateFormat.parse(entry.getValue().get(0));
                double timeDiffMillis = endDate.getTime() - startDate.getTime();
                totalTime = totalTime + (double) (timeDiffMillis / (60 * 1000));
                startDate = endDate;
            }
        }
        System.out.println("total distance: " + totalDistance);
        System.out.println("total elevation: " + totalElevation);
        System.out.println("total time: " + totalTime);

        finalMap.put("w" + name, String.valueOf(totalDistance));
        finalMap.put("e" + name, String.valueOf(totalElevation));
        finalMap.put("t" + name, String.valueOf(totalTime));
    }

    private double haversine(double lat1, double lon1, double lat2, double lon2) {
        double R = 6371.0; // Earth radius in kilometers
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                        Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double d = R * c;
        return d;
    }

    public void clearValues() {
        elements.clear();
        elemWpt.clear();
        elemEle.clear();
        elemTime.clear();
        wptID.clear();
        eleID.clear();
        timeID.clear();
        map.clear();
        finalMap.clear();
    }

    public static void main(String[] args) throws ParseException, IOException {

        /*To simulate several different pcs running run each worker instance in a different worker execution
        comment out the rest in each one. */

        Worker worker0 = new Worker("localhost", 5678, "worker0");
        Thread worker0Thread = new Thread(worker0);
        worker0Thread.start();

        Worker worker1 = new Worker("localhost", 5678, "worker1");
        Thread worker1Thread = new Thread(worker1);
        worker1Thread.start();

        Worker worker2 = new Worker("localhost", 5678, "worker2");
        Thread worker2Thread = new Thread(worker2);
        worker2Thread.start();


    }

}