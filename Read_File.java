import java.io.Serializable;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Read_File implements Serializable {

    ArrayList<String> wpt = new ArrayList<>();
    ArrayList<String> ele = new ArrayList<>();
    ArrayList<String> time = new ArrayList<>();

    String creator;

    public void setCreator(String creator) {
        this.creator = creator;
    }

    public String getCreator() {
        return creator;
    }

    public void ReadFile(String fileContent) {

        Pattern creatorPattern = Pattern.compile("creator=\"(.*?)\"");
        Matcher creatorMatcher = creatorPattern.matcher(fileContent);

        while (creatorMatcher.find()) {
            setCreator(creatorMatcher.group(1));
        }

        Pattern wptPattern = Pattern.compile("<wpt lat=\"(.*?)\" lon=\"(.*?)\">");
        Matcher wptMatcher = wptPattern.matcher(fileContent);

        Pattern elePattern = Pattern.compile("<ele>(.*?)</ele>");
        Matcher eleMatcher = elePattern.matcher(fileContent);

        Pattern timePattern = Pattern.compile("<time>(.*?)</time>");
        Matcher timeMatcher = timePattern.matcher(fileContent);

        while (wptMatcher.find()) {
            wpt.add(wptMatcher.group(1));
            wpt.add(wptMatcher.group(2));
        }
        while (eleMatcher.find()) {
            ele.add(eleMatcher.group(1));
        }

        while (timeMatcher.find()) {
            time.add(timeMatcher.group(1));
        }

        // Print the extracted data

    }


}