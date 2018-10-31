package examples;


import traceability.Artifact;

import java.sql.Timestamp;
import java.util.*;

public class TestBase {
    public void TestBase() {

    }

    public List<Artifact> getArtifacts() {
        Artifact a1 = new Artifact("a1");
        Artifact a2 = new Artifact("a2");

        a1.addAttribute("content", "content for a1");
        a2.addAttribute("timeStamp", String.valueOf(new Timestamp(new Date().getTime())));

        return Arrays.asList(a1, a2);
    }
}
