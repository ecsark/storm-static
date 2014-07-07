package storm.blueprint.util;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * User: ecsark
 * Date: 7/7/14
 * Time: 8:25 PM
 */
public class ResultWriter {

    private static ResultWriter _instance = null;

    private ResultWriter () {
        try {
            writers = new HashMap<String, PrintWriter>();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        open = new HashMap<String, Boolean>();
    }

    private Map<String, PrintWriter> writers;
    private Map<String, Boolean> open;

    private static ResultWriter instance() {
        if (_instance == null)
            _instance = new ResultWriter();
        return _instance;
    }

    public synchronized static void write(String file, String content) {
        Map<String, PrintWriter> ws = instance().writers;
        Map<String, Boolean> op = instance().open;
        if (!ws.containsKey(file)) {
            try {
                File f = new File("~/Desktop/"+file+".txt");
                if (!f.exists())
                    f.createNewFile();
                ws.put(file, new PrintWriter(f));
                op.put(file, true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        if (op.get(file))
            ws.get(file).write(content);
    }

    public static void close(String file) {

        PrintWriter pw = instance().writers.get(file);

        if (pw != null) {
            pw.close();
            instance().open.put(file, false);
        }
    }

    public static void closeAll() {
        for (PrintWriter pw : instance().writers.values())
            pw.close();
        for (String f : instance().open.keySet())
            instance().open.put(f, false);

    }
}
