package storm.blueprint.util;

import java.util.Properties;

/**
 * User: ecsark
 * Date: 6/6/14
 * Time: 4:03 PM
 */
public class SystemInfoTest {
        public static void main(String[] a) {
            Properties sysProps = System.getProperties();
            sysProps.list(System.out);
        }
    }
