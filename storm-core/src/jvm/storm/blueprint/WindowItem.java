package storm.blueprint;

import java.io.Serializable;

/**
 * User: ecsark
 * Date: 5/31/14
 * Time: 9:41 PM
 */
public class WindowItem implements Serializable {
    String id;
    int windowLength;
    int pace;
    boolean emitting = true;

    WindowItem(String id, int windowLength, int pace) {
        this.id = id;
        this.windowLength = windowLength;
        this.pace = pace;
    }

    void setEmitting (boolean emitting) {
        this.emitting = emitting;
    }

    boolean isEmitting () {
        return emitting;
    }

    @Override
    public String toString() {
        return id +": "+Integer.toString(windowLength)+"/"+Integer.toString(pace);
    }
}