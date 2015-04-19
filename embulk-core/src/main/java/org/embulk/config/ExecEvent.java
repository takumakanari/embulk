package org.embulk.config;

public interface ExecEvent {

    public void onStart();

    public void onComplete(ConfigDiff configDiff);

}
