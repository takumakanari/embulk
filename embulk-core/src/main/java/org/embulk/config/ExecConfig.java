package org.embulk.config;

public class ExecConfig {

    private ConfigSource configSource;

    private ExecEvent execEvent;

    public ExecConfig(ConfigSource configSource, ExecEvent execEvent)
    {
        this.configSource = configSource;
        this.execEvent = execEvent;
    }

    public ExecConfig(ConfigSource configSource)
    {
        this(configSource, null);
    }

    public ConfigSource getConfigSource()
    {
        return configSource;
    }

    public ExecEvent getExecEvent()
    {
        return execEvent;
    }

}
