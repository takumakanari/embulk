package org.embulk.jruby;


import org.embulk.config.ConfigDiff;
import org.embulk.config.ExecEvent;
import org.jruby.embed.ScriptingContainer;

public class JrubyExecEvent implements ExecEvent {

    private ScriptingContainer jruby;

    private Object dslObject;

    public JrubyExecEvent(ScriptingContainer jruby, Object dslObject)
    {
        this.jruby = jruby;
        this.dslObject = dslObject;
    }

    @Override
    public void onStart()
    {
        jruby.callMethod(dslObject, "exec_event", "start");
    }

    @Override
    public void onComplete(ConfigDiff configDiff)
    {
        jruby.callMethod(dslObject, "exec_event", "complete", configDiff);
    }

}
