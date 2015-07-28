package com.comcast.artemis.exception;

/**
 * Created by kmatth207 on 4/30/2015.
 */
public class ConnectionException extends Exception {
    public ConnectionException(Exception e) {
      super(e);
    }

    public ConnectionException(String s) {
        super(new Exception(s));
    }

    public ConnectionException(Exception e, String s) {
        super(s, e);
    }
}
