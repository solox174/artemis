package com.comcast.artemis.exception;

/**
 * Created by kmatth207 on 4/30/2015.
 */
public class ResultAccessException extends Exception {
    public ResultAccessException(Exception e) {
      super(e);
    }

    public ResultAccessException(String s) {
        super(new Exception(s));
    }

    public ResultAccessException(Exception e, String s) {
        super(s, e);
    }
}
