package com.comcast.artemis.cassandra.data;

import com.comcast.artemis.exception.ResultAccessException;
import com.comcast.x1.crypt.CryptUtil;
import com.datastax.driver.core.ResultSet;
import com.google.common.base.Optional;
import org.eclipse.jetty.util.BlockingArrayQueue;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;

/**
 * Created by kmatth002c on 4/7/2015.
 */
public class Result<T> {
    private BlockingQueue mappedResultQueue = new BlockingArrayQueue();
    private BlockingQueue unmappedResultQueue = new BlockingArrayQueue();
    private Optional mappedOptional;
    private Optional unmappedOptional;

    public void setError(Throwable error) {
        mappedResultQueue.add(error);
        unmappedResultQueue.add(error);
    }

    /**
     * Gets result.
     *
     * @return the result. The result will either be a mapped object, or list of objects, of type T or an exception,
     * which is thrown.
     * <p>
     * fields annotated with @Secure in mapped objects will be decrypted before return
     * <p>
     * This method is not thread safe. Once a result is consumed, subsequent requests to this method will throw an
     * Exception stating "Element already consumed".
     * @throws com.comcast.artemis.exception.ResultAccessException the mapped result access exception
     */
    public T getMappedResult() throws ResultAccessException {
        Object obj =  getResult(mappedOptional, mappedResultQueue);

        if(obj != null) {
            return (T)obj;
        }
        return null;
    }

    public void setMappedResult(T mappedResult) {
        Optional optional = (Optional) Optional.fromNullable(mappedResult);
        mappedResultQueue.add(optional);
    }

    /**
     * Gets result.
     *
     * @return the unmapped result as a ResultSet.
     * This method is not thread safe. Once a result is consumed, subsequent requests to this method will throw an
     * Exception stating "Element already consumed".
     * @throws com.comcast.artemis.exception.ResultAccessException the mapped result access exception
     */
    public ResultSet getUnmappedResult() throws ResultAccessException {
        Object obj =  getResult(unmappedOptional, unmappedResultQueue);

        if(obj != null) {
            return (ResultSet)obj;
        }
        return null;
    }
    
    public void setUnmappedResultSet(ResultSet unmappedResult) {
        Optional optional = (Optional) Optional.fromNullable(unmappedResult);
        unmappedResultQueue.add(optional);
    }

    private Object getResult(Optional optional, BlockingQueue resultQueue) throws  ResultAccessException {
        if (optional == null) {
            try {
                Object obj = resultQueue.take();

                if(obj instanceof Exception) {
                    throw new ResultAccessException((Exception)obj);
                } else {
                    if(((Optional)obj).isPresent()) {
                        return ((Optional)obj).get();
                    } else {
                        return null;
                    }
                }
            } catch (InterruptedException e) {
                throw new ResultAccessException(e);
            }
        } else {
            throw new ResultAccessException(new NoSuchElementException("Element already consumed"));
        }
    }
}
