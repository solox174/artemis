package com.comcast.artemis.cassandra.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by kmatth002c on 1/20/2015.
 */
public class ModelUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ModelUtils.class);

    public Object transcribe(Object original, Class copyClass) {
        Class originalClass = original.getClass();
        String fieldName, upperCaseFieldName;
        Field[] fields;
        Method setter, getter;
        Object copy = null;
        char firstChar;

        try {
            copy = copyClass.newInstance();
        } catch (InstantiationException e) {
            LOG.error("Could not create target class", e);
        } catch (IllegalAccessException e) {
            LOG.error("Could not create target class", e);
        }
        fields = originalClass.getDeclaredFields();

        for (Field field : fields) {
            try {
                fieldName = field.getName();
                firstChar = Character.toUpperCase(fieldName.charAt(0));
                upperCaseFieldName = firstChar + fieldName.substring(1);
                setter = copyClass.getMethod("set" + upperCaseFieldName, field.getType());
                getter = originalClass.getMethod("get" + upperCaseFieldName);
                setter.invoke(copy, getter.invoke(original));
            } catch (NoSuchMethodException e) {
                LOG.debug("No matching field found");
            } catch (InvocationTargetException e) {
                LOG.debug("Could not call getter for matching field");
            } catch (IllegalAccessException e) {
                LOG.debug("Could not call getter for matching field");
            }
        }

        return copy;
    }
}
