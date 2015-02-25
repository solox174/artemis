package net.disbelieve.artemis.utils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by kmatth002c on 1/20/2015.
 */
public class ModelUtils {
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
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        fields = originalClass.getDeclaredFields();

        for (Field field : fields) {
            try {
                fieldName = field.getName();
                firstChar = Character.toUpperCase(fieldName.charAt(0));
                upperCaseFieldName = firstChar + fieldName.substring(1);
                setter = copyClass.getMethod("set" + upperCaseFieldName, String.class);
                getter = originalClass.getMethod("get" + upperCaseFieldName);
                setter.invoke(copy, getter.invoke(original));
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        return copy;
    }
}
