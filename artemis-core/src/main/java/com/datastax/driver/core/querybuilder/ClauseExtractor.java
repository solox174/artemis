package com.datastax.driver.core.querybuilder;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Set;

/**
 * Created by kmatth207 on 7/20/15.
 */
public class ClauseExtractor {
    public static Object getValues(Clause clause) {
        Object values = null;
        if(clause instanceof Clause.InClause) {
            try {
                Field inValuesField = Clause.InClause.class.getDeclaredField("values");
                inValuesField.setAccessible(true);

                values =  inValuesField.get(clause);
            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        } else {
            values = clause.firstValue();
        }
        return values;
    }
}
