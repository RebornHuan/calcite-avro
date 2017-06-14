package my.skypiea.punygod.adapter;

import org.apache.calcite.linq4j.Enumerator;

import static java.sql.Types.NULL;

/**
 * Created by wanghuan on 2017/6/13.
 */
public class AvroEnumerator implements Enumerator<Object[]> {
    boolean flag = true;
    private int currntId = 1;

    @Override
    public Object[] current() {
        System.out.println("current");
        Object[] test = {"wanghuan1", "", "", NULL};
        return test;
    }

    @Override
    public boolean moveNext() {
        System.out.println("moveNext");
        if (flag) {
            flag = false;
            return true;
        }
        return false;
    }

    @Override
    public void reset() {

    }

    @Override
    public void close() {

    }
}
