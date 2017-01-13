package chapter04;

import scala.collection.immutable.List;

/**
 * Created by Administrator on 2016/8/22 0022.
 */
public class MySQLDAOImpl implements MySQLDAO{
    @Override
    public boolean delete(String id) {
        return false;
    }

    @Override
    public boolean add(Object o) {
        return false;
    }

    @Override
    public int update(Object o) {
        return 0;
    }

    @Override
    public List<Object> query(String id) {
        return null;
    }
}
