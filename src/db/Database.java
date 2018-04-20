package db;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by White Noise on 2017/2/22.
 */
public class Database {
    public static HashMap<String, Table> memory;

    public Database() {
        memory = new HashMap<>();
    }
    public String transact(String query) throws IOException {
        String[] queries = new String[1];
        queries[0] = query;
        return Parse.main(queries);
    }

    /**public static void main(String[] args) throws IOException{
        Database db = new Database();
        String line =  "load t1";
        System.out.print(db.transact(line));
        line =  "print t1";
        System.out.print(db.transact(line));

        line = "load t4";
        System.out.print(db.transact(line));
        line = "select * from t1,t4";
        System.out.print(db.transact(line));
    }*/
}
