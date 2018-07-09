package com.mkyong;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import io.debezium.connector.oracle.util.TestHelper;

public class OracleJDBCExample {

    private static final String QUERY_LOGMNR_CONTENTS =
            "SELECT scn, commit_scn, username, operation, sql_redo " +
            "FROM v$logmnr_contents " +
            "WHERE " +
                "src_con_name = ?" +
                "AND seg_owner <> 'SYS'" +
                "AND seg_owner <> 'SYSTEM'" +
                "AND (commit_scn > ? OR scn > ?)";

    public static void main(String[] argv) {
        String pdb = "ORCLPDB1";
        String schema = "DEBEZIUM";

        try {

            Class.forName("oracle.jdbc.driver.OracleDriver");

        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }


        Connection connection = null;

        try {
            connection = DriverManager.getConnection(
                    "jdbc:oracle:thin:@localhost:1521:ORCLCDB", TestHelper.CONNECTOR_USER, "xs");

        }
        catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return;
        }

        try {
            long startScn = getCurrentScn(connection);
            long lastCommitScn = -1;
            long lastScn = -1;

            System.out.println("build");

            PreparedStatement queryLogManagerContents = connection.prepareStatement(QUERY_LOGMNR_CONTENTS);

            CallableStatement s1 = connection.prepareCall("BEGIN DBMS_LOGMNR_D.BUILD (options => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;");
            s1.execute();
            s1.close();

            System.out.println("build done");
            for(int i = 1; i <= 1000; i++) {
                long endScn = getCurrentScn(connection);
                System.out.println("\n#" + i + " start logminer (start: " + startScn + ", end: " + endScn + ")");


                CallableStatement s = connection.prepareCall("BEGIN " +
                        "dbms_logmnr.start_logmnr(" +
                        "startScn => '" + startScn + "', " +
                        "endScn => '" + endScn + "', " +
                        "OPTIONS => DBMS_LOGMNR.dict_from_redo_logs + " +
                        "DBMS_LOGMNR.DDL_DICT_TRACKING + " +
                        "DBMS_LOGMNR.COMMITTED_DATA_ONLY + " +
                        "DBMS_LOGMNR.CONTINUOUS_MINE);" +
                        "END;");
                s.execute();

//                System.out.println("Logs:");
//                ResultSet res1 = s.executeQuery("select log_id, filename, low_scn from v$logmnr_logs order by low_scn desc");
//                while(res1.next()) {
//                    System.out.println(res1.getObject(1) + " " + res1.getObject(2) + " " + res1.getObject(3));
//                }

                System.out.println("query v$logmnr_contents (commit_scn > " + lastCommitScn + ", scn > " + lastScn + ")");


                queryLogManagerContents.setString(1, pdb);
                queryLogManagerContents.setLong(2, lastCommitScn);
                queryLogManagerContents.setLong(3, lastScn);

                ResultSet res = queryLogManagerContents.executeQuery();

                while(res.next()) {
                    long scn = res.getLong(1);
                    long commitScn = res.getLong(2);
                    String operation = res.getString(4);
                    String sql = res.getString(5);

                    // DDL
                    if (commitScn == 0 && scn > lastScn) {
                        System.out.println("DDL " + scn + " - " + operation + " - " + sql);
                        lastScn = scn;
                    }
                    // DDL
                    else if (commitScn > lastCommitScn) {
                        System.out.println("DML " + scn + " - " + operation + " - " + sql);
                        lastCommitScn = commitScn;
                    }
                }
                res.close();

                long nextStartScn = getLogStartScn(lastCommitScn, connection);

                if (nextStartScn > -1) {
                    startScn = nextStartScn;
                }

                res.close();

                s.close();
            }

        }
        catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static long getLogStartScn(long lastCommitScn, Connection connection) throws SQLException {
        if (lastCommitScn == -1) {
            return -1;
        }

        System.out.println("getting first scn of online log");

        Statement s = connection.createStatement();
        ResultSet res = s.executeQuery("select first_change# from V$LOG where status = 'CURRENT'");
        res.next();
        long firstScnOfOnlineLog = res.getLong(1);
        res.close();

        if (firstScnOfOnlineLog <= lastCommitScn) {
            return firstScnOfOnlineLog;
        }

        System.out.println("getting first scn of archived log");

        res = s.executeQuery("select first_change# from v$archived_log order by NEXT_CHANGE# desc");
        long firstScnOfArchivedLog = -1;
        while (res.next()) {
            firstScnOfArchivedLog = res.getLong(1);;
            if (firstScnOfArchivedLog <= lastCommitScn) {
                break;
            }
        }
        res.close();

        System.out.println("done getting first scn of archived log");

        return firstScnOfArchivedLog;
    }

    private static long getCurrentScn(Connection connection) throws SQLException {
        Statement s = connection.createStatement();
        ResultSet res = s.executeQuery("select CURRENT_SCN from V$DATABASE");
        res.next();
        long scn = res.getLong(1);
        res.close();
        return scn;
    }

}
