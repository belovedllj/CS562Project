import java.sql.*;
import java.util.*;

public class EsqlQuery {
    private Map<String, MFStructure> mapOfMFStructure = new LinkedHashMap<String, MFStructure>();
    private ResultSet getResultSet() throws Exception {
        String usr = "postgres";
        String pwd = "cherishtina";
        String url = "jdbc:postgresql://localhost:5432/postgres";

        PreparedStatement pstmt;
        Class.forName("org.postgresql.Driver");
        Connection conn = DriverManager.getConnection(url, usr, pwd);
        pstmt = conn.prepareStatement("select * from sales");
        return pstmt.executeQuery();
    }

    public static void main(String[] args) {
        try {
            EsqlQuery esqlQuery = new EsqlQuery();
            esqlQuery.makeDatabaseQuery();
            esqlQuery.outprintTable();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void makeDatabaseQuery() throws Exception {
        ResultSet rs0 = getResultSet();
        while(rs0.next()){
            String cust = rs0.getString("cust");
            String keyValue ="";
            keyValue += cust;
            if (this.mapOfMFStructure.containsKey(keyValue)) {
            	MFStructure mf = this.mapOfMFStructure.get(keyValue);
            } else {
            	MFStructure mf = new MFStructure();
            	mf.cust = cust;
                this.mapOfMFStructure.put(keyValue, mf);
            }
        }
        Set<String> keyValueSet1 = new HashSet<String>();
        ResultSet rs1 = getResultSet();
        while(rs1.next()){
            String cust = rs1.getString("cust");
            String keyValue ="";
            keyValue += cust;
            int quant = rs1.getInt("quant");
            int year = rs1.getInt("year");
            String prod = rs1.getString("prod");
            if (year == 1991 && prod.equals("Eggs")) {
                MFStructure mf = this.mapOfMFStructure.get(keyValue);
                if (keyValueSet1.contains(keyValue)) {
                    mf.num1_sum_quant += quant;
                    mf.num1_count_quant += 1;
                } else {
                    mf.num1_sum_quant = quant;
                    mf.num1_count_quant = 1;
                    keyValueSet1.add(keyValue);
                }
            }
        }
        for (String keyValue : this.mapOfMFStructure.keySet()) {
            MFStructure mfStruc = this.mapOfMFStructure.get(keyValue);
            mfStruc.num1_avg_quant = mfStruc.num1_sum_quant / mfStruc.num1_count_quant;
        }
        Set<String> keyValueSet2 = new HashSet<String>();
        ResultSet rs2 = getResultSet();
        while(rs2.next()){
            String cust = rs2.getString("cust");
            String keyValue ="";
            keyValue += cust;
            int quant = rs2.getInt("quant");
            int year = rs2.getInt("year");
            String prod = rs2.getString("prod");
            if (year == 1991 && prod.equals("Pepsi")) {
                MFStructure mf = this.mapOfMFStructure.get(keyValue);
                if (keyValueSet2.contains(keyValue)) {
                    mf.num2_count_quant += 1;
                } else {
                    mf.num2_count_quant = 1;
                    keyValueSet2.add(keyValue);
                }
            }
        }
    }
    public void outprintTable() {
        System.out.printf("|%-15s|" , "cust");
        System.out.printf("|%-15s|" , "1_avg_quant");
        System.out.printf("|%-15s|" , "1_sum_quant");
        System.out.printf("|%-15s|" , "2_count_quant");
        System.out.print('\n');
        for (String key : this.mapOfMFStructure.keySet()) {
            MFStructure mf_temp = this.mapOfMFStructure.get(key);
            System.out.printf("|%-15s|", mf_temp.cust);
            if(mf_temp.num1_count_quant>0)
                   System.out.printf("|%-15.1f|", mf_temp.num1_sum_quant/mf_temp.num1_count_quant);
            else System.out.printf("|%-15d|", 0);
            System.out.printf("|%-15s|", mf_temp.num1_sum_quant);
            System.out.printf("|%-15s|", mf_temp.num2_count_quant);
            System.out.print('\n');
        }
    }
}

class MFStructure {
    String cust;
    double num1_sum_quant;
    int num2_count_quant;
    int num1_count_quant;
    double num1_avg_quant;
}