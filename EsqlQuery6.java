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
            String prod = rs0.getString("prod");
            keyValue += "-" + prod;
            int quant = rs0.getInt("quant");
            if (this.mapOfMFStructure.containsKey(keyValue)) {
            	MFStructure mf = this.mapOfMFStructure.get(keyValue);
                mf.num0_count_quant += 1;
                mf.num0_sum_quant += quant;
            } else {
            	MFStructure mf = new MFStructure();
            	mf.cust = cust;
            	mf.prod = prod;
                mf.num0_count_quant = 1;
                mf.num0_sum_quant = quant;
                this.mapOfMFStructure.put(keyValue, mf);
            }
        }
        for (String keyValue : this.mapOfMFStructure.keySet()) {
            MFStructure mfStruc = this.mapOfMFStructure.get(keyValue);
            mfStruc.num0_avg_quant = mfStruc.num0_sum_quant / mfStruc.num0_count_quant;
        }
        Set<String> keyValueSet1 = new HashSet<String>();
        ResultSet rs1 = getResultSet();
        while(rs1.next()){
            String cust = rs1.getString("cust");
            String keyValue ="";
            keyValue += cust;
            String prod = rs1.getString("prod");
            keyValue += "-" + prod;
            int quant = rs1.getInt("quant");
            int month = rs1.getInt("month");
            int year = rs1.getInt("year");
            if (year == 1997 && month >= 1 && month <= 3 && quant > this.mapOfMFStructure.get(keyValue).num0_avg_quant) {
                MFStructure mf = this.mapOfMFStructure.get(keyValue);
                if (keyValueSet1.contains(keyValue)) {
                    mf.num1_count_quant += 1;
                } else {
                    mf.num1_count_quant = 1;
                    keyValueSet1.add(keyValue);
                }
            }
        }
        Set<String> keyValueSet2 = new HashSet<String>();
        ResultSet rs2 = getResultSet();
        while(rs2.next()){
            String cust = rs2.getString("cust");
            String keyValue ="";
            keyValue += cust;
            String prod = rs2.getString("prod");
            keyValue += "-" + prod;
            int quant = rs2.getInt("quant");
            int month = rs2.getInt("month");
            int year = rs2.getInt("year");
            if (year == 1997 && month >= 7 && month <= 9 && quant > this.mapOfMFStructure.get(keyValue).num0_avg_quant) {
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
        System.out.printf("|%-15s|" , "prod");
        System.out.printf("|%-15s|" , "1_count_quant");
        System.out.printf("|%-15s|" , "2_count_quant");
        System.out.print('\n');
        for (String key : this.mapOfMFStructure.keySet()) {
            MFStructure mf_temp = this.mapOfMFStructure.get(key);
            System.out.printf("|%-15s|", mf_temp.cust);
            System.out.printf("|%-15s|", mf_temp.prod);
            System.out.printf("|%-15s|", mf_temp.num1_count_quant);
            System.out.printf("|%-15s|", mf_temp.num2_count_quant);
            System.out.print('\n');
        }
    }
}

class MFStructure {
    String cust;
    String prod;
    double num0_sum_quant;
    int num2_count_quant;
    double num0_avg_quant;
    int num1_count_quant;
    int num0_count_quant;
}