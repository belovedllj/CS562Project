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
            if (this.mapOfMFStructure.containsKey(keyValue)) {
            	MFStructure mf = this.mapOfMFStructure.get(keyValue);
            } else {
            	MFStructure mf = new MFStructure();
            	mf.cust = cust;
            	mf.prod = prod;
                this.mapOfMFStructure.put(keyValue, mf);
            }
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
            String state = rs1.getString("state");
            if (state.equals("NY")) {
                MFStructure mf = this.mapOfMFStructure.get(keyValue);
                if (keyValueSet1.contains(keyValue)) {
                    mf.num1_sum_quant += quant;
                } else {
                    mf.num1_sum_quant = quant;
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
            String state = rs2.getString("state");
            if (state.equals("NJ")) {
                MFStructure mf = this.mapOfMFStructure.get(keyValue);
                if (keyValueSet2.contains(keyValue)) {
                    mf.num2_sum_quant += quant;
                    mf.num2_count_quant += 1;
                } else {
                    mf.num2_sum_quant = quant;
                    mf.num2_count_quant = 1;
                    keyValueSet2.add(keyValue);
                }
            }
        }
        for (String keyValue : this.mapOfMFStructure.keySet()) {
            MFStructure mfStruc = this.mapOfMFStructure.get(keyValue);
            mfStruc.num2_avg_quant = mfStruc.num2_sum_quant / mfStruc.num2_count_quant;
        }
        Set<String> keyValueSet3 = new HashSet<String>();
        ResultSet rs3 = getResultSet();
        while(rs3.next()){
            String cust = rs3.getString("cust");
            String keyValue ="";
            keyValue += cust;
            String prod = rs3.getString("prod");
            keyValue += "-" + prod;
            int quant = rs3.getInt("quant");
            String state = rs3.getString("state");
            if (state.equals("CT")) {
                MFStructure mf = this.mapOfMFStructure.get(keyValue);
                if (keyValueSet3.contains(keyValue)) {
                    mf.num3_count_quant += 1;
                } else {
                    mf.num3_count_quant = 1;
                    keyValueSet3.add(keyValue);
                }
            }
        }
    }
    public void outprintTable() {
        System.out.printf("|%-15s|" , "cust");
        System.out.printf("|%-15s|" , "prod");
        System.out.printf("|%-15s|" , "1_sum_quant");
        System.out.printf("|%-15s|" , "2_avg_quant");
        System.out.printf("|%-15s|" , "3_count_quant");
        System.out.print('\n');
        for (String key : this.mapOfMFStructure.keySet()) {
            MFStructure mf_temp = this.mapOfMFStructure.get(key);
            System.out.printf("|%-15s|", mf_temp.cust);
            System.out.printf("|%-15s|", mf_temp.prod);
            System.out.printf("|%-15s|", mf_temp.num1_sum_quant);
            if(mf_temp.num2_count_quant>0)
                   System.out.printf("|%-15.1f|", mf_temp.num2_sum_quant/mf_temp.num2_count_quant);
            else System.out.printf("|%-15d|", 0);
            System.out.printf("|%-15s|", mf_temp.num3_count_quant);
            System.out.print('\n');
        }
    }
}

class MFStructure {
    String cust;
    String prod;
    double num1_sum_quant;
    int num3_count_quant;
    int num2_count_quant;
    double num2_sum_quant;
    double num2_avg_quant;
}