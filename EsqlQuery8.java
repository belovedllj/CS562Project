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
                int max_quant = mf.num0_max_quant;
                if (quant > max_quant)
                mf.num0_max_quant = quant;
                mf.num0_count_quant += 1;
                mf.num0_sum_quant += quant;
            } else {
            	MFStructure mf = new MFStructure();
            	mf.cust = cust;
            	mf.prod = prod;
                mf.num0_max_quant = quant;
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
            String state = rs1.getString("state");
            int year = rs1.getInt("year");
            if (year > 1997 && state.equals("NJ")) {
                MFStructure mf = this.mapOfMFStructure.get(keyValue);
                if (keyValueSet1.contains(keyValue)) {
                    int min_quant = mf.num1_min_quant;
                    if (quant < min_quant)
                    mf.num1_min_quant = quant;
                    mf.num1_sum_quant += quant;
                    mf.num1_count_quant += 1;
                } else {
                    mf.num1_min_quant = quant;
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
            String prod = rs2.getString("prod");
            keyValue += "-" + prod;
            int num2_month = rs2.getInt("month");
            int quant = rs2.getInt("quant");
            if (quant == this.mapOfMFStructure.get(keyValue).num0_avg_quant) {
                MFStructure mf = this.mapOfMFStructure.get(keyValue);
                mf.num2_month = num2_month;
                if (keyValueSet2.contains(keyValue)) {
                } else {
                    keyValueSet2.add(keyValue);
                }
            }
        }
    }
    public void outprintTable() {
        System.out.printf("|%-15s|" , "cust");
        System.out.printf("|%-15s|" , "prod");
        System.out.printf("|%-15s|" , "2_month");
        System.out.printf("|%-15s|" , "0_max_quant");
        System.out.printf("|%-15s|" , "1_avg_quant");
        System.out.printf("|%-15s|" , "1_min_quant");
        System.out.print('\n');
        for (String key : this.mapOfMFStructure.keySet()) {
            MFStructure mf_temp = this.mapOfMFStructure.get(key);
            System.out.printf("|%-15s|", mf_temp.cust);
            System.out.printf("|%-15s|", mf_temp.prod);
            System.out.printf("|%-15s|", mf_temp.num2_month);
            System.out.printf("|%-15s|", mf_temp.num0_max_quant);
            if(mf_temp.num1_count_quant>0)
                   System.out.printf("|%-15.1f|", mf_temp.num1_sum_quant/mf_temp.num1_count_quant);
            else System.out.printf("|%-15d|", 0);
            System.out.printf("|%-15s|", mf_temp.num1_min_quant);
            System.out.print('\n');
        }
    }
}

class MFStructure {
    String cust;
    String prod;
    int num2_month;
    double num1_sum_quant;
    double num0_sum_quant;
    int num0_max_quant;
    int num1_min_quant;
    double num0_avg_quant;
    int num1_count_quant;
    int num0_count_quant;
    double num1_avg_quant;
}