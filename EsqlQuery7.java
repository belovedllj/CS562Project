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
            } else {
            	MFStructure mf = new MFStructure();
            	mf.cust = cust;
            	mf.prod = prod;
                mf.num0_max_quant = quant;
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
            if (quant < this.mapOfMFStructure.get(keyValue).num0_max_quant) {
                MFStructure mf = this.mapOfMFStructure.get(keyValue);
                if (keyValueSet1.contains(keyValue)) {
                    int min_quant = mf.num1_min_quant;
                    if (quant < min_quant)
                    mf.num1_min_quant = quant;
                } else {
                    mf.num1_min_quant = quant;
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
            if (quant < this.mapOfMFStructure.get(keyValue).num0_max_quant) {
                MFStructure mf = this.mapOfMFStructure.get(keyValue);
                if (keyValueSet2.contains(keyValue)) {
                    int max_quant = mf.num2_max_quant;
                    if (quant > max_quant)
                    mf.num2_max_quant = quant;
                } else {
                    mf.num2_max_quant = quant;
                    keyValueSet2.add(keyValue);
                }
            }
        }
    }
    public void outprintTable() {
        System.out.printf("|%-15s|" , "cust");
        System.out.printf("|%-15s|" , "prod");
        System.out.printf("|%-15s|" , "1_min_quant");
        System.out.printf("|%-15s|" , "2_max_quant");
        System.out.print('\n');
        for (String key : this.mapOfMFStructure.keySet()) {
            MFStructure mf_temp = this.mapOfMFStructure.get(key);
            System.out.printf("|%-15s|", mf_temp.cust);
            System.out.printf("|%-15s|", mf_temp.prod);
            System.out.printf("|%-15s|", mf_temp.num1_min_quant);
            System.out.printf("|%-15s|", mf_temp.num2_max_quant);
            System.out.print('\n');
        }
    }
}

class MFStructure {
    String cust;
    String prod;
    int num2_max_quant;
    int num0_max_quant;
    int num1_min_quant;
}