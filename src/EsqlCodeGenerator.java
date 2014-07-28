import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class EsqlCodeGenerator {
	
	private SqlInputCondition sqlInputCondition;
	private StructureGenerator structureGenerator;
	private String inputFilePath;
	private StringBuffer queryCodeStringBuffer;
	private HashMap<String, String> informationSchema;
	private String[] groupAttributes;
	private int numberOfGroupingVariables;
	private List<GroupingVariable> listOfAggrFunctions;
	private List<String> listOfSingleVariables;
	private String tableName;
	private List<String> selectedAttributes;
	
	public EsqlCodeGenerator(String inputFilePath, String tableName) {
		this.inputFilePath = inputFilePath;
		this.tableName = tableName;
	}
	/*
	 * Initiate each arguments we needed in generating java code
	 */
	protected void init() throws IOException {
		this.sqlInputCondition = new SqlInputCondition(this.inputFilePath);
		this.structureGenerator = new StructureGenerator(this.inputFilePath);
		this.queryCodeStringBuffer = new StringBuffer();
		this.informationSchema = this.structureGenerator.getInformationScheme();
		this.groupAttributes = this.structureGenerator.getGroupingAttributes();
		this.numberOfGroupingVariables = this.structureGenerator.getGroupingVariableNumber();
		this.listOfAggrFunctions = this.structureGenerator.getAggrFunction();
		this.listOfSingleVariables = this.structureGenerator.getDifferentAttributes(-2);
		this.selectedAttributes = this.structureGenerator.getDifferentAttributes(-1);
	}
	
	public static void main(String[] args) {
		EsqlCodeGenerator esqlCoderGenerator = new EsqlCodeGenerator("input5.txt", "sales");
        try {
        	esqlCoderGenerator.generateQueryDBCode();
		} catch (IOException e) {
			e.printStackTrace();
		}
        
		esqlCoderGenerator.createJavaFile();
	}
	/*
	 * write java code to file and console
	 */
	public void createJavaFile() {
		String esqlJavaCode = this.queryCodeStringBuffer.toString();
		System.out.println(esqlJavaCode);
		try {
			BufferedWriter javaCodeWriter = new BufferedWriter(new FileWriter(new File("EsqlQuery5.java")));
			javaCodeWriter.write(esqlJavaCode);
			javaCodeWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/*
	 * Main flow work 
	 */
	public void generateQueryDBCode() throws IOException {
		init();
		outPrintImportStat();
		outPrintClassHeader();
		outPrintMainMethod();
		outPrintDataBaseQueryMethod();
		outPrintTable();
        outPrintLastBracket();
		outPrintMFStructure();
	}
	/*
	 * Wring "import" statements
	 */
	protected void outPrintImportStat() {
		this.queryCodeStringBuffer.append("import java.sql.*;\n");
		this.queryCodeStringBuffer.append("import java.util.*;\n\n");
	}
	/*
	 * Write class header and instance variables
	 */
	protected void outPrintClassHeader() {
		this.queryCodeStringBuffer.append("public class EsqlQuery {\n");
		this.queryCodeStringBuffer.append("    private Map<String, MFStructure> mapOfMFStructure"
				+ " = new LinkedHashMap<String, MFStructure>();\n");
		this.queryCodeStringBuffer.append("    private ResultSet getResultSet() throws Exception {\n");
		this.queryCodeStringBuffer.append("        String usr = \"postgres\";\n");
		this.queryCodeStringBuffer.append("        String pwd = \"cherishtina\";\n");
		this.queryCodeStringBuffer.append("        String url = \"jdbc:postgresql://localhost:5432/postgres\";\n\n");
		this.queryCodeStringBuffer.append("        PreparedStatement pstmt;\n");
		this.queryCodeStringBuffer.append("        Class.forName(\"org.postgresql.Driver\");\n");
		this.queryCodeStringBuffer.append("        Connection conn = DriverManager.getConnection(url, usr, pwd);\n");
		this.queryCodeStringBuffer.append("        pstmt = conn.prepareStatement(\"select * from " + 
		                                    this.tableName + "\");\n");
		this.queryCodeStringBuffer.append("        return pstmt.executeQuery();\n    }\n\n");	
	}
	/*
	 * write main method
	 */
	protected void outPrintMainMethod() {
		this.queryCodeStringBuffer.append("    public static void main(String[] args) {\n"
				+ "        try {\n" + "            EsqlQuery esqlQuery = new EsqlQuery();\n"
				+ "            esqlQuery.makeDatabaseQuery();\n" +
				"            esqlQuery.outprintTable();\n"
				+ "        } catch (Exception e) {\n" + "            e.printStackTrace();\n"
				+"        }\n" + "    }\n");
	}
	/*
	 * Wring core java code
	 */
	protected void outPrintDataBaseQueryMethod() {	
		this.queryCodeStringBuffer.append("    public void makeDatabaseQuery()"
				+ " throws Exception {\n");
		for(int i = 0; i < this.numberOfGroupingVariables + 1; i++) {
			if (i != 0) {
				this.queryCodeStringBuffer.append("        " +
			         "Set<String> keyValueSet" + i + " = new HashSet<String>();\n");
			}
			this.queryCodeStringBuffer.append("        ResultSet rs" + i 
					+ " = getResultSet();\n" + "        while(rs" + i + ".next()){\n");
			List<String> groupVariables = outPrintGetGroupAttrInDB(i);
			List<String> singleVaribales = outPrintGetSingleAttrInDB(i);
			Set<String> attrForgetting = new HashSet<String>();
			getAttrInCondition(i, attrForgetting);
			List<String> aggrVaribales = getAggrFuctionVars(i, attrForgetting);
			outPrintAttrsInGet(i, attrForgetting);
			List<String> variablesHaveAvg = this.structureGenerator.getVariableNameWithAvg(i);
			if(i == 0) {
				outPrintMfInstanceBuildFirstTime(groupVariables, aggrVaribales, i);
			}
			if(i != 0) {
				outPrintQueryCondition(aggrVaribales,singleVaribales, i);
			}
			this.queryCodeStringBuffer.append("        }\n");
			if (!variablesHaveAvg.isEmpty()) {
				outPrintCountAvgValue(variablesHaveAvg, i);
			}	
		}
		this.queryCodeStringBuffer.append("    }\n");
	}
	/*
	 * write code of caculating avg value after each scan
	 */
	protected void outPrintCountAvgValue(List<String> variablesHaveAvg, int index) {
		this.queryCodeStringBuffer.append("        for (String keyValue :"
				+ " this.mapOfMFStructure.keySet()) {\n");
		this.queryCodeStringBuffer.append("            MFStructure mfStruc ="
				+ " this.mapOfMFStructure.get(keyValue);\n");
		for (String avgVariableName : variablesHaveAvg) {
			this.queryCodeStringBuffer.append("            mfStruc.num" + index +
					"_avg_" + avgVariableName + " = " + "mfStruc.num" + index +
					"_sum_" + avgVariableName +" / " + "mfStruc.num" + index +
					"_count_" + avgVariableName + ";\n");
		}
		this.queryCodeStringBuffer.append("        }\n");
	}
	/*
	 * write code of map build from multiple mf instance at scan 0
	 */
	protected void outPrintMfInstanceBuildFirstTime(List<String> groupVariables, 
			List<String> aggrVaribales, int index) {
		if (groupVariables.isEmpty()) throw new IllegalArgumentException();
		this.queryCodeStringBuffer.append("            if (this.mapOfMFStructure."
				+ "containsKey(keyValue)) {\n" + "            	MFStructure mf = "
						+ "this.mapOfMFStructure.get(keyValue);\n");
		for (String aggrVar : aggrVaribales) {
			String[] eachPartOfAggrVar = aggrVar.split("_");
			if (eachPartOfAggrVar.length != 3) throw new IllegalArgumentException();
			String aggrFuc = eachPartOfAggrVar[1];
			String aggrAttr = eachPartOfAggrVar[2];
			switch (aggrFuc) {
			    case "max":  outPrintMaxUpadate(aggrAttr, index, false); break;
			    case "min":  outPrintMinUpadate(aggrAttr, index, false); break;
			    case "sum":  outPrintSumUpadate(aggrAttr, index, false); break;
			    case "count":  outPrintCountUpadate(aggrAttr, index, false); break;
			}
		}	
		this.queryCodeStringBuffer.append("            } else {\n");
		this.queryCodeStringBuffer.append("            	MFStructure mf = " + 
		           "new MFStructure();\n");
		for (String groupVar : groupVariables) {
			this.queryCodeStringBuffer.append("            	mf." + groupVar + 
					" = " + groupVar + ";\n");
		}
		for (String aggrVar : aggrVaribales) {
			String[] eachPartOfAggrVar = aggrVar.split("_");
			if (eachPartOfAggrVar.length != 3) throw new IllegalArgumentException();
			String aggrFuc = eachPartOfAggrVar[1];
			String aggrAttr = eachPartOfAggrVar[2];
			switch (aggrFuc) {
			    case "max":  outPrintMaxBuild(aggrAttr, index, false); break;
			    case "min":  outPrintMinBuild(aggrAttr, index, false); break;
			    case "sum":  outPrintSumBuild(aggrAttr, index, false); break;
			    case "count":  outPrintCountBuild(aggrAttr, index, false); break;
			}
		}
		this.queryCodeStringBuffer.append("                this.mapOfMFStructure.put(keyValue, mf);\n");
		this.queryCodeStringBuffer.append("            }\n");

	}
	/*
	 * wring mf count initial value
	 */
	protected void outPrintCountBuild(String aggrAttr, int index, boolean hasBuffer) {
		String buffer = "            ";
		if (hasBuffer)   buffer = "                ";
		this.queryCodeStringBuffer.append(buffer + "    " + "mf.num" + index + "_count_" 
		+ aggrAttr + " = 1;\n");
	}
	
	/*
	 * wring mf count value update for each mf instance
	 */
	
	protected void outPrintCountUpadate(String aggrAttr, int index, boolean hasBuffer) {
		String buffer = "            ";
		if (hasBuffer)   buffer = "                ";
		this.queryCodeStringBuffer.append(buffer + "    " + "mf.num" + index + "_count_" + aggrAttr +
				" += 1" + ";\n");		
	}
	
	protected void outPrintSumBuild(String aggrAttr, int index, boolean hasBuffer) {
		String buffer = "            ";
		if (hasBuffer)   buffer = "                ";
		this.queryCodeStringBuffer.append(buffer + "    " + "mf.num" + index + "_sum_" + aggrAttr +
				" = " + aggrAttr + ";\n");	
	}
	
	protected void outPrintSumUpadate(String aggrAttr, int index, boolean hasBuffer) {
		String buffer = "            ";
		if (hasBuffer)   buffer = "                ";
		this.queryCodeStringBuffer.append(buffer + "    " + "mf.num" + index + "_sum_" + aggrAttr +
				" += " + aggrAttr + ";\n");		
	}
	
	protected void outPrintMinBuild(String aggrAttr, int index, boolean hasBuffer) {
		String buffer = "            ";
		if (hasBuffer)   buffer = "                ";
		this.queryCodeStringBuffer.append(buffer + "    " + "mf.num" + index + "_min_" + aggrAttr +
				" = " + aggrAttr + ";\n");
	}	
	
	protected void outPrintMinUpadate(String aggrAttr, int index, boolean hasBuffer) {
		String buffer = "            ";
		if (hasBuffer)   buffer = "                ";
		this.queryCodeStringBuffer.append(buffer + "    " + "int min_" + aggrAttr + " = "
				+ "mf.num" + index + "_min_" + aggrAttr + ";\n");
		this.queryCodeStringBuffer.append(buffer + "    " + "if (" + aggrAttr + 
				" < min_" + aggrAttr + ")\n" + buffer + "    "
						+ "mf.num" + index + "_min_" + aggrAttr + " = " + aggrAttr
						+ ";\n");		
	}	
	
	protected void outPrintMaxBuild(String aggrAttr, int index, boolean hasBuffer) {
		String buffer = "            ";
		if (hasBuffer)   buffer = "                ";
		this.queryCodeStringBuffer.append(buffer + "    " + "mf.num" + index + "_max_" + aggrAttr +
				" = " + aggrAttr + ";\n");
	}
	
	protected void outPrintMaxUpadate(String aggrAttr, int index, boolean hasBuffer) {
		String buffer = "            ";
		if (hasBuffer)   buffer = "                ";
		this.queryCodeStringBuffer.append(buffer + "    " + "int max_" + aggrAttr + " = "
				+ "mf.num" + index + "_max_" + aggrAttr + ";\n");
		this.queryCodeStringBuffer.append(buffer + "    " + "if (" + aggrAttr + 
				" > max_" + aggrAttr + ")\n" + buffer + "    "
						+ "mf.num" + index + "_max_" + aggrAttr + " = " + aggrAttr
						+ ";\n");		
	}
	/*
	 * write definition of value to avoid duplicates
	 */
	protected void outPrintAttrsInGet(int index, Set<String> attrForgetting) {
		for (String attrName : attrForgetting) {
			outPrintTypeAndAttr(index, attrName, false);
		}
	}
	/*
	 * build value definition set
	 */
	protected void getAttrInCondition(int index, Set<String> attrForgetting) {
		List<String> attrInCondition = this.sqlInputCondition.getAttr(index);
		for (String attrName : attrInCondition) {
			attrForgetting.add(attrName);
		}
	}
	/*
	 * Build value of definition set in aggregation attribute
	 */
	protected List<String> getAggrFuctionVars(int index, Set<String> attrForgetting) {
		List<String> aggrFunctionVariables = new ArrayList<String>();
		List<GroupingVariable> listOfGroupVars = this.structureGenerator.getAggrFunctionByScanIndex(index);
		for (GroupingVariable groupVars : listOfGroupVars) {
			String attrName = groupVars.coAttribute;
			aggrFunctionVariables.add("num" + index + "_" + groupVars.Operation + "_" + attrName);
			attrForgetting.add(attrName);
		}
		return aggrFunctionVariables;

	}
	// for each scan 's condition and mf instance update
	protected void outPrintQueryCondition(
			List<String> aggrVaribales, List<String>singleVaribales, int index) {
		List<SqlInputCondition.Data> queryCondtionOfEachScan = 
				this.sqlInputCondition.getCondition(index);
		if(queryCondtionOfEachScan.isEmpty()) return;

		String combinedConditions = "";
		for (int i = 0; i < queryCondtionOfEachScan.size(); i++) {
			SqlInputCondition.Data condition = queryCondtionOfEachScan.get(i);
			String attrName = condition.con_ga;
			String typeOfAttr = this.informationSchema.get(attrName);
			String eachCondtion;
			if(typeOfAttr.equals("String") || typeOfAttr.equals("char")) {
				eachCondtion = getStringCondition(condition);
			} else {
				eachCondtion = getNumberCondition(condition);
			}	
			if (eachCondtion == null) 
				throw new IllegalArgumentException("condition couldn't be null.");
			if (i == 0) combinedConditions += eachCondtion;
			else combinedConditions += " && " + eachCondtion;
		}
		this.queryCodeStringBuffer.append("            " + "if (" +
				combinedConditions +") {\n");
		outPrintUpadeMfInstanceValue(aggrVaribales, singleVaribales, index);			
		this.queryCodeStringBuffer.append("            }\n");
		
		// leave the >= <= for the string
	}
	/*
	 * in scan >= 1 write mf instance update
	 */
	protected void outPrintUpadeMfInstanceValue(
			List<String> aggrVaribales, List<String>singleVaribales, int index) {
		this.queryCodeStringBuffer.append("                MFStructure mf = " + 
				"this.mapOfMFStructure.get(keyValue);\n");
		for (String singleVariable : singleVaribales) {
			this.queryCodeStringBuffer.append("                mf." + singleVariable +
					 " = " + singleVariable + ";\n");
		}
		this.queryCodeStringBuffer.append("");
		this.queryCodeStringBuffer.append("                if (keyValueSet" + index +
				"." + "contains(keyValue)) {\n");
		for (String aggrVar : aggrVaribales) {
			String[] eachPartOfAggrVar = aggrVar.split("_");
			if (eachPartOfAggrVar.length != 3)
				throw new IllegalArgumentException();
			String aggrFuc = eachPartOfAggrVar[1];
			String aggrAttr = eachPartOfAggrVar[2];
			switch (aggrFuc) {
			    case "max":  outPrintMaxUpadate(aggrAttr, index, true); break;
			    case "min":  outPrintMinUpadate(aggrAttr, index, true); break;
			    case "sum":  outPrintSumUpadate(aggrAttr, index, true); break;
			    case "count":  outPrintCountUpadate(aggrAttr, index, true); break;
			}
		}	
		this.queryCodeStringBuffer.append("                } else {\n");
		for (String aggrVar : aggrVaribales) {
			String[] eachPartOfAggrVar = aggrVar.split("_");
			if (eachPartOfAggrVar.length != 3) throw new IllegalArgumentException();
			String aggrFuc = eachPartOfAggrVar[1];
			String aggrAttr = eachPartOfAggrVar[2];
			switch (aggrFuc) {
			    case "max":  outPrintMaxBuild(aggrAttr, index, true); break;
			    case "min":  outPrintMinBuild(aggrAttr, index, true); break;
			    case "sum":  outPrintSumBuild(aggrAttr, index, true); break;
			    case "count":  outPrintCountBuild(aggrAttr, index, true); break;
			}
		}
		this.queryCodeStringBuffer.append("                    keyValueSet" + index + 
				".add(keyValue);\n");
		this.queryCodeStringBuffer.append("                }\n");

	}
	/*
	 * wring judge condition for String variables 
	 */
	protected String getStringCondition(SqlInputCondition.Data conditionData) {
		String conditionOfStr = null;
		if(!conditionData.hasAggfunBoolean) {
			if (conditionData.operand.equals("=")) {
				conditionOfStr = conditionData.con_ga + ".equals(" +
			        conditionData.rightString + ")";
			} else if (conditionData.operand.equals("<>")) {
				conditionOfStr = "!" + conditionData.con_ga + ".equals(" +
				        conditionData.rightString + ")";
			}
		} else {
			if (conditionData.operand.equals("=")) {
				conditionOfStr = conditionData.con_ga + ".equals(" +
			        "this.mapOfMFStructure.get(keyValue).num" + 
			        conditionData.rightString + ")";
			} else if (conditionData.operand.equals("<>")) {
				conditionOfStr = "!" + conditionData.con_ga + ".equals(" +
				        "this.mapOfMFStructure.get(keyValue).num" + 
				        conditionData.rightString + ")";
			}
			return conditionOfStr;
		}
		
		return conditionOfStr;
	}
	/*
	 * for number condition
	 */
	protected String getNumberCondition(SqlInputCondition.Data conditionData) {
		String conditionOfStr = null;
		if(!conditionData.hasAggfunBoolean) {
			conditionOfStr = conditionData.getOperationSatement();
		} else {
			conditionOfStr = conditionData.getPartialOperation() + 
					"this.mapOfMFStructure.get(keyValue).num" + 
			        conditionData.rightString;
		}		
		return conditionOfStr;
	}
	/*
	 * write type and get type in variables definition
	 */
	protected void outPrintTypeAndAttr(int index, String attrName, boolean isAggr) {
		if(!isAggr) {
			String typeOfAttr = this.informationSchema.get(attrName);
			char first = Character.toUpperCase(typeOfAttr.charAt(0));
			// first letter uppercase
			String typeInGet = first + typeOfAttr.substring(1);
			this.queryCodeStringBuffer.append("            " + typeOfAttr + " " + 
					attrName + " = rs" + index + ".get" + typeInGet + "(\"" + 
					attrName + "\");\n");
		} else {
			String typeOfAttr = this.informationSchema.get(attrName);
			char first = Character.toUpperCase(typeOfAttr.charAt(0));
			// first letter uppercase
			String typeInGet = first + typeOfAttr.substring(1);
			this.queryCodeStringBuffer.append("            " + typeOfAttr + " " +
			"num" + index + "_" + attrName + " = rs" + index + ".get" + typeInGet +
			"(\"" + attrName + "\");\n");
		}
	}
	
	/*
	 * wring definition of single attributes
	 */
	
	protected List<String> outPrintGetSingleAttrInDB(int scanIndex) {
		List<String> singleAttrs = new ArrayList<String>();
		if (this.listOfSingleVariables.size() == 0 || this.listOfSingleVariables == null) 
			return singleAttrs;
		for (String singleVariable : this.listOfSingleVariables) {
			String[] singleVariableParts = singleVariable.split("\\.");
			int numOfSingleVariable = Integer.parseInt(singleVariableParts[0]);
			if (numOfSingleVariable != scanIndex) continue;
			String fieldNameOfSingleVariable = singleVariableParts[1];
			singleAttrs.add("num" + scanIndex + "_" + fieldNameOfSingleVariable);
			outPrintTypeAndAttr(scanIndex, fieldNameOfSingleVariable, true);			
		}
		return singleAttrs;
	}
	/*
	 *  wring definition of group attributes
	 */
	protected List<String> outPrintGetGroupAttrInDB(int scanIndex) {
		List<String> groupVaribales = new ArrayList<String>();
		for(int j = 0; j < this.groupAttributes.length; j++) {
			// using Map of Information Schema get the primitive type of each group attribute
			String attrName = this.groupAttributes[j];
			groupVaribales.add(attrName);
			outPrintTypeAndAttr(scanIndex, attrName, false);
			outPrintBuildKey(j, attrName);
		}
		return groupVaribales;
		//outPrintBuildMfInstance(groupAttrsKey);
	}
	/*
	 * write key value build for map
	 */
	protected void outPrintBuildKey(int index, String attrName) {
		if (index == 0) {
			this.queryCodeStringBuffer.append("            String keyValue =\"\";\n" +
		     "            keyValue += " + attrName + ";\n");
		} else {
			this.queryCodeStringBuffer.append("            keyValue += \"-\" + " + attrName + ";\n");
		}
		
	}
	

	protected void outPrintLastBracket() {
		this.queryCodeStringBuffer.append("}\n");
	}
	/*
	 * mf class writing
	 */
	protected void outPrintMFStructure() {
		String MFStructure = this.structureGenerator.printClass(this.informationSchema,
				this.groupAttributes, this.listOfAggrFunctions, this.listOfSingleVariables);
		this.queryCodeStringBuffer.append(MFStructure);
	}
	/*The function outprintTable is used to produce the output function in the final .java file, the
	 * input parameter selectedAttributes is a string list of all the selected attributes that will be
	 * display, the produced function will be added to the end of StringBuffer outPut
	 */
	protected void outPrintTable() {
		String S = "    public void outprintTable() {\n" ;
		for (int i = 0; i < this.selectedAttributes.size(); i++){
			S = S + "        System.out.printf(" + '"' + "|%-15s|" + '"'+ " , " + '"' +  this.selectedAttributes.get(i) + '"' + ");\n";
			//S = S + "|  " + selectedAttributes.get(i) + " |";
		}
		S = S +"        System.out.print('\\n');\n" + 
		        "        for (String key : "
				+ "this.mapOfMFStructure.keySet()) {\n" 
		+ "            MFStructure mf_temp = this.mapOfMFStructure.get(key);\n";
		for(int j = 0; j < this.selectedAttributes.size(); j++){
			String temp = this.selectedAttributes.get(j);
			if(Character.isDigit(temp.charAt(0))){
				if(temp.indexOf("avg")>0) 
					S = S + "            if(mf_temp.num" + temp.replaceAll("avg", "count") + ">0)\n"
				+ "                   System.out.printf(" + '"' +  "|%-15.1f|" + '"' 
				+ ", mf_temp.num" + temp.replaceAll("avg", "sum") + "/" 
				+  "mf_temp.num" + temp.replaceAll("avg", "count") + ");\n"
				+  "            else System.out.printf(" + '"' +  "|%-15d|" + '"' + ", 0);\n";
				else  
				S = S + "            System.out.printf(" + '"' +  "|%-15s|" + '"' + ", mf_temp.num" + temp + ");\n";
			}
			else S = S + "            System.out.printf(" + '"' +  "|%-15s|" + '"' + ", mf_temp." + temp + ");\n";	
		}
		S = S + "            System.out.print('\\n');\n        }\n    }\n";
		this.queryCodeStringBuffer.append(S);		
	}
}
