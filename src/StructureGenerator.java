
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class StructureGenerator {
	
	private final String usr ="postgres";
	private final String pwd ="cherishtina";
	private final String url ="jdbc:postgresql://localhost:5432/postgres";
	private String filePath;
	
	public StructureGenerator() {
		this.filePath = "input.txt";
	}
	public StructureGenerator(String filePath) {
		this.filePath = filePath;
	}
	

	
/*
 * the getDifferentAttributes function is to get all the column names for the
 * selected attributes that will be displayed in the final result,
 *  and return them in a linked list of strings
 */
	public LinkedList<String> getDifferentAttributes(int Number){
		LinkedList<String> outPut = new LinkedList<String>(); //for storage of all of the selected attributes
		LinkedList<String> outputSub = new LinkedList<String>(); //for storage of the selected attributes without grouping attributes  
		LinkedList<String> buildClass = new LinkedList<String>(); //for storage of only the names with '.' inside  
		String[] Select_Att = null;
		String S1;
		try {
		       File f1 = new File(this.filePath);   //find input file
		       BufferedReader input = new BufferedReader(new FileReader(f1));  //read input from file
		                                                   													   
		       while ((S1 = input.readLine()) != null) {
		    	     	
		    	   /**read select attributes from the file**/
		    	   if(S1.equals("﻿SELECT ATTRIBUTES(S):")){
		    		   String InputS = input.readLine().trim();
		    		   Select_Att = InputS.split(",");  //split the line
		    		   for(int i=0; i<Select_Att.length; i++){
		    			   Select_Att[i] = Select_Att[i].trim();
		    		   }
		    		   
		    		   for(int i=0; i<Select_Att.length; i++){
		    			   if (Select_Att[i].charAt(1) - '.' ==0||Select_Att[i].charAt(1) - '_' == 0){
		    				   outPut.add(Select_Att[i].replace('.', '_'));
		    				   if(Select_Att[i].charAt(0) - '0' == Number) outputSub.add(Select_Att[i]); //Exclude grouping attributes
		    				   if(Select_Att[i].charAt(1) - '.' == 0) buildClass.add(Select_Att[i]); //Exclude aggregate function
		    			   }
		    			   else outPut.add(Select_Att[i]);
		    				   
		    		   }
		    	   }
		       }
		       input.close();
		}
		catch (Exception e) {
			e.printStackTrace();
	    }
		if(Number == -1) return outPut; //when the input choice is -1, return all the select attributes
		else if(Number == -2) return buildClass; //when the choice is -2, return the select attributes with '.'.
		else return outputSub; //otherwise, return select attributes without grouping attributes
	}
/*
 * The function printClass is used to produce the mfstructure class in the final .java file witch will be stored in
 * a StringBuffer, the input parameter Information_Schema is a hashmap variable include data name and type in the 
 * database, Group_Att is a String array of grouping attribute names, Group_V is a list of instance of class 
 * GroupingVariables, witch include the details of grouping variables and its aggregate function, Other is a list of 
 * selected colunms witch has a name with '.' inside, like 1.quant, and the reulst will be added to the end of 
 * StringBuffer outPut 
 */
    public String printClass(HashMap<String, String> Information_Scheme, 
    		String[] Group_Att, List<GroupingVariable> Group_V, List<String> Other) {
		String mfStructure = "\nclass MFStructure {\n";
		for(int i=0; i<Group_Att.length; i++){
			mfStructure = mfStructure + "    " + Information_Scheme.get(Group_Att[i]) + " " + Group_Att[i] + ";\n";
		}
		Set<String> temp_R = new HashSet<String>();
		for(int j=0; j < Group_V.size(); j++){
			GroupingVariable temp = Group_V.get(j);
			if(temp.Operation.equals("avg")){
				temp_R.add("    double" + " num" + temp.Number + "_" + "sum" + "_" + temp.coAttribute + ";\n");
				temp_R.add("    int" + " num" + temp.Number + "_" + "count" + "_" + temp.coAttribute + ";\n");
				temp_R.add("    double" + " num" + temp.Number + "_" + "avg" + "_" + temp.coAttribute + ";\n");
			}
			else temp_R.add("    " + temp.Type + " num" + temp.Number + "_" + temp.Operation + "_" + temp.coAttribute + ";\n");
		}
		for(int k=0;k<Other.size();k++){
			String temp_A = Other.get(k);
			//System.out.println(temp_A);
			String Temp[] = temp_A.split("\\.");
			for(int n=0;n<Temp.length;n++){
				Temp[n] = Temp[n].trim();
			}
			mfStructure = mfStructure + "    " + Information_Scheme.get(Temp[1])
					+ " num" + Temp[0] + "_" + Temp[1] + ";\n";
		}
		for(String temp : temp_R){
			mfStructure = mfStructure + temp;
		}
		mfStructure = mfStructure + "}";
		return mfStructure;		
	}
/*
 * The function getGroupingAttributes is used to read grouping attribute names from the input file
 * and store them into a String array and return to the main function, the input parameter File_Path
 * is a string of input file path
 */
    public String[] getGroupingAttributes(){
		String[] groupAtt = null;
		String S1;
		try {
		       File f1 = new File(this.filePath);   //find input file
		       BufferedReader input = new BufferedReader(new FileReader(f1));  //read input from file
		                                                   													   //save the num of grouping variables
		       while ((S1 = input.readLine()) != null) {
		    	  		    	
		    	   /**Set data type for grouping attributes**/
		    	   if(S1.equals("GROUPING ATTRIBUTES(V):")){
		    		    String InputG = input.readLine();
		    		    groupAtt = InputG.split(",");  //split the line
		    		    for(int i=0;i<groupAtt.length;i++){
		    		    	groupAtt[i]=groupAtt[i].trim();                           //store the attributes in the array
		    		    }


		    	   }
		       }
		       input.close();
		}
		catch (Exception e) {
			e.printStackTrace();
	    }
		return groupAtt;      //return the String array of grouping attribute names.  
	}
    
/*The function getInformation is used to get column_name and data_type from the database information
* schema via embed SQL, and return them in a hash map with column_name as keys. The input parameters
* are the user name, password and URL of the database  **/
    public HashMap<String, String> getInformationScheme(){
		try 
		{
			Class.forName("org.postgresql.Driver");
		} 

		catch(Exception e) 
		{
			System.out.println("Fail loading Driver!");
			e.printStackTrace();
		}
		HashMap<String, String> Information_Scheme = new HashMap<String, String>();
		try 
		{
			Connection conn = DriverManager.getConnection(this.url, this.usr, this.pwd);
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT table_name, column_name, is_nullable, " + 
			"data_type, character_maximum_length\n"+
		    "FROM INFORMATION_SCHEMA.Columns\n"+
            "WHERE table_name = 'sales'");
			
			
			
			/**Load a single row form information scheme **/

			while (rs.next()) 
			{
			    String temp=rs.getString("data_type");
				if(temp.equals("character varying")) temp = "String";
				else if(temp.equals("integer")) temp = "int";
				else if(temp.equals("character")) temp = "String";
				Information_Scheme.put(rs.getString("column_name"), temp);
			}
		}
		
		
		
		catch(SQLException e) 
		{
			System.out.println("Connection URL or username or password errors!");
			e.printStackTrace();
		}
		/**display data type and column name in database, not necessary **/
		/*LinkedList<String> attName = new LinkedList<String>();
		LinkedList<String> attType = new LinkedList<String>();
		for(String string : Information_Scheme.keySet()){
			attName.add(string);
			attType.add(Information_Scheme.get(string));
		}
		for(int i=0; i<attName.size();i++){
			System.out.println(attName.get(i) + "    " + attType.get(i));
		} */
		return Information_Scheme;
	}
    
    public List<String> getVariableNameWithAvg(int index) {
    	List<String> variableListWithAvg = new ArrayList<String>();
    	try {
    		   String S1;
		       File f1 = new File(this.filePath);   //find input file
		       BufferedReader input = new BufferedReader(new FileReader(f1));  //read input from file
		       
		       String[] F_Vect = null;  //save the num of grouping variables
		       while ((S1 = input.readLine()) != null) {
		    	   /**Set data type for grouping variables**/
		    	   if(S1.equals("F-VECT([F]):")){
		    		   String inputF = input.readLine();
		    			F_Vect = inputF.split(",");  //split the line
		    		    for(int j=0; j<F_Vect.length; j++) {
		    		    
		    		    	F_Vect[j]=F_Vect[j].trim();
		    		    	String[] temp = F_Vect[j].split("_");
		    		    	for(int i=0;i<temp.length;i++){
		    		    		temp[i] = temp[i].trim();
		    		    	}
		    		    	if (temp.length == 2) {
		    		    		if (temp[0].equalsIgnoreCase("avg")) {
		    		    			if(index == 0)
		    		    				variableListWithAvg.add(temp[1]);		    		    			
		    		    		}     		    		
		    		    	}
		    		    	else {
		    		    		if (temp[1].equalsIgnoreCase("avg")) {
		    		    			if(index == Integer.parseInt(temp[0]))
		    		    				variableListWithAvg.add(temp[2]);			    		    			
		    		    		}   
		    		    	}		    		    	   	
		    			}
		    	    }		    		  
		       }
		       input.close();
		  }
	      catch (Exception e) {
		       e.printStackTrace();
	      }
    	return variableListWithAvg;
    }
    
    public ArrayList<GroupingVariable> getAggrFunctionByScanIndex(int index){
    	String S1;
		ArrayList<GroupingVariable> aggregateFuncSub = new ArrayList<GroupingVariable>();
		 Set<String> tempSet = new HashSet<String>();
		try {
		       File f1 = new File(this.filePath);   //find input file
		       BufferedReader input = new BufferedReader(new FileReader(f1));  //read input from file
		       
		       String[] F_Vect = null;  //save the num of grouping variables
		       while ((S1 = input.readLine()) != null) {
		    	   /**Set data type for grouping variables**/
		    	   if(S1.equals("F-VECT([F]):")){
		    		   String inputF = input.readLine();
		    			F_Vect = inputF.split(",");  //split the line
		    		    for(int j=0;j<F_Vect.length;j++){
		    		    
		    		    	F_Vect[j]=F_Vect[j].trim();
		    		    	String[] temp = F_Vect[j].split("_");
		    		    	for(int i=0;i<temp.length;i++){
		    		    		temp[i] = temp[i].trim();
		    		    	}
		    		    	if (temp.length == 2) {
		    		    		if (!temp[0].equalsIgnoreCase("avg")) {
		    		    			tempSet.add("0_" + temp[0] + "_" + temp[1]);		    		    			
		    		    		} else {
		    		    			tempSet.add("0_" + "count" + "_" + temp[1]);
		    		    			tempSet.add("0_" + "sum" + "_" + temp[1]);
		    		    		}	    		    		
		    		    	}
		    		    	else {
		    		    		if (!temp[1].equalsIgnoreCase("avg")) {
		    		    			tempSet.add(temp[0] + "_" + temp[1] + "_" + temp[2]);		    		    			
		    		    		} else {
		    		    			tempSet.add(temp[0] + "_" + "count" + "_" + temp[2]);
		    		    			tempSet.add(temp[0] + "_" + "sum" + "_" + temp[2]);
		    		    		}	   
		    		    	}		    		    	   	
		    			}
		    	    }		    		  
		       }
		       input.close();
		  }
	      catch (Exception e) {
		       e.printStackTrace();
	      }
		
		for (String aggrStr : tempSet) {
			String[] temp = aggrStr.split("_");
			GroupingVariable groupingVariable = new GroupingVariable
					(Integer.valueOf(temp[0]).intValue(),temp[1],temp[2]);
			if(temp[1].equals("count") || temp[1].equals("max") || temp[1].equals("min"))
				groupingVariable.setType("int");
	    	else groupingVariable.setType("double");
			if(groupingVariable.Number == index) aggregateFuncSub.add(groupingVariable);
		}
	     return aggregateFuncSub;        //return the certain functions as required
	
    }
    
/*
 * The function getGroupingVariablesList is used to read aggregate functions from the input file and cut it 
 * into grouping variable number, operation and the corresponding column that the operation will apply to,
 * as well as the data type of the result, File_Path it the input file path, Group_Number is the number of 
 * grouping variables witch is to check the input, and Number is to limit the return value to all the aggregate
 * functions(-1) or only the aggregate functions of a certain grouping variable(ex.0,1,2,3)
 */
    
	public ArrayList<GroupingVariable> getAggrFunction(){
		String S1;
		ArrayList<GroupingVariable> aggregateFunc = new ArrayList<GroupingVariable>();
		try {
		       File f1 = new File(this.filePath);   //find input file
		       BufferedReader input = new BufferedReader(new FileReader(f1));  //read input from file
		       
		       String[] F_Vect = null;  //save the num of grouping variables
		       while ((S1 = input.readLine()) != null) {
		    	   /**Set data type for grouping variables**/
		    	   if(S1.equals("F-VECT([F]):")){
		    		   String inputF = input.readLine();
		    			F_Vect = inputF.split(",");  //split the line
		    		    for(int j=0;j<F_Vect.length;j++){
		    		    
		    		    	F_Vect[j]=F_Vect[j].trim();
		    		    	String[] temp = F_Vect[j].split("_");
		    		    	for(int i=0;i<temp.length;i++){
		    		    		temp[i] = temp[i].trim();
		    		    	}
		    		    	GroupingVariable groupingVariable;
		    		    	if (temp.length == 2) {
		    		    		groupingVariable = new GroupingVariable(0, temp[0], temp[1]);
		    		    		if(temp[0].equals("count") || temp[0].equals("max") || temp[0].equals("min"))
		    						groupingVariable.setType("int");
		    		    		else 
		    		    			groupingVariable.setType("double");
		    		    	}
		    		    	else {
		    		    		groupingVariable = new GroupingVariable(Integer.valueOf(temp[0]).intValue(),temp[1],temp[2]);
		    		    		if(temp[1].equals("count") || temp[1].equals("max") || temp[1].equals("min"))
		    						groupingVariable.setType("int");
		    		    		else 
		    		    			groupingVariable.setType("double");
		    		    	}		    	    		    	
		    		    	aggregateFunc.add(groupingVariable);	    		    			    	
		    			}
		    	   }		    		  
		       }
		       input.close();
		  }
	      catch (Exception e) {
		       e.printStackTrace();
	      }
		return aggregateFunc;  //return all the aggregate functions
	}
	/*
	 * The function getGroupingVariableNumber is used to get the number of grouping variable number
	 */
	public int getGroupingVariableNumber(){
		String S1;		       
		int Number = 0;
		try {
		       File f1 = new File(this.filePath);   //find input file
		       BufferedReader input = new BufferedReader(new FileReader(f1));  //read input from file
		       while ((S1 = input.readLine()) != null) {
		    	  		    	
		    	   /**Set data type for grouping attributes**/
		    	   if(S1.equals("NUMBER OF GROUPING VARIABLES(n):")){
		    		    String InputN = input.readLine();
		    		    InputN = InputN.trim();
		    		    Number = Integer.valueOf(InputN).intValue();
		    	   }
		       }
		       input.close();
		}
		catch (Exception e) {
		       e.printStackTrace();
	    }
		return Number;
	}	
}

