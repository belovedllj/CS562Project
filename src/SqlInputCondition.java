
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class SqlInputCondition {
	File file=null;
	BufferedReader input = null;
	String con_gv = null;     // Grouping variable number parsed from suchthat condition
	String con_ga = null;      // Grouping attribute parsed from suchthat condition
	String operend = null;     // operand parsed from suchthat condition
	String rightside = null;    // right side expression parsed from suchthat condition 
	boolean hasAggFun = false;  // whether the right side expression has aggregate function
	String havingPart1 = null;     // first part parsed from having condition
	String havingPart2 = null;     // first part parsed from having condition
	String havingPart3 = null;     // first part parsed from having condition
	String havingPart4 = null;    // first part parsed from having condition
	ArrayList<Data> list = null;  // store all the suchthat condition with Data type
	ArrayList<String[]> havingList = new ArrayList<String[]>();  // store all the having condition with String type

	 //Constructor to put input inside
	public SqlInputCondition(File file) throws IOException {
		this.file = file;
		this.list=this.readAndParse();
	}
	
	public SqlInputCondition(String filepath) throws IOException {
		this.file = new File(filepath);
		this.list=this.readAndParse();
	}
    
	/* Judge whether a condition has aggregate function on the right side */
	public boolean hasAggregateFun() {
		return this.hasAggFun;
	}
	

	// Method to get suchthat conditon based on grouping variable number
	public ArrayList<Data> getCondition(int num) {
		//this.readAndParse();
		ArrayList<Data> tempList = this.list;
		ArrayList<Data> resultList = new ArrayList<Data>();
		for (int i = 0; i < tempList.size(); i++) {
			int var = tempList.get(i).con_gv;
			if (num == var) {
				resultList.add(tempList.get(i));
			}
		}
		return resultList;
	}
  
	// Method to get number of condition for one specific grouping variable number
	public int numOfCondition(int num)  {
		ArrayList<Data> tempCondition = this.list;
		HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
		int count = 0;
		for (int i = 0; i < tempCondition.size(); i++) {
			int var = tempCondition.get(i).con_gv;
			if (map.containsKey(var)) {
				int j = map.get(var);
				j++;
				map.put(var, j);
			} else {
				count = 1;
				map.put(tempCondition.get(i).con_gv, count);
			}
		}
		return map.get(num);
	}
  
	// Method to get grouping variable for one specific grouping variable number
	public ArrayList<String> getAttr(int num) {
		ArrayList<String> resultAttribute = new ArrayList<String>();
		ArrayList<Data> tempAttribute = this.getCondition(num);
		for (Data data : tempAttribute) {
			if(!resultAttribute.contains(data.con_ga))
				resultAttribute.add(data.con_ga);
		}
		return resultAttribute;
	}
    
	// Method to get all having condition
	public ArrayList<String[]> getHavingConditon() {
		// this.ParseHaving(input);		
		//this.readAndParse();
		return this.havingList;
	}
     
	// Method to read and parse input file and initialize all the defined variables
	public ArrayList<Data>  readAndParse() throws IOException {
		ArrayList<Data> returnList=new ArrayList<Data>();
		this.input= new BufferedReader(new FileReader(this.file));
		String s1 = null;
		String temp[] = null;
		int i = 0;
		while ((s1 =this.input.readLine()) != null) {
			// System.out.println(s1+"s1");
			if (s1.equals("﻿SELECT CONDITION-VECT([σ]):")) {
				String line = null;
				while ((line =this. input.readLine()) != null
						&& !line.equals("Having:")) {
					// System.out.println(line);
					temp = line.split("\\.");
					this.con_gv = temp[0];
					// System.out.println(temp.length+" length");
					for (i = 0; i < temp[1].length(); i++) {
						if (!Character.isLetter(temp[1].charAt(i))) {
							this.con_ga = temp[1].substring(0, i);
							break;
						}
					}
					// System.out.println(Con_ga);
					if (!Character.isLetter(temp[1].charAt(i + 1))
							&& !Character.isDigit(temp[1].charAt(i + 1))
							&& temp[1].charAt(i + 1) != '"') {
						this.operend = temp[1].substring(i, i + 2);
					} else {
						this.operend = temp[1].substring(i, i + 1);
					}
					this.rightside = temp[1].substring(this.con_ga.length()
							+ this.operend.length());
					 //System.out.println(rightside+"rightside");
				      char c[]=rightside.toCharArray();
				      for (int j = 0; j < c.length; j++) {
						if(c[j]=='_'){
							this.hasAggFun=true;
						}
					}					
					Data data = new Data(Integer.parseInt(this.con_gv), this.con_ga,
							this.operend, this.rightside, this.hasAggFun);
					this.hasAggFun=false;
					returnList.add(data);
				}
				//System.out.println(line);
				String line2 = null;
				int record = 0;
				// read and parse having condition and initialize the four parts
				while ((line2 = this.input.readLine()) != null) {
					//System.out.println(line2);
					char[] c = line2.toCharArray();
					boolean flag = false;
					for (int j = 0; j < c.length; j++) {
						if (c[j] == '+' || c[j] == '-' || c[j] == '*'
								|| c[j] == '/') {
							this.havingPart1 = line2.substring(0, j);
							this.havingPart2 = line2.substring(j, j + 2);
							flag = true;
						} else if (c[j] == '>' || c[j] == '<') {
							this.havingPart3 = c[j + 1] == '=' ? line2.substring(
									j, j + 2) : String.valueOf(c[j]);
							record = c[j + 1] == '=' ? (j + 2) : (j + 1);
						} else if (c[j] == '=') {
							this.havingPart3 = String.valueOf(c[j]);
							record = j + 1;
						}
						this.havingPart4 = line2.substring(record);
					}

					if (!flag) {
						if (this.havingPart3.length() == 2) {
							this.havingPart1 = line2.substring(0, record - 2);
						} else {
							this.havingPart1 = line2.substring(0, record - 1);
						}
						    this.havingPart2 = null;
					}
					String[] temp1 = { this.havingPart1, this.havingPart2,
							this.havingPart3, this.havingPart4 };
					this.havingList.add(temp1);
					this.havingPart1 = null;
					this.havingPart2 = null;
					this.havingPart3 = null;
					this.havingPart4 = null;
				}
			}
		} 
		this.input.close();
		return returnList ;
		
	}

	 // Inner class to store one instance of suchthat condition
	class Data {
		int con_gv;
		String con_ga;
		String operand;
		String rightString;
		Boolean hasAggfunBoolean;
		
		public String getOperationSatement() {
			if (operand.equals("=")) {
				return con_ga + " == " + rightString;
			} else if (operand.equals("<>")) {
				return con_ga + " != " + rightString;
			}
			return con_ga + " " + operand + " " + rightString;
		}
		
		public String getPartialOperation() {
			if (operand.equals("=")) {
				return con_ga + " == ";
			} else if (operand.equals("<>")) {
				return con_ga + " != ";
			}
			return con_ga + " " + operand + " ";
		}

		public Data(int con_gv, String con_ga, String operand,
				String rightString, Boolean hasAggfunBoolean) {
			this.con_gv = con_gv;
			this.con_ga = con_ga;
			this.operand = operand;
			this.rightString = rightString;
			this.hasAggfunBoolean = hasAggfunBoolean;
		}
	}
}
