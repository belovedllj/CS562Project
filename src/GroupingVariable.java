

public class GroupingVariable {
	public int Number;
	public String Operation;
	public String coAttribute;
	public String Type;
	public GroupingVariable(){}
	public GroupingVariable(int Number, String Operation, String Co_Attribute){
		this.Number = Number;
		this.Operation = Operation;
		this.coAttribute = Co_Attribute;
	}
	public void setType(String Type){
		this.Type = Type;
	}
}