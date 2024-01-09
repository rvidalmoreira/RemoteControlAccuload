package modelo;

public class ProdutoPercentualAccuload {
	private String id_bicos_produtos_percentual_accuload = "";
	private int id_bicos = 0;
	private String id_produtos = "";
	private String id_produtos_1 = "";
	private String id_produtos_2 = "";
	private String percentual_produtos_1 = "";
	private String percentual_produtos_2 = "";
	
	public int getId_bicos() {
		return id_bicos;
	}
	
	public void setId_bicos(int id_bicos) {
		this.id_bicos = id_bicos;
	}
	
	
	public String getPercentual_produtos_1() {
		return percentual_produtos_1;
	}

	public void setPercentual_produtos_1(String percentual_produtos_1) {
		this.percentual_produtos_1 = percentual_produtos_1;
	}

	public String getPercentual_produtos_2() {
		return percentual_produtos_2;
	}

	public void setPercentual_produtos_2(String percentual_produtos_2) {
		this.percentual_produtos_2 = percentual_produtos_2;
	}

	public String getId_produtos() {
		return id_produtos;
	}

	public void setId_produtos(String id_produtos) {
		this.id_produtos = id_produtos;
	}

	public String getId_bicos_produtos_percentual_accuload() {
		return id_bicos_produtos_percentual_accuload;
	}

	public void setId_bicos_produtos_percentual_accuload(String id_bicos_produtos_percentual_accuload) {
		this.id_bicos_produtos_percentual_accuload = id_bicos_produtos_percentual_accuload;
	}

	public String getId_produtos_1() {
		return id_produtos_1;
	}

	public void setId_produtos_1(String id_produtos_1) {
		this.id_produtos_1 = id_produtos_1;
	}

	public String getId_produtos_2() {
		return id_produtos_2;
	}

	public void setId_produtos_2(String id_produtos_2) {
		this.id_produtos_2 = id_produtos_2;
	}
	
	public String toString() {
		String str = new String("[id_bicos_produtos_percentual_accuload="+id_bicos_produtos_percentual_accuload+",id_bicos="+id_bicos+",id_produtos="
	+id_produtos+",id_produtos_1="+id_produtos_1+",id_produtos_2="+id_produtos_2+",percentual_produtos_1="+percentual_produtos_1+",percentual_produtos_2="+percentual_produtos_2+"]");
		return str;
	}
}
