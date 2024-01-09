package vistorias;

import java.sql.ResultSet;

import bd.ConexaoUnisystem;
/**
 * Classe de manipulação de sequencial
 * @author d0d0
 * @since 30-01-2017
 */
public class Sequencial {

	/**
	 * Sequencial
	 */
    private int sequencial = 0;
    /**
     * Ano referente
     */
    private String ano = "0000";

    /**
     * Obtém o sequencial
     * @return O sequencial.
     */
    public int getSequencial() {
        return sequencial;
    }

    /**
     * Obtém o ano
     * @return o ano
     */
    public String getAno() {
        return ano;
    }
    
    /**
     * Obtém o sequencial e ano formatados
     * @return sequencial / ano
     */
    public String getSequencialAnoFormatado() {
        return String.format("%06d", sequencial) + "/" + this.ano;
    }

    /**
     * Cria um novo sequencial
     * @param tipo Tipo de squencial. V para Vistorias
     * @param c conexão com o banco
     * @throws Exception Caso ocorra alguma exceção
     */
    public Sequencial(String tipo, ConexaoUnisystem c) throws Exception {
                   
    	// lock tables
    	//c.getConexao().executaSql("lock tables sacc.sequencial_vistorias write", new String[0]);
    	c.getConexao().executaSql("BEGIN; LOCK TABLE sequencial_vistorias IN SHARE MODE;", new String[0]);

    	String[] parametros = new String[1]; // parâmetros da query

    	parametros[0] = tipo;

    	//ResultSet retorno = c.getConexao().getRegistros("select sequencial "
    		//	+ "from sacc.sequencial_vistorias "
    		//	+ "where tipo = ? and ano = year(now())", parametros);
    	
    	ResultSet retorno = c.getConexao().getRegistros("SELECT sequencial " +
                "FROM sequencial_vistorias " +
                "WHERE tipo = ? AND ano = EXTRACT(YEAR FROM NOW())::VARCHAR", parametros);


    	if (retorno.next()) {
    		//c.getConexao().executaSql("update sacc.sequencial_vistorias set sequencial = sequencial + 1 "
    			//	+ "where tipo = ? and ano = year(now())", parametros);
    		c.getConexao().executaSql("update sequencial_vistorias set sequencial = sequencial + 1 "
    				+ "where tipo = ? and ano = EXTRACT(YEAR FROM NOW())::VARCHAR", parametros);
    	} else {
    		//c.getConexao().executaSql("insert into sacc.sequencial_vistorias (id_sequencial_vistorias, "
    			//	+ "tipo, ano, sequencial, ativo) values (0, ?, year(now()), 1, 'S')", parametros);
    		c.getConexao().executaSql("insert into sequencial_vistorias (tipo, ano, sequencial, ativo) values (?, EXTRACT(YEAR FROM NOW())::VARCHAR, 1, true)", parametros);

    	}

    	// sequencial atualizado
    	//ResultSet retorno1 = c.getConexao().getRegistros("select sequencial, year(now()) "
    		//	+ "from sacc.sequencial_vistorias "
    			//+ "where tipo = ? and ano = year(now())", parametros);
    	ResultSet retorno1 = c.getConexao().getRegistros("select sequencial, EXTRACT(YEAR FROM NOW())::VARCHAR "
    			+ "from sequencial_vistorias "
    			+ "where tipo = ? and ano = EXTRACT(YEAR FROM NOW())::VARCHAR", parametros);
    	
    	retorno1.next();
            
    	this.sequencial = retorno1.getInt(1);
    	this.ano = retorno1.getString(2);

    	// unlock tables
    	//c.getConexao().executaSql("unlock tables", new String[0]);
    	c.getConexao().executaSql("COMMIT;", new String[0]);
    }
}