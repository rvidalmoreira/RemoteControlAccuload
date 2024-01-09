package vistorias;

import java.sql.ResultSet;

import bd.Conexao;
import bd.ConexaoUnisystem;

/**
 * Classe que implementa a vistoria em veículo
 * @author d0d0
 * @since 30-01-2017
 */
public class VistoriaVeiculo {

	/**
	 * Indica se o sorteio já foi realizado.
	 */
    private String sorteio_ja_realizado = "N";
    /**
     * Indica se sorteado para vistoria.
     */
    private String sorteado = "N";
    /**
     * Data da vistoria.
     */
    private String data_vistoria = "00/00/0000";
    /**
     * Hora da vistoria.
     */
    private String hora_vistoria = "00:00:00";
    /**
     * Sequencial de vistoria.
     */
    private String sequencial = "000000/0000";
    /**
     * Indica se existe uma ordem vinculada.
     */
    private String ordem_existe = "N";
    /**
     * Indica se veículo está liberado.
     */
    //private String liberado = "N";
    private Boolean liberado = false;
    /**
     * Obtém se veículo está liberado
     * @return Se veículo está liberado
     */
    public Boolean getLiberado() {
        return liberado;
    }
    //public String getLiberado() {
      //  return liberado;
    //}

    /**
     * Obtém se existe ordem vinculada.
     * @return Se existe ordem vinculada.
     */
    public String getOrdem_existe() {
        return ordem_existe;
    }

    /**
     * Obtém se foi sorteado para vistoria.
     * @return Se foi sorteado para vistoria.
     */
    public String getFoi_sorteado() {
        if (!"000000/0000".equals(this.sequencial)) {
            return "S";
        }

        return "N";
    }

    /**
     * Obtém data da vistoria
     * @return data da vistoria
     */
    public String getData_vistoria() {
        return data_vistoria;
    }

    /**
     * Obtém hora da vistoria
     * @return hora da vistoria
     */
    public String getHora_vistoria() {
        return hora_vistoria;
    }

    /**
     * Obtém sequencial de vistoria
     * @return O sequencial de vistoria.
     */
    public String getSequencial() {
        return sequencial;
    }

    /**
     * Obtém se sorteado para vistoria
     * @return Se sorteado para vistoria
     */
    public String getSorteado() {
        return sorteado;
    }

    /**
     * obtém se sorteio já foi realizado
     * @return Se sorteio já foi realizado
     */
    public String getSorteio_ja_realizado() {
        return sorteio_ja_realizado;
    }

    /**
     * Cria um nova vistoria para o veículo
     * @param id_ordens Id da ordem
     * @param alertas Se <code>true</code>, assionará giroflex de alteratas
     * @param c Conexão com o banco
     * @throws Exception Caso ocorra alguma exceção
     */
    public VistoriaVeiculo(int id_ordens, boolean alertas, ConexaoUnisystem c) throws Exception {
    	int id_ordens_vinculada = 0;
    	boolean vistoria = false;
    
        // ordem existe?
    	//String[] parametros = new String[1]; // parametros da query

    	//parametros[0] = Integer.toString(id_ordens);
            
    	ResultSet retorno;

    	//retorno = c.getConexao().getRegistros("select id_ordens "
    	//		+ "from sacc.ordens "
    	//		+ "where id_ordens = ? and ativo = 'S'", parametros);
    	
    	retorno = c.getConexao().getRegistros(
    		    "SELECT id " +
    		    "FROM ordens " +
    		    "WHERE id = " + id_ordens + " AND ativo = true", 
    		    new String[] {}
    		);

    	if (retorno.next()) {
    		this.ordem_existe = "S";

    		// tem ordem vinculada?
    		//ResultSet retorno1 = c.getConexao().getRegistros("select id_ordens_vinculada "
    			//	+ "from sacc.ordens "
    				//+ "where id_ordens = ? and id_ordens_vinculada > 0 and ativo = 'S'", parametros);
    		
    		ResultSet retorno1 = c.getConexao().getRegistros(
        		    "SELECT \"ordemVinculadaId\" " +
        		    "FROM ordens " +
        		    "WHERE id = " + id_ordens + " AND \"ordemVinculadaId\" > 0 AND ativo = true", 
        		    new String[] {}
        		);

    		if (retorno1.next()) {    			
    			id_ordens_vinculada = retorno1.getInt(1);
    		}

    		// é vinculada de outra ordem?
    		//ResultSet retorno2 = c.getConexao().getRegistros("select id_ordens "
    			//	+ "from sacc.ordens "
    				//+ "where id_ordens_vinculada = ? and ativo = 'S'", parametros);
    		
    		ResultSet retorno2 = c.getConexao().getRegistros(
        		    "SELECT id " +
        		    "FROM ordens " +
        		    "WHERE \"ordemVinculadaId\" = "+id_ordens+" AND ativo = true", 
        		    new String[] {}
        		);

    		if (retorno2.next()) {    			

    			id_ordens_vinculada = retorno2.getInt(1);
    		}

    		// tem que fazer a vistoria de veículos?
    		//ResultSet retorno3 = c.getConexao().getRegistros("select tipos_de_ordem.vistoria_veiculo "
    			//	+ "from sacc.ordens "
    			//	+ "left outer join sacc.tipos_de_ordem on tipos_de_ordem.id_tipos_de_ordem = ordens.id_tipos_de_ordem "
    			//	+ "where ordens.id_ordens = ?", parametros);
    		
    		ResultSet retorno3 = c.getConexao().getRegistros(
        		    "SELECT tipos_ordem.vistoria_veiculo " +
        		    "FROM ordens " +
        		    "left outer join tipos_ordem on tipos_ordem.id = ordens.\"tipoOrdemId\" " +		
        		    "WHERE ordens.id = "+id_ordens, 
        		    new String[] {}
        		);

    		
    		if(retorno3.next()){
    			//if ("S".equals(retorno3.getString(1))){
    				//vistoria = true;
    			//}
    			if (retorno3.getBoolean(1)){
    				vistoria = true;
    			}
    		}
    		

    		if (id_ordens_vinculada > 0) {
    			//parametros[0] = Integer.toString(id_ordens_vinculada);

    			//ResultSet retorno4 = c.getConexao().getRegistros("select tipos_de_ordem.vistoria_veiculo "
    				//	+ "from sacc.ordens "
    				//	+ "left outer join sacc.tipos_de_ordem on tipos_de_ordem.id_tipos_de_ordem = ordens.id_tipos_de_ordem "
    				//	+ "where ordens.id_ordens = ?", parametros);
    			
    			ResultSet retorno4 = c.getConexao().getRegistros(
            		    "SELECT tipos_ordem.vistoria_veiculo " +
            		    "FROM ordens " +
            		    "left outer join tipos_ordem on tipos_ordem.id = ordens.\"tipoOrdemId\" " +		
            		    "WHERE ordens.id = "+id_ordens_vinculada, 
            		    new String[] {}
            		);
    			
    			if(retorno4.next()){
        			//if ("S".equals(retorno4.getString(1))){
        				//vistoria = true;
        			//}
        			if (retorno4.getBoolean(1)){
        				vistoria = true;
        			}
        		}    			
    		}

    		
    		if (vistoria) {
    			// o sorteio já ocorreu?
    			//parametros[0] = Integer.toString(id_ordens);

    			//ResultSet retorno5 = c.getConexao().getRegistros("select ordens.vistoria_veiculo, veiculos.vistoria, "
    				//	+ "ordens.id_veiculos "
    					//+ "from sacc.ordens "
    					//+ "left outer join sacc.veiculos on veiculos.id_veiculos = ordens.id_veiculos "
    					//+ "where ordens.id_ordens = ?", parametros);

    			ResultSet retorno5  = c.getConexao().getRegistros(
            		    "SELECT ordens.vistoria_veiculo, veiculos.vistoria, ordens.\"veiculoId\" "+
            		    "FROM ordens " +
            		    "left outer join veiculos on veiculos.id = ordens.\"veiculoId\" " +		
            		    "WHERE ordens.id = "+id_ordens, 
            		    new String[] {}
            		);
    			
    			retorno5.next();

    			if ("X".equals(retorno5.getString(1))) {
    				
                    /**
				     * UNIBRPOSC-154 - Utilização do parâmetro PERCENTUAL_VISTORIA_VEICULO
				     * @since 22/09/2020
				     * Os valores abaixo avaliados correspondem ao cadastro do veículo onde o usuário
				     * pode informar se o veículo será vistoriado sempre ("S".equals(lista5[1])) ou
				     * na próxima vistoria ("P".equals(lista5[1])).
				     * Sendo: lista5[1] ==>> veiculos.vistoria
                     */
    				// fazer o sorteio aleatório
    				int i = (int) (1 + Math.random() * 100);
    				if ((i <= this.getPercentualVistoriaVeiculo(c.getConexao())) || ("S".equals(retorno5.getString(2))) || ("P".equals(retorno5.getString(2)))) { // caiu no sorteio
    		    				
    					// sequencial
    					vistorias.Sequencial s = new vistorias.Sequencial("V", c);

    					//c.getConexao().executaSql("update sacc.ordens set vistoria_veiculo = 'S', "
    						//	+ "data_vistoria_veiculo = now(), hora_vistoria_veiculo = now(), "
    						//	+ "sequencial_da_vistoria_veiculo = '" + s.getSequencialAnoFormatado() + "', "
    						//	+ "liberado_vistoria_veiculo = 'N' "
    						//	+ "where id_ordens in ('" + id_ordens + "', '" + id_ordens_vinculada + "')", new String[0]);
    					
    					c.getConexao().executaSql("update ordens set vistoria_veiculo = 'S', "
    							+ "data_vistoria_veiculo = now(), hora_vistoria_veiculo = now(), "
    							+ "sequencial_da_vistoria_veiculo = '" + s.getSequencialAnoFormatado() + "', "
    							+ "liberado_vistoria_veiculo = false "
    							+ "where id in ('" + id_ordens + "', '" + id_ordens_vinculada + "')", new String[0]);

    					this.sorteado = "S";

    					// se a vistoria for próxima, muda para aleatória
    					if ("P".equals(retorno5.getString(2))) {
    						//c.getConexao().executaSql("update sacc.veiculos set vistoria = 'A' where id_veiculos = '" + retorno5.getString(3) + "'", new String[0]);
    						c.getConexao().executaSql("update veiculos set vistoria = 'A' where id = '" + retorno5.getString(3) + "'", new String[0]);
    			  	}
    				} else {
    					//c.getConexao().executaSql("update sacc.ordens set vistoria_veiculo = 'N', "
    						//	+ "data_vistoria_veiculo = now(), hora_vistoria_veiculo = now(), "
    						//	+ "sequencial_da_vistoria_veiculo = '000000/0000', liberado_vistoria_veiculo = 'S' "
    						//	+ "where id_ordens in ('" + id_ordens + "', '" + id_ordens_vinculada + "')", new String[0]);
    					c.getConexao().executaSql("update ordens set vistoria_veiculo = 'N', "
    							+ "data_vistoria_veiculo = now(), hora_vistoria_veiculo = now(), "
    							+ "sequencial_da_vistoria_veiculo = '000000/0000', liberado_vistoria_veiculo = true "
    							+ "where id in ('" + id_ordens + "', '" + id_ordens_vinculada + "')", new String[0]);
    					
    				}

    				new OperacaoRegistrarSituacao(id_ordens, "V", c);   				
    				
    			} else {
    				this.sorteio_ja_realizado = "S";
    			}
    		}

    		// dados da vistoria
    		//parametros[0] = Integer.toString(id_ordens);

    		//ResultSet retorno6 = c.getConexao().getRegistros("select data_vistoria_veiculo, "
    			//	+ "hora_vistoria_veiculo, sequencial_da_vistoria_veiculo, liberado_vistoria_veiculo "
    				//+ "from sacc.ordens "
    				//+ "where id_ordens = ?", parametros);
    		ResultSet retorno6 = c.getConexao().getRegistros("select data_vistoria_veiculo, "
    				+ " TO_CHAR(hora_vistoria_veiculo, 'HH24:MI:SS'), sequencial_da_vistoria_veiculo, liberado_vistoria_veiculo "
    				+ "from ordens "
    				+ "where id = "+id_ordens, new String[0]);

    		retorno6.next();

    		this.data_vistoria = retorno6.getString(1);
    		this.hora_vistoria = retorno6.getString(2);
    		this.sequencial = retorno6.getString(3);
    		//this.liberado = retorno6.getString(4);
    		this.liberado = retorno6.getBoolean(4);
    		
    		if (alertas) {
    			// aciona as luzes/giroflex
    			//if (("S".equals(getLiberado())) || ("N".equals(getFoi_sorteado()))) {
    			if ((getLiberado()) || ("N".equals(getFoi_sorteado()))) {	
    				//c.getConexao().executaSql("update sacc.sequencial_vistorias "
    					//	+ "set alerta2 = 'S' "
    						//+ "where tipo = 'V' and ano = year(now())", new String[0]);
    				c.getConexao().executaSql("update sequencial_vistorias "
    						+ "set alerta2 = true "
    						+ "where tipo = 'V' and ano = EXTRACT(YEAR FROM NOW())::VARCHAR", new String[0]);
    			} else {
    				//c.getConexao().executaSql("update sacc.sequencial_vistorias "
    					//	+ "set alerta1 = 'S' "
    					//	+ "where tipo = 'V' and ano = year(now())", new String[0]);
    				c.getConexao().executaSql("update sequencial_vistorias "
    						+ "set alerta1 = true "
    						+ "where tipo = 'V' and ano = EXTRACT(YEAR FROM NOW())::VARCHAR", new String[0]);
    			}
    		}

    	}
     
    }
    // END VistoriaVeiculo

    /**
     * UNIBRPOSC-154 - Utilização do parâmetro PERCENTUAL_VISTORIA_VEICULO
     * @since 22/09/2020
     * @return Valor da classe Parametros ou, se existir, parâmetro da tabela parametros_sistema.
     */
	private Integer getPercentualVistoriaVeiculo(Conexao conn) {
    	Integer retorno = 5; // Valor antigo da rotina
    	try {
    		String[] parametros = new String[1];
    		parametros[0] = "PERCENTUAL_VISTORIA_VEICULO";
    		//ResultSet resultado = 
            	//	conn.getRegistros(
            		//		"SELECT valor FROM sacc.parametros_sistema WHERE parametro = ? ",
            			//	parametros);
    		ResultSet resultado = 
            		conn.getRegistros(
            				"SELECT valor FROM parametros_sistema WHERE parametro = ? ",
            				parametros);
    		if(resultado != null && resultado.next())  {
    			String valor = resultado.getString(1);
    			// Retirando caracteres caso o parâmetro tenha o caracter '%'
    			valor = valor.replaceAll("[^0-9]", "");    			
    			retorno = Integer.valueOf(valor);
    		}
    		resultado.close();
    	} catch (Exception e) {
    		//logger.error("Falha na leitura do parâmetro \'PERCENTUAL_VISTORIA_VEICULO\'",e);
    	}
    	return retorno;
	}
    /***************************************************************************/
	
}