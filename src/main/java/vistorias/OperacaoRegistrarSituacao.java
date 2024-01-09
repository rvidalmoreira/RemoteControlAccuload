package vistorias;

import bd.ConexaoUnisystem;

/**
 * Classe que implementa a Operacao do Registro da situacao
 * @author everton
 * @since 19-12-2016
 * @version 1.0
 */
public class OperacaoRegistrarSituacao {

	/**
	 * Gera a Operacao da Situacao
	 * @param id_ordens ID da ordem
	 * @param situacao Situacao
	 * @param c Base conectada
	 * @throws Exception 
	 */
    public OperacaoRegistrarSituacao(int id_ordens, String situacao, ConexaoUnisystem c) throws Exception {

    	String data = c.getConexao().getData();            		
    	String hora = c.getConexao().getHora();

    	//String[] parametros = new String[2]; // parâmetros da query

    	//parametros[0] = Integer.toString(id_ordens);
    	//parametros[1] = situacao;

    	//c.getConexao().executaSql("insert into sacc.ordens_situacoes (id_ordens_situacoes, id_ordens, data, hora, situacao, ativo) "
          //          + "values (0, ?, '" + data + "', '" + hora + "', ?, 'S')", parametros);

    	c.getConexao().executaSql("insert into ordens_situacoes (\"ordemId\", data, hora, situacao, ativo) "
                + "values ("+id_ordens+", '" + data + "', '" + hora + "', '" + situacao + "', true)", new String[0]);

    	//parametros[0] = situacao;
    	//parametros[1] = Integer.toString(id_ordens);

    	//c.getConexao().executaSql("update sacc.ordens set situacao = ?, data_situacao = '" + data + "', hora_situacao = '" + hora + "' "
    		//	+ "where id_ordens = ?", parametros);
    	
    	c.getConexao().executaSql("update ordens set situacao = '" + situacao + "', data_situacao = '" + data + "', hora_situacao = '" + hora + "' "
    			+ "where id = "+id_ordens, new String[0]);
    	
    	 // tempos
    	 new OrdensTempos(id_ordens, c);

    }   
    
}