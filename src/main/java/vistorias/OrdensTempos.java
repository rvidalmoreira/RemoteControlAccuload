package vistorias;

import java.sql.ResultSet;

import bd.ConexaoUnisystem;

/**
 * Classe que implementa a Ordem do tempo
 * @author everton
 * @since 19-12-2016
 * @version 1.0
 */
public class OrdensTempos {

	/**
	 * Gera o tempo de ordem
	 * @param id_ordens ID da ordem
	 * @param c Base conectada
	 * @throws Exception 
	 */
    public OrdensTempos(int id_ordens, ConexaoUnisystem c) throws Exception {

    	// variáveis
    	ResultSet retorno;          
    	String tempo_inicial, tempo_final;
    	int tempo_de_espera = 0, tempo_de_operacao = 0, tempo_de_faturamento = 0, tempo_de_permanencia_total = 0;

    	String data_hora_fila;
    	String data_hora_operacao;
    	String data_hora_vistoria;
    	String data_hora_faturamento;            
            
    	// tempo de espera (FILA ATÉ OPERAÇÃO)
    	// inicial
    	//retorno = c.getConexao().getRegistros("select concat(data, ' ', hora) as data_hora "
    		//	+ "from sacc.ordens_situacoes "
    			//+ "where id_ordens = '" + id_ordens + "' and situacao = 'N' and ativo = 'S' "
    			//+ "order by data desc, hora desc limit 1", new String[0]);
    	
    	retorno = c.getConexao().getRegistros("select concat(data, ' ', hora) as data_hora "
    			+ "from ordens_situacoes "
    			+ "where \"ordemId\" = '" + id_ordens + "' and situacao = 'N' and ativo = true "
    			+ "order by data desc, hora desc limit 1", new String[0]);

    	if (retorno.next()) {
    		tempo_inicial = retorno.getString(1);  
    	} else {
    		tempo_inicial = "0000-00-00 00:00:00";
    	}
            
    	data_hora_fila = tempo_inicial;

    	// final
    	//retorno = c.getConexao().getRegistros("select concat(data, ' ', hora) as data_hora "
    		//	+ "from sacc.ordens_situacoes "
    		//	+ "where id_ordens = '" + id_ordens + "' and situacao = 'O' and ativo = 'S' "
    		//	+ "order by data desc, hora desc limit 1", new String[0]);

     	retorno = c.getConexao().getRegistros("select concat(data, ' ', hora) as data_hora "
    			+ "from ordens_situacoes "
    			+ "where \"ordemId\" = '" + id_ordens + "' and situacao = 'O' and ativo = true "
    			+ "order by data desc, hora desc limit 1", new String[0]);

    	if (retorno.next()) {
    		tempo_final = retorno.getString(1);
    	} else {
    		tempo_final = "0000-00-00 00:00:00";
    	}
            
    	data_hora_operacao = tempo_final;

    	// calcular tempo em segundos
    	if ((!"0000-00-00 00:00:00".equals(tempo_inicial)) && (!"0000-00-00 00:00:00".equals(tempo_final))) {
    		//retorno = c.getConexao().getRegistros("select UNIX_TIMESTAMP('" + tempo_final + "') - UNIX_TIMESTAMP('" + tempo_inicial + "')",
    			//	new String[0]);   
    		retorno = c.getConexao().getRegistros("SELECT EXTRACT(EPOCH FROM TIMESTAMP '" + tempo_final + "' - TIMESTAMP '" + tempo_inicial + "')",
				new String[0]);   
    		retorno.next();
    		tempo_de_espera = retorno.getInt(1);
    	}

    	// tempo de operação (OPERAÇÃO ATÉ VISTORIA OU FATURAMENTO)
    	// inicial
    	tempo_inicial = tempo_final;

    	// final
    	//retorno = c.getConexao().getRegistros("select concat(data, ' ', hora) as data_hora "
    		//	+ "from sacc.ordens_situacoes "
    		//	+ "where id_ordens = '" + id_ordens + "' and situacao = 'V' and ativo = 'S' "
    		//	+ "order by data desc, hora desc limit 1", new String[0]);

    	retorno = c.getConexao().getRegistros("select concat(data, ' ', hora) as data_hora "
    			+ "from ordens_situacoes "
    			+ "where \"ordemId\" = '" + id_ordens + "' and situacao = 'V' and ativo = true "
    			+ "order by data desc, hora desc limit 1", new String[0]);
    	
    	if (retorno.next()) {                
    		tempo_final = retorno.getString(1);
    	} else {
            	
    		//retorno = c.getConexao().getRegistros("select concat(data, ' ', hora) as data_hora "
    			//	+ "from sacc.ordens_situacoes "
    			//	+ "where id_ordens = '" + id_ordens + "' and situacao = 'F' and ativo = 'S' "
    			//	+ "order by data desc, hora desc limit 1", new String[0]);

     		retorno = c.getConexao().getRegistros("select concat(data, ' ', hora) as data_hora "
    				+ "from ordens_situacoes "
    				+ "where \"ordemId\" = '" + id_ordens + "' and situacao = 'F' and ativo = true "
    				+ "order by data desc, hora desc limit 1", new String[0]);

    		if (retorno.next()) {                   
    			tempo_final = retorno.getString(1);
    		} else {
    			tempo_final = "0000-00-00 00:00:00";
    		}
    	}
            
    	data_hora_vistoria = tempo_final;

    	// calcular tempo em segundos
    	if ((!"0000-00-00 00:00:00".equals(tempo_inicial)) && (!"0000-00-00 00:00:00".equals(tempo_final))) {
            	
    		//retorno = c.getConexao().getRegistros("select UNIX_TIMESTAMP('" + tempo_final + "') - UNIX_TIMESTAMP('" + tempo_inicial + "')", 
    			//	new String[0]);
    		retorno = c.getConexao().getRegistros("SELECT EXTRACT(EPOCH FROM TIMESTAMP '" + tempo_final + "' - TIMESTAMP '" + tempo_inicial + "')",
				new String[0]);   
    		retorno.next();
    		tempo_de_operacao = retorno.getInt(1);
    	}

    	// tempo de faturamento (VISTORIA ATÉ FATURAMENTO OU 0)
    	// inicial
    	tempo_inicial = tempo_final;

    	// final
    	//retorno = c.getConexao().getRegistros("select concat(data, ' ', hora) as data_hora "
    		//	+ "from sacc.ordens_situacoes "
    		//	+ "where id_ordens = '" + id_ordens + "' and situacao = 'F' and ativo = 'S' "
    		//	+ "order by data desc, hora desc limit 1", new String[0]);
    	
    	retorno = c.getConexao().getRegistros("select concat(data, ' ', hora) as data_hora "
    			+ "from ordens_situacoes "
    			+ "where \"ordemId\" = '" + id_ordens + "' and situacao = 'F' and ativo = true "
    			+ "order by data desc, hora desc limit 1", new String[0]);

    	if (retorno.next()) {                
    		tempo_final = retorno.getString(1);  
    	} else {
    		tempo_final = "0000-00-00 00:00:00";
    	}

    	// calcular tempo em segundos
    	if ((!"0000-00-00 00:00:00".equals(tempo_inicial)) && (!"0000-00-00 00:00:00".equals(tempo_final))) {
    		//retorno = c.getConexao().getRegistros("select UNIX_TIMESTAMP('" + tempo_final + "') - UNIX_TIMESTAMP('" + tempo_inicial + "')",
    			//	new String[0]);
    		retorno = c.getConexao().getRegistros("SELECT EXTRACT(EPOCH FROM TIMESTAMP '" + tempo_final + "' - TIMESTAMP '" + tempo_inicial + "')",
				new String[0]);
    		retorno.next();
    		tempo_de_faturamento = retorno.getInt(1);
    	}
            
    	data_hora_faturamento = tempo_final;

    	// tempo de permanência total (FILA ATÉ FATURAMENTO)
    	// inicial
    	//retorno = c.getConexao().getRegistros("select concat(data, ' ', hora) as data_hora "
    	//		+ "from sacc.ordens_situacoes "
    	//		+ "where id_ordens = '" + id_ordens + "' and situacao = 'N' and ativo = 'S' "
    	//		+ "order by data desc, hora desc limit 1", new String[0]);

    	retorno = c.getConexao().getRegistros("select concat(data, ' ', hora) as data_hora "
    			+ "from ordens_situacoes "
    			+ "where \"ordemId\" = '" + id_ordens + "' and situacao = 'N' and ativo = true "
    			+ "order by data desc, hora desc limit 1", new String[0]);
    	
    	if (retorno.next()) {                
    		tempo_inicial = retorno.getString(1);
    	} else {
    		tempo_inicial = "0000-00-00 00:00:00";
    	}

    	// final
    	//retorno = c.getConexao().getRegistros("select concat(data, ' ', hora) as data_hora "
    		//	+ "from sacc.ordens_situacoes "
    		//	+ "where id_ordens = '" + id_ordens + "' and situacao in ('F', 'V', 'C') and ativo = 'S' "
    		//	+ "order by data desc, hora desc limit 1", new String[0]);
    	
    	retorno = c.getConexao().getRegistros("select concat(data, ' ', hora) as data_hora "
    			+ "from ordens_situacoes "
    			+ "where \"ordemId\" = '" + id_ordens + "' and situacao in ('F', 'V', 'C') and ativo = true "
    			+ "order by data desc, hora desc limit 1", new String[0]);

    	if (retorno.next()) {
    		tempo_final = retorno.getString(1);
    	} else {
    		tempo_final = "0000-00-00 00:00:00";
    	}

    	// calcular tempo em segundos
    	if ((!"0000-00-00 00:00:00".equals(tempo_inicial)) && (!"0000-00-00 00:00:00".equals(tempo_final))) {
    		//retorno = c.getConexao().getRegistros("select UNIX_TIMESTAMP('" + tempo_final + "') - UNIX_TIMESTAMP('" + tempo_inicial + "')",
    			//	new String[0]);
    		retorno = c.getConexao().getRegistros("SELECT EXTRACT(EPOCH FROM TIMESTAMP '" + tempo_final + "' - TIMESTAMP '" + tempo_inicial + "')",
				new String[0]);   
    		retorno.next();
    		tempo_de_permanencia_total = retorno.getInt(1);
    	}

    	// zerar valores negativos
    	if (tempo_de_espera < 0) {
    		tempo_de_espera = 0;
    	}
            
    	if (tempo_de_operacao < 0) {
    		tempo_de_operacao = 0;
    	}
            
    	if (tempo_de_faturamento < 0) {
    		tempo_de_faturamento = 0;
    	}
            
    	if (tempo_de_permanencia_total < 0) {
    		tempo_de_permanencia_total = 0;
    	}

    	// registrar tempos
    	//c.getConexao().executaSql("insert into sacc.ordens_tempos (id_ordens_tempos, id_ordens, tempo_de_espera, "
    		//	+ "tempo_de_operacao, tempo_de_faturamento, tempo_de_permanencia_total, ativo, "
    		//	+ "data_hora_fila, data_hora_operacao, data_hora_vistoria, data_hora_faturamento) "
    		//	+ "values (0, '" + id_ordens + "', '" + tempo_de_espera + "', '" + tempo_de_operacao + "', "
    		//	+ "'" + tempo_de_faturamento + "', '" + tempo_de_permanencia_total + "', 'S', '" + data_hora_fila + "', '" + data_hora_operacao + "', '" + data_hora_vistoria + "', '" + data_hora_faturamento + "') "
    		//	+ "on duplicate key update tempo_de_espera = '" + tempo_de_espera + "', tempo_de_operacao = '" + tempo_de_operacao + "', tempo_de_faturamento = '" + tempo_de_faturamento + "', "
    		//	+ "tempo_de_permanencia_total = '" + tempo_de_permanencia_total + "', "
    		//	+ "data_hora_fila = '" + data_hora_fila + "', data_hora_operacao = '" + data_hora_operacao + "', "
    		//	+ "data_hora_vistoria = '" + data_hora_vistoria + "', data_hora_faturamento = '" + data_hora_faturamento + "'", new String[0]);
    	c.getConexao().executaSql(
    		    "INSERT INTO ordens_tempos (" +
    		    "   \"ordemId\", " +
    		    "   tempo_espera, " +
    		    "   tempo_operacao, " +
    		    "   tempo_faturamento, " +
    		    "   tempo_permanencia_total, " +
    		    "   ativo, " +
    		    "   data_hora_fila, " +
    		    "   data_hora_operacao, " +
    		    "   data_hora_vistoria, " +
    		    "   data_hora_faturamento" +
    		    ") " +
    		    "VALUES (" +
    		    "   '" + id_ordens + "', " +
    		    "   '" + tempo_de_espera + "', " +
    		    "   '" + tempo_de_operacao + "', " +
    		    "   '" + tempo_de_faturamento + "', " +
    		    "   '" + tempo_de_permanencia_total + "', " +
    		    "   true, " +
    		    "   " + (data_hora_fila.equals("0000-00-00 00:00:00") ? "NULL" : "'" + data_hora_fila + "'") + ", " +
    		    "   " + (data_hora_operacao.equals("0000-00-00 00:00:00") ? "NULL" : "'" + data_hora_operacao + "'") + ", " +
    		    "   " + (data_hora_vistoria.equals("0000-00-00 00:00:00") ? "NULL" : "'" + data_hora_vistoria + "'") + ", " +
    		    "   " + (data_hora_faturamento.equals("0000-00-00 00:00:00") ? "NULL" : "'" + data_hora_faturamento + "'") +
    		    ") " +
    		    "ON CONFLICT (\"ordemId\") DO UPDATE SET " +
    		    "   tempo_espera = '" + tempo_de_espera + "', " +
    		    "   tempo_operacao = '" + tempo_de_operacao + "', " +
    		    "   tempo_faturamento = '" + tempo_de_faturamento + "', " +
    		    "   tempo_permanencia_total = '" + tempo_de_permanencia_total + "', " +
    		    "   data_hora_fila = " + (data_hora_fila.equals("0000-00-00 00:00:00") ? "NULL" : "'" + data_hora_fila + "'") + ", " +
    		    "   data_hora_operacao = " + (data_hora_operacao.equals("0000-00-00 00:00:00") ? "NULL" : "'" + data_hora_operacao + "'") + ", " +
    		    "   data_hora_vistoria = " + (data_hora_vistoria.equals("0000-00-00 00:00:00") ? "NULL" : "'" + data_hora_vistoria + "'") + ", " +
    		    "   data_hora_faturamento = " + (data_hora_faturamento.equals("0000-00-00 00:00:00") ? "NULL" : "'" + data_hora_faturamento + "'"),
    		    new String[0]
    		);

    }
}