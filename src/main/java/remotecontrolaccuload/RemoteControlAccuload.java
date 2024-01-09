/*
 * RemoteControlAccuload.java
 * Copyright (c) Unibraspe.
 *
 * Este software é confidencial e propriedade da Unibraspe.
 * Não é permitida sua distribuição ou divulgação do seu conteúdo sem expressa autorização da Unibraspe.
 * Este arquivo contém informações proprietárias.
 */
package remotecontrolaccuload;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.Properties;

import org.slf4j.LoggerFactory;

import bd.Conexao;
import bd.ConexaoUnisystem;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import formatar.FormatarNumero;
import formatar.FormatarTexto;
import modelo.ProdutoPercentualAccuload;
import vistorias.ImprimirVistoriaVeiculo;
import vistorias.VistoriaVeiculo;

//import javax.transaction.InvalidTransactionException;

public class RemoteControlAccuload {
	public static Logger LOG;
	public static final String IP_192_168_10_22 = "192.168.10.22";
	public static final String IP_192_168_10_26 = "192.168.10.26";
	public static final String ID_GAS_C_IMP = "16";
	public static final String ID_GAS_A_IMP = "15";
	public static final String ID_GAS_C = "7";
	public static final String ID_GAS_A = "4";
	public static final String ID_EA = "5";
	public static final String ID_S500 = "1";
	public static final String ID_S500_B = "9";
	public static final String ID_S10_B = "14";
	public static final String ID_S10 = "13";
	public static final String ID_B100 = "6";
	public static final String ID_BICO_GAS_IMP_IP_192_168_10_22 = "7";
	public static final String ID_BICO_GAS_IMP_IP_192_168_10_26 = "11";
	public static final String ID_BICO_GAS_IP_192_168_10_22 = "25";
	public static final String ID_BICO_GAS_IP_192_168_10_26 = "26";
	public static final int TIPO_CARREGAR = 0;
	public static final int TIPO_COMPLETAR = 1;
	//public static final String DML_ATUALIZAR_PARAMS_FINAIS_CARREGAMENTO_ORDENS_CLIENTES = "UPDATE sacc.ordens_clientes SET temperatura_produtos_1 = ?, temperatura_produtos_2 = ?, temperatura_tanque_produtos_1 = ?, densidade_tanque_produtos_1 = ?, temperatura_tanque_produtos_2 = ?, densidade_tanque_produtos_2 = ?, id_produtos_1 = ?, id_produtos_2 = ?, id_bicos = ?, id_operadores = ?, hora_carregada = ?, qtd_carregada = ?, qtd_carregada_1 = ?, qtd_carregada_2 = ? WHERE id_ordens_clientes = ?";
	//public static final String DML_ATUALIZAR_PARAMS_FINAIS_CARREGAMENTO_SOMATIVO_ORDENS_CLIENTES = "UPDATE sacc.ordens_clientes SET temperatura_produtos_1 = ?, temperatura_produtos_2 = ?, temperatura_tanque_produtos_1 = ?, densidade_tanque_produtos_1 = ?, temperatura_tanque_produtos_2 = ?, densidade_tanque_produtos_2 = ?, id_produtos_1 = ?, id_produtos_2 = ?, id_bicos = ?, id_operadores = ?, hora_carregada = ?, qtd_carregada = qtd_carregada + ?, qtd_carregada_1 = qtd_carregada_1 + ?, qtd_carregada_2 = qtd_carregada_2 + ? WHERE id_ordens_clientes = ?";
	//public static final String DML_ATUALIZAR_PARAMS_FINAIS_COMPLEMENTO_ORDENS_CLIENTES = "UPDATE sacc.ordens_clientes SET temperatura_produtos_1 = ?, temperatura_produtos_2 = ?, temperatura_tanque_produtos_1 = ?, densidade_tanque_produtos_1 = ?, temperatura_tanque_produtos_2 = ?, densidade_tanque_produtos_2 = ?, id_produtos_1 = ?, id_produtos_2 = ?, id_bicos = ?, id_operadores = ?, hora_completada = ?, qtd_completada = qtd_completada + ?, qtd_completada_1 = qtd_completada_1 + ?, qtd_completada_2 = qtd_completada_2 + ? WHERE id_ordens_clientes = ?";
	//public static final String DML_CONSULTAR_TANQUE_CARREGAMENTO_ATIVO_POR_PRODUTO = "SELECT temperatura,densidade FROM sacc.tanques WHERE id_tanques =  (SELECT ptq.id_tanques FROM ( SELECT id_produtos, MAX(CONCAT(data_parametros,' ',hora_parametros)) AS datahora,id_tipos_de_ordem FROM sacc.parametros_tanque WHERE data_parametros BETWEEN DATE_ADD(CURDATE(), INTERVAL -3 DAY) AND CURDATE() AND id_produtos = ? AND id_tipos_de_ordem = 3 GROUP BY id_produtos,id_tipos_de_ordem) PreQuery , sacc.parametros_tanque ptq  WHERE PreQuery.datahora = CONCAT(ptq.data_parametros,' ',ptq.hora_parametros)  AND ptq.id_produtos = PreQuery.id_produtos  AND ptq.id_tipos_de_ordem = PreQuery.id_tipos_de_ordem)";
	
	public static final String ID_S10_B_11_PORCENTO = "17";
	public static final String ID_S10_B_12_PORCENTO = "18";	
	public static final String ID_S10_B_13_PORCENTO = "19";	
	public static final String ID_S10_B_14_PORCENTO = "20";	
	public static final String ID_S10_B_15_PORCENTO = "21";	
	
	public static final String ID_S500_B_11_PORCENTO = "22";
	public static final String ID_S500_B_12_PORCENTO = "23";	
	public static final String ID_S500_B_13_PORCENTO = "24";	
	public static final String ID_S500_B_14_PORCENTO = "25";	
	public static final String ID_S500_B_15_PORCENTO = "26";	
	
	static {
		Properties props = System.getProperties();
		props.setProperty("user.timezone", "America/Sao_Paulo");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Logger initLogger(String ip) {
		LoggerContext logCtx = (LoggerContext) LoggerFactory.getILoggerFactory();
		PatternLayoutEncoder logEncoder = new PatternLayoutEncoder();
		logEncoder.setContext(logCtx);
		logEncoder.setPattern("%d{dd-MM-yyyy HH:mm:ss} %-5p %c{1}:%L - %m%n");
		logEncoder.start();

		RollingFileAppender logFileAppender = new RollingFileAppender();
		logFileAppender.setContext(logCtx);
		logFileAppender.setName(ip);
		logFileAppender.setEncoder(logEncoder);
		logFileAppender.setAppend(true);
		logFileAppender.setFile("/var/log/tomcat8/" + ip + ".log");

		TimeBasedRollingPolicy logFilePolicy = new TimeBasedRollingPolicy();
		logFilePolicy.setContext(logCtx);
		logFilePolicy.setParent(logFileAppender);
		logFilePolicy.setFileNamePattern("/var/log/tomcat8/" + ip + "-%d{yyyyMMdd}.tar.gz");
		logFilePolicy.setMaxHistory(3);
		logFilePolicy.start();

		logFileAppender.setRollingPolicy(logFilePolicy);
		logFileAppender.start();

		Logger log = logCtx.getLogger(RemoteControlAccuload.class);
		log.setAdditive(false);
		log.setLevel(Level.INFO);
		log.addAppender(logFileAppender);

		return log;
	}

	public static String ultimaTecla(String ip, int porta, String endereco, Comando ca) {
		String gk = "";
		try {
			String resposta = ca.EnviaComando(ip, porta, endereco, "GK");
			if (resposta.startsWith("*01GK*")) {
				gk = resposta.substring(6);
			} else {
				LOG.error("[ACCULOAD] ULTIMA TECLA [resposta=" + resposta + "]");
			}
		} catch (Exception e) {
			LOG.error("[ACCULOAD] ULTIMA TECLA [exception=" + e.getLocalizedMessage() + "]");
		}
		return gk;
	}

	public static String valorDigitado(String ip, int porta, String endereco, Comando ca) {
		try {
			String resposta = ca.EnviaComando(ip, porta, endereco, "RK");
			if (resposta.startsWith("*01RK")) {
				return resposta.substring(6, resposta.length() - 1).trim();
			}
			LOG.info("FALHA FUNCAO VALORDIGITADO / IP: " + ip + " / COMANDO RK: " + resposta);
		} catch (Exception e) {
			LOG.info("ERRO VALORDIGITADO: " + e.getLocalizedMessage());
		}
		return "";
	}

	public static String gravaSenha(String ip, int porta, String endereco, Comando ca) {
		try {
			String resposta = ca.EnviaComando(ip, porta, endereco, "RK");
			if (resposta.startsWith("*01RK")) {
				return resposta.substring(6, resposta.length() - 1).trim();
			}
			LOG.info("FALHA FUNCAO VALORDIGITADO / IP: " + ip + " / COMANDO RK: " + resposta);
		} catch (Exception e) {
			LOG.info("ERRO VALORDIGITADO: " + e.getLocalizedMessage());
		}
		return "";
	}



	public static boolean teclaStop(String ip, int porta, String endereco, Comando ca) {
		try {
			ca.EnviaComando(ip, porta, endereco, "ET");

			String resposta = ca.EnviaComando(ip, porta, endereco, "AR");
			if (!resposta.equals("*01OK")) {
				LOG.info("FALHA FUNCAO TECLASTOP / IP: " + ip + " / COMANDO AR: " + resposta);

				return false;
			}
			resposta = ca.EnviaComando(ip, porta, endereco, "SP");
			if (!resposta.equals("*01OK")) {
				LOG.info("FALHA FUNCAO TECLASTOP / IP: " + ip + " / COMANDO SP: " + resposta);

				return false;
			}
			resposta = ca.EnviaComando(ip, porta, endereco, "DA");
			if (!resposta.equals("*01OK")) {
				LOG.info("FALHA FUNCAO TECLASTOP / IP: " + ip + " / COMANDO DA: " + resposta);

				return false;
			}
		} catch (Exception e) {
			LOG.error("ERRO TECLASTOP: " + e.getLocalizedMessage(),e);
		}
		return true;
	}

	public static boolean Pergunta(String ip, int porta, String endereco, Comando ca, String pergunta) {
		try {
			ca.EnviaComando(ip, porta, endereco, "DA");

			String resposta = ca.EnviaComando(ip, porta, endereco,
					"WD 060 " + FormatarTexto.espacos(pergunta, 29, 'E', ' ') + "&09");
			if (!resposta.equals("*01OK")) {
				LOG.info("FALHA FUNCAO PERGUNTA / IP: " + ip + " / COMANDO WD: " + resposta);

				return false;
			}
		} catch (Exception e) {
			LOG.error("ERRO PERGUNTA: " + e.getLocalizedMessage(),e);
		}
		return true;
	}

	public static boolean Mensagem(String ip, int porta, String endereco, Comando ca, String mensagem1,
			String mensagem2) {
		try {
			String resposta = ca.EnviaComando(ip, porta, endereco, "WB 003 " + mensagem1 + "&01");
			if (!resposta.equals("*01OK")) {
				LOG.info("FALHA FUNCAO MENSAGEM / IP: " + ip + " / COMANDO WB: " + resposta);
				return false;
			}
			resposta = ca.EnviaComando(ip, porta, endereco, "WC 003 " + mensagem2 + "&01");
			if (!resposta.equals("*01OK")) {
				LOG.info("FALHA FUNCAO MENSAGEM / IP: " + ip + " / COMANDO WC: " + resposta);
				return false;
			}
			Thread.sleep(3000L);
		} catch (Exception e) {
			LOG.info("ERRO MENSAGEM: " + e.getLocalizedMessage());
		}
		return true;
	}

	public static boolean MensagemEspecial(String ip, int porta, String endereco, Comando ca, String mensagem1,
			String mensagem2) {
		try {
			String resposta = ca.EnviaComando(ip, porta, endereco, "WB 030 " + mensagem1 + "&01");
			if (!resposta.equals("*01OK")) {
				LOG.info("FALHA FUNCAO MENSAGEM ESPECIAL / IP: " + ip + " / COMANDO WB: " + resposta);
				return false;
			}
			resposta = ca.EnviaComando(ip, porta, endereco, "WC 030 " + mensagem2 + "&01");
			if (!resposta.equals("*01OK")) {
				LOG.info("FALHA FUNCAO MENSAGEM ESPECIAL / IP: " + ip + " / COMANDO WC: " + resposta);
				return false;
			}
			Thread.sleep(30000L);
		} catch (Exception e) {
			LOG.info("ERRO MENSAGEM ESPECIAL: " + e.getLocalizedMessage());
		}
		return true;
	}

	public static boolean Erro(String ip, int porta, String endereco, Comando ca, String erro) {
		try {
			String resposta = ca.EnviaComando(ip, porta, endereco, "WB 010 " + erro + "&01");
			if (!resposta.equals("*01OK")) {
				LOG.info("FALHA FUNCAO ERRO / IP: " + ip + " / COMANDO WB: " + resposta);
				return false;
			}
			Thread.sleep(10000L);
		} catch (Exception e) {
			LOG.info("ERRO ERRO: " + e.getLocalizedMessage());
		}
		return true;
	}
	
	public static boolean CarregarBS(int id_bicos, String ip, int porta, String endereco, Comando ca, String receita,
			int volume, ConexaoUnisystem c, String id_produtos) {
		try {
			if("1".equals(receita)) {
				try {
					ProdutoPercentualAccuload ppa = consultarProdutoPercentualAccuload(id_bicos, Integer.parseInt(id_produtos), c.getConexao());
					ajustarPercentual(id_bicos,receita,ip,porta,endereco,ca,ppa);
				} catch(Exception ex) {
					LOG.info("[EX] Problemas na definicao de percentual");
				}
			}
			
			ajustarVazao(id_bicos, receita, ip, porta, endereco, ca, c);
			int volOrig = volume;

			volume = ajustarVolume(id_bicos, receita, volume, c);
			if (receita.equals("3")) {
				receita = "4";
			}
			String resposta = ca.EnviaComando(ip, porta, endereco, "AB " + receita);
			if (!resposta.equals("*01OK")) {
				LOG.info("FALHA FUNCAO CARREGAR / IP: " + ip + " / COMANDO AB: " + resposta);

				return false;
			}
			resposta = ca.EnviaComando(ip, porta, endereco, "SF " + volume);
			if (!resposta.equals("*01OK")) {
				LOG.info("FALHA FUNCAO CARREGAR / IP: " + ip + " / COMANDO SF: " + resposta);

				return false;
			}
			resposta = ca.EnviaComando(ip, porta, endereco, "SA");
			if (!resposta.equals("*01OK")) {
				LOG.info("FALHA FUNCAO CARREGAR / IP: " + ip + " / COMANDO SA: " + resposta);

				return false;
			}
			LOG.info("CARREGAR - AJUSTES REALIZADOS [volume=" + volume + ",volumeOrig=" + volOrig + ",receita="
					+ receita + "]");
		} catch (Exception e) {
			LOG.info("ERRO CARREGAR: " + e.getLocalizedMessage());
		}
		return true;
	}
	
	
	public static boolean Carregar(int id_bicos, String ip, int porta, String endereco, Comando ca, String receita,
			int volume, ConexaoUnisystem c) {
		try {
			ajustarVazao(id_bicos, receita, ip, porta, endereco, ca, c);
			int volOrig = volume;

			volume = ajustarVolume(id_bicos, receita, volume, c);
			if (receita.equals("3")) {
				receita = "4";
			}
			String resposta = ca.EnviaComando(ip, porta, endereco, "AB " + receita);
			if (!resposta.equals("*01OK")) {
				LOG.info("FALHA FUNCAO CARREGAR / IP: " + ip + " / COMANDO AB: " + resposta);

				return false;
			}
			resposta = ca.EnviaComando(ip, porta, endereco, "SF " + volume);
			if (!resposta.equals("*01OK")) {
				LOG.info("FALHA FUNCAO CARREGAR / IP: " + ip + " / COMANDO SF: " + resposta);

				return false;
			}
			resposta = ca.EnviaComando(ip, porta, endereco, "SA");
			if (!resposta.equals("*01OK")) {
				LOG.info("FALHA FUNCAO CARREGAR / IP: " + ip + " / COMANDO SA: " + resposta);

				return false;
			}
			LOG.info("CARREGAR - AJUSTES REALIZADOS [volume=" + volume + ",volumeOrig=" + volOrig + ",receita="
					+ receita + "]");
		} catch (Exception e) {
			LOG.info("ERRO CARREGAR: " + e.getLocalizedMessage());
		}
		return true;
	}

	public static boolean mensagemTimeOut(String ip, int porta, String endereco, Comando ca) {
		try {
			return ca.EnviaComando(ip, porta, endereco, "RS").contains("TO");
		} catch (Exception e) {
			LOG.info("ERRO MENSAGEMTIMEOUT: " + e.getLocalizedMessage());
		}
		return false;
	}

	public static boolean dadosDigitadosPendentes(String ip, int porta, String endereco, Comando ca) {
		try {
			return ca.EnviaComando(ip, porta, endereco, "RS").contains("KY");
		} catch (Exception e) {
			LOG.info("ERRO DADOSDIGITADOSPENDENTES: " + e.getLocalizedMessage());
		}
		return false;
	}

	public static String saudacao(ConexaoUnisystem c) throws Exception {
		//ResultSet rsDados = c.getConexao().getRegistros(
			//	"select if(hour(now()) >= 5 and hour(now()) < 12, 'BOM DIA', if(hour(now()) >= 12 and hour(now()) < 18, 'BOA TARDE', 'BOA NOITE')) as saudacao",
				//new String[0]);
		ResultSet rsDados = c.getConexao().getRegistros(
			    "SELECT " +
			    "    CASE " +
			    "        WHEN EXTRACT(HOUR FROM NOW()) >= 5 AND EXTRACT(HOUR FROM NOW()) < 12 THEN 'BOM DIA' " +
			    "        WHEN EXTRACT(HOUR FROM NOW()) >= 12 AND EXTRACT(HOUR FROM NOW()) < 18 THEN 'BOA TARDE' " +
			    "        ELSE 'BOA NOITE' " +
			    "    END as saudacao",
			    new String[0]);

		if (rsDados.next()) {
			return rsDados.getString(1);
		}
		return "";
	}

	public static String nomeOperador(int operador, ConexaoUnisystem c) throws Exception {
		//String[] sParametros = new String[1];

		//sParametros[0] = Integer.toString(operador);

		//ResultSet rsDados = c.getConexao()
		//		.getRegistros("select codigo_automacao from sacc.operadores where id_operadores = ?", sParametros);
		ResultSet rsDados = c.getConexao()
				.getRegistros("select codigo_automacao from operadores where id = "+operador, new String[] {});
		if (rsDados.next()) {
			return rsDados.getString(1).substring(0, Math.min(29, rsDados.getString(1).length()));
		}
		return "";
	}

	public static boolean validarOperador(int operador, ConexaoUnisystem c) throws Exception {
		//String[] sParametros = new String[1];

		//sParametros[0] = Integer.toString(operador);

		//ResultSet rsDados = c.getConexao().getRegistros(
			//	"select operadores.id_operadores from sacc.operadores left outer join sacc.pessoas on pessoas.id_pessoas = operadores.id_pessoas where operadores.id_operadores = ? and operadores.ativo = 'S' and pessoas.ativo = 'S'",
				//sParametros);
		ResultSet rsDados = c.getConexao().getRegistros(
				"SELECT operadores.\"pessoaId\" " +
                "FROM operadores " +
                "LEFT OUTER JOIN pessoas ON pessoas.id = operadores.\"pessoaId\" " +
                "WHERE operadores.id = " + operador + " " +
                "AND operadores.ativo = true " +
                "AND pessoas.ativo = true",
                new String[] {});
		if (rsDados.next()) {
			return true;
		}
		return false;
	}

	public static int validarSenha(String senha, ConexaoUnisystem c, int id_bicos) throws Exception {
		String where = "";
		//String[] sParametros = new String[1];
		if (senha.length() > 4) {
			//where = "where ordens_clientes.id_ordens_clientes = ? ";
			//sParametros[0] = senha.substring(0, senha.length() - 1);
			where = "where ordens_clientes.id = "+Integer.valueOf(senha.substring(0, senha.length() - 1)) + " ";
			
			String digito = digitoVerificador(senha.substring(0, senha.length() - 1));
			if (!senha.endsWith(digito)) {
				return 0;
			}
		} else {
			//where = "where ordens_clientes.senha = ? ";
			//sParametros[0] = senha;
			where = "where ordens_clientes.senha = '" + senha + "' ";
		}
		//String sql = "select ordens_clientes.id_ordens_clientes, ordens_clientes.id_bicos from sacc.ordens_clientes left outer join sacc.ordens on ordens.id_ordens = ordens_clientes.id_ordens left outer join sacc.clientes on clientes.id_clientes = ordens_clientes.id_clientes left outer join sacc.pessoas on pessoas.id_pessoas = clientes.id_pessoas left outer join sacc.produtos on produtos.id_produtos = ordens_clientes.id_produtos left outer join sacc.veiculos_compartimentos on veiculos_compartimentos.id_veiculos_compartimentos = ordens_clientes.id_veiculos_compartimentos left outer join sacc.veiculos on veiculos.id_veiculos = veiculos_compartimentos.id_veiculos "
			//	+ where	+ "and ordens_clientes.ativo = 'S' and ordens.situacao = 'O' and ordens.ativo = 'S' and clientes.ativo = 'S' and pessoas.ativo = 'S' and produtos.ativo = 'S' and veiculos_compartimentos.ativo = 'S' and veiculos.ativo = 'S'";
		String sql = "SELECT " +
	              "  ordens_clientes.id, " +
	              "  ordens_clientes.\"bicoId\" " +
	              "FROM " +
	              "  ordens_clientes " +
	              "  LEFT OUTER JOIN ordens ON ordens.id = ordens_clientes.\"ordemId\" " +
	              "  LEFT OUTER JOIN clientes ON clientes.id = ordens_clientes.\"clienteId\" " +
	              "  LEFT OUTER JOIN pessoas ON pessoas.id = clientes.\"pessoaId\" " +
	              "  LEFT OUTER JOIN produtos ON produtos.id = ordens_clientes.\"produtoId\" " +
	              "  LEFT OUTER JOIN veiculos_compartimentos ON veiculos_compartimentos.id = ordens_clientes.\"veiculoCompartimentoId\" " +
	              "  LEFT OUTER JOIN veiculos ON veiculos.id = veiculos_compartimentos.\"veiculoId\" " +
	              where +
	              "  AND ordens_clientes.ativo = true " +
	              "  AND ordens.situacao = 'O' " +
	              "  AND ordens.ativo = true " +
	              "  AND clientes.ativo = true " +
	              "  AND pessoas.ativo = true " +
	              "  AND produtos.ativo = true " +
	              "  AND veiculos_compartimentos.ativo = true " +
	              "  AND veiculos.ativo = true";
		LOG.info("[DB] "+sql+" "+"[senha="+senha+"]");
		ResultSet rsDados = c.getConexao().getRegistros(sql, new String[0]);
		if (rsDados.next()) {
			return (rsDados.getInt(2) == 0) || (rsDados.getInt(2) == id_bicos) ? rsDados.getInt(1) : 0;
		}
		return 0;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static ArrayList<String> dadosSenha(String senha, ConexaoUnisystem c) throws Exception {
		//String[] sParametros = new String[1];
		ArrayList<String> retorno = new ArrayList();
		String where = "";
		if (senha.length() > 4) {
			//where = "where ordens_clientes.id_ordens_clientes = ? ";
			//sParametros[0] = senha.substring(0, senha.length() - 1);
			where = "where ordens_clientes.id = "+Integer.valueOf(senha.substring(0, senha.length() - 1)) + " ";
		} else {
			//where = "where ordens_clientes.senha = ? ";
			//sParametros[0] = senha;
			where = "where ordens_clientes.senha = '" + senha + "' ";
		}
		/*ResultSet rsDados = c.getConexao().getRegistros(
				"select "
				+"  ordens_clientes.id_ordens_clientes"         // 1
				+"  , ordens_clientes.id_ordens"                // 2
				+"  , ordens_clientes.id_clientes"              // 3
				+"  , pessoas.apelido_nome_fantasia as cliente" // 4
				+"  , ordens_clientes.id_produtos"              // 5
				+"  , produtos.apelido as produto"              // 6
				+"  , veiculos.placa"                           // 7
				+"  , lpad(veiculos_compartimentos.compartimento, 2, '0') as compartimento" // 8
				+"  , ordens_clientes.quantidade"               // 9
				+"  , veiculos.id_veiculos"                     // 10
				+"  , ordens_clientes.qtd_carregada"            // 11
				+"  , ordens_clientes.qtd_completada"           // 12
				+"  from sacc.ordens_clientes "
				+"       left outer join sacc.ordens "
				+"           on ordens.id_ordens = ordens_clientes.id_ordens "
				+"       left outer join sacc.clientes "
				+"           on clientes.id_clientes = ordens_clientes.id_clientes "
				+"       left outer join sacc.pessoas "
				+"           on pessoas.id_pessoas = clientes.id_pessoas "
				+"       left outer join sacc.produtos "
				+"           on produtos.id_produtos = ordens_clientes.id_produtos "
				+"       left outer join sacc.veiculos_compartimentos "
				+"           on veiculos_compartimentos.id_veiculos_compartimentos = ordens_clientes.id_veiculos_compartimentos "
				+"       left outer join sacc.veiculos "
				+"           on veiculos.id_veiculos = veiculos_compartimentos.id_veiculos "
				+ where
				+"  and ordens_clientes.ativo = 'S' "
				+"  and ordens.situacao = 'O' "
				+"  and ordens.ativo = 'S' "
				+"  and clientes.ativo = 'S' "
				+"  and pessoas.ativo = 'S' "
				+"  and produtos.ativo = 'S' "
				+"  and veiculos_compartimentos.ativo = 'S' "
				+"  and veiculos.ativo = 'S'",
				sParametros);*/
		ResultSet rsDados = c.getConexao().getRegistros("select " + "  ordens_clientes.id" // 1
				+ "  , ordens_clientes.\"ordemId\" " // 2
				+ "  , ordens_clientes.\"clienteId\" " // 3
				+ "  , pessoas.apelido_nome_fantasia as cliente" // 4
				+ "  , ordens_clientes.\"produtoId\" " // 5
				+ "  , produtos.apelido as produto" // 6
				+ "  , veiculos.placa" // 7
				+ "  , LPAD(veiculos_compartimentos.compartimento::text, 2, '0') AS compartimento " // 8
				+ "  , ordens_clientes.quantidade" // 9
				+ "  , veiculos.id" // 10
				+ "  , ordens_clientes.qtd_carregada" // 11
				+ "  , ordens_clientes.qtd_completada" // 12
				+ "  from ordens_clientes " + "       left outer join ordens "
				+ "           on ordens.id = ordens_clientes.\"ordemId\" "
				+ "       left outer join clientes "
				+ "           on clientes.id = ordens_clientes.\"clienteId\" "
				+ "       left outer join pessoas " + "           on pessoas.id = clientes.\"pessoaId\" "
				+ "       left outer join produtos "
				+ "           on produtos.id = ordens_clientes.\"produtoId\" "
				+ "       left outer join veiculos_compartimentos "
				+ "           on veiculos_compartimentos.id = ordens_clientes.\"veiculoCompartimentoId\" "
				+ "       left outer join veiculos "
				+ "           on veiculos.id = veiculos_compartimentos.\"veiculoId\" " + where
				+ "  and ordens_clientes.ativo = true " + "  and ordens.situacao = 'O' " + "  and ordens.ativo = true "
				+ "  and clientes.ativo = true " + "  and pessoas.ativo = true " + "  and produtos.ativo = true "
				+ "  and veiculos_compartimentos.ativo = true " + "  and veiculos.ativo = true", new String[0]);
		while (rsDados.next()) {
			retorno.add(rsDados.getString(1));
			retorno.add(rsDados.getString(2));
			retorno.add(rsDados.getString(3));
			retorno.add(rsDados.getString(4));
			retorno.add(rsDados.getString(5));
			retorno.add(rsDados.getString(6));
			retorno.add(rsDados.getString(7));
			retorno.add(rsDados.getString(8));
			retorno.add(rsDados.getString(9));
			retorno.add(rsDados.getString(10));
			retorno.add(rsDados.getString(11));
			retorno.add(rsDados.getString(12));
		}
		return retorno;
	}

	public static String digitoVerificador(String numero) {
		int soma = 0;
		for (int i = 0; i <= numero.length() - 1; i++) {
			soma += Integer.parseInt(numero.substring(i, i + 1)) * (i + 2);
		}
		soma %= 11;

		return Integer.toString(soma >= 10 ? 0 : soma);
	}

	public static String receitaBico(String ip, String id_produtos, ConexaoUnisystem c) throws Exception {
		//String[] sParametros = new String[4];

		//sParametros[0] = ip;
		//sParametros[1] = id_produtos;
		//sParametros[2] = id_produtos;
		//sParametros[3] = id_produtos;

		//ResultSet rsDados = c.getConexao().getRegistros(
			//	"select receita1, receita2, receita3 from bicos where ip = ? and (receita1 = ? or receita2 = ? or receita3 = ?) and ativo = 'S'",
			//	sParametros);
		ResultSet rsDados = c.getConexao().getRegistros(
				"select receita1, receita2, receita3 from bicos where ip = '" + ip + "' and (receita1 = "+id_produtos+" or receita2 = "+id_produtos+" or receita3 = "+id_produtos+") and ativo = true",
				new String[0]);
		while (rsDados.next()) {
			if (rsDados.getString(1).equals(id_produtos)) {
				return "1";
			}
			if (rsDados.getString(2).equals(id_produtos)) {
				return "2";
			}
			if (rsDados.getString(3).equals(id_produtos)) {
				return "3";
			}
		}
		return "0";
	}
	
	public static void ajustarPercentual(int id_bicos, String receita, String ip, int porta, String endereco, Comando ca, ProdutoPercentualAccuload ppa) throws Exception {
		LOG.info("ajuste percentual [ip="+ip+",porta="+porta+",endereco="+endereco+",percentual_1="+ppa.getPercentual_produtos_1()+",percentual_2="+ppa.getPercentual_produtos_2()+"]");
		ca.EnviaComando(ip, porta, endereco, "PC 01 005 " + ppa.getPercentual_produtos_1());
		ca.EnviaComando(ip, porta, endereco, "PC 01 007 " + ppa.getPercentual_produtos_2());
	}
	
	public static String receitaBicoBS(int id_bicos, String ip, String id_produtos, ConexaoUnisystem c) throws Exception {
		//String[] sParametros = new String[4];
		String resultado = "0";

		//sParametros[0] = ip;
		//sParametros[1] = id_produtos;
		//sParametros[2] = id_produtos;
		//sParametros[3] = id_produtos;

		//ResultSet rsDados = c.getConexao().getRegistros(
			//	"select receita1, receita2, receita3 from bicos where ip = ? and (receita1 = ? or receita2 = ? or receita3 = ?) and ativo = 'S' limit 1",
			//	sParametros);
		ResultSet rsDados = c.getConexao().getRegistros(
				"select receita1, receita2, receita3 from bicos where ip = '" + ip + "' and (receita1 = "+id_produtos+" or receita2 = "+id_produtos+" or receita3 = "+id_produtos+") and ativo = true limit 1",
				new String[0]);
		if (rsDados.next()) {
			if (rsDados.getString(2).equals(id_produtos)) {
				resultado = "2";
			} else if (rsDados.getString(3).equals(id_produtos)) {
				resultado = "3";
			} else {
				boolean bsVariavel = validarExistenciaProdutoBSVariavel(id_bicos, Integer.parseInt(id_produtos), c.getConexao());
				resultado = bsVariavel ? "1" : "0";
			}
		} else {
			boolean bsVariavel = validarExistenciaProdutoBSVariavel(id_bicos, Integer.parseInt(id_produtos), c.getConexao());
			resultado = bsVariavel ? "1" : "0";
		}
		LOG.info("receitaBicoBS resultado [ip="+ip+",receita="+resultado+",id_produtos="+id_produtos+",id_bicos="+id_bicos+"]");
		return resultado;
	}

	public static String receitaBico(int id_bicos, String ip, String id_produtos, ConexaoUnisystem c)
			throws Exception {
		//String[] sParametros = new String[5];

		//sParametros[0] = ip;
		//sParametros[1] = id_produtos;
		//sParametros[2] = id_produtos;
		//sParametros[3] = id_produtos;
		//sParametros[4] = Integer.toString(id_bicos);

		//ResultSet rsDados = c.getConexao().getRegistros(
			//	"select receita1, receita2, receita3 from bicos where ip = ? and (receita1 = ? or receita2 = ? or receita3 = ?) and id_bicos = ? and ativo = 'S'",
			//	sParametros);
		ResultSet rsDados = c.getConexao().getRegistros(
				"select receita1, receita2, receita3 from bicos where ip = '" + ip + "' and (receita1 = "+id_produtos+" or receita2 = "+id_produtos+" or receita3 = "+id_produtos+") and id = "+id_bicos+" and ativo = true",
				new String[0]);
		while (rsDados.next()) {
			if (rsDados.getString(1).equals(id_produtos)) {
				return "1";
			}
			if (rsDados.getString(2).equals(id_produtos)) {
				return "2";
			}
			if (rsDados.getString(3).equals(id_produtos)) {
				return "3";
			}
		}
		return "0";
	}

	/*public static String produtoDescarga(int id_ordens, ConexaoSupervisorio c) throws Exception {
		String[] sParametros = new String[1];

		sParametros[0] = Integer.toString(id_ordens);

		ResultSet rsDados = c.getConexao()
				.getRegistros("select id_produtos from sacc.ordens_clientes where id_ordens = ? limit 1", sParametros);
		if (rsDados.next()) {
			return rsDados.getString(1);
		}
		return "0";
	}*/

	public static boolean produtoUnico(String ip, ConexaoUnisystem c) throws Exception {
		String[] sParametros = new String[1];

		sParametros[0] = ip;

		//ResultSet rsDados = c.getConexao().getRegistros("select id_produtos2 from bicos where ip = ? and ativo = 'S'",
			//	sParametros);
		ResultSet rsDados = c.getConexao().getRegistros("select \"produto2Id\" from bicos where ip = ? and ativo = true",
				sParametros);
		while (rsDados.next()) {
			if (rsDados.getInt(1) > 0) {
				return false;
			}
		}
		return true;
	}

	public static String completarCom(String ip, ConexaoUnisystem c) throws Exception {
		String[] sParametros = new String[1];

		sParametros[0] = ip;

		//ResultSet rsDados = c.getConexao()
			//	.getRegistros("select receita_completar_b from bicos where ip = ? and ativo = 'S'", sParametros);
		ResultSet rsDados = c.getConexao()
				.getRegistros("select receita_completar_b from bicos where ip = ? and ativo = true", sParametros);
		if (rsDados.next()) {
			return rsDados.getString(1);
		}
		return "1";
	}

	public static void ajustarVazao(int id_bicos, String receita, String ip, int porta, String endereco, Comando ca,
			ConexaoUnisystem c) throws Exception {
		//ResultSet rsDados = c.getConexao().getRegistros("select receita" + receita + "_vazao_prod1, receita" + receita
		//		+ "_vazao_prod2 from bicos where id_bicos = " + id_bicos, new String[0]);
		ResultSet rsDados = c.getConexao().getRegistros("select receita" + receita + "_vazao_prod1, receita" + receita
				+ "_vazao_prod2 from bicos where id = " + id_bicos, new String[0]);
		while (rsDados.next()) {
			ca.EnviaComando(ip, porta, endereco, "PC P1 202 " + rsDados.getString(1));
			ca.EnviaComando(ip, porta, endereco, "PC P2 202 " + rsDados.getString(2));
		}
	}

	public static int ajustarVolume(int id_bicos, String receita, int volume, ConexaoUnisystem c) throws Exception {
		int[] valores = { 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000, 13000, 14000,
				15000, 16000, 17000, 18000, 19000, 20000, 21000, 22000, 23000, 24000, 25000, 26000, 27000, 28000, 29000,
				30000, 31000, 32000, 33000, 34000, 35000, 36000, 37000, 38000, 39000, 40000, 41000, 42000, 43000, 44000,
				45000, 46000, 47000, 48000, 49000, 50000 };

		int menorDiferenca = 999999;
		int valorMaisProximo = 0;
		for (int valor : valores) {
			int diferenca = Math.abs(valor - volume);
			if (diferenca < menorDiferenca) {
				menorDiferenca = diferenca;
				valorMaisProximo = valor;
			}
		}
		int ajuste1 = 0;
		int ajuste2 = 0;

		//ResultSet rsDados = c.getConexao().getRegistros("select ajuste_" + valorMaisProximo + "_prod1, ajuste_"
			//	+ valorMaisProximo + "_prod2 from bicos where id_bicos = " + id_bicos, new String[0]);
		ResultSet rsDados = c.getConexao().getRegistros("select ajuste_" + valorMaisProximo + "_prod1, ajuste_"
				+ valorMaisProximo + "_prod2 from bicos where id = " + id_bicos, new String[0]);
		while (rsDados.next()) {
			ajuste1 = rsDados.getInt(1);
			ajuste2 = rsDados.getInt(2);
		}
		if (volume != valorMaisProximo) {
			double percentual = volume / valorMaisProximo;

			ajuste1 = (int) Math.round(ajuste1 * percentual);
			ajuste2 = (int) Math.round(ajuste2 * percentual);
		}
		if (receita.equals("1")) {
			volume = volume + ajuste1 + ajuste2;
		}
		if (receita.equals("2")) {
			volume += ajuste1;
		}
		if (receita.equals("3")) {
			volume += ajuste2;
		}
		return volume;
	}

	/*public static boolean ordemDescarga(int id_ordens, ConexaoSupervisorio c) throws Exception {
		String[] sParametros = new String[1];

		sParametros[0] = Integer.toString(id_ordens);

		ResultSet rsDados = c.getConexao().getRegistros(
				"select ordens.id_ordens from sacc.ordens left outer join sacc.tipos_de_ordem on tipos_de_ordem.id_tipos_de_ordem = ordens.id_tipos_de_ordem where ordens.id_ordens = ? and ordens.situacao = 'O' and ordens.ativo = 'S' and tipos_de_ordem.descarga = 'S'",
				sParametros);
		if (rsDados.next()) {
			return true;
		}
		return false;
	}*/

	public static boolean ordemCarregamento(int id_ordens, ConexaoUnisystem c) throws Exception {
		String[] sParametros = new String[1];

		sParametros[0] = Integer.toString(id_ordens);
		LOG.info(sParametros[0]);
		//ResultSet rsDados = c.getConexao().getRegistros(
			//	"select ordens.id_ordens from sacc.ordens left outer join sacc.tipos_de_ordem on tipos_de_ordem.id_tipos_de_ordem = ordens.id_tipos_de_ordem where ordens.id_ordens = ? and ordens.ativo = 'S' and ordens.situacao in ('O','V') and tipos_de_ordem.carregamento = 'S'",
				//sParametros);
		ResultSet rsDados = c.getConexao().getRegistros(
			    "SELECT ordens.id " +
			    	    "FROM ordens " +
			    	    "LEFT OUTER JOIN tipos_ordem ON tipos_ordem.id = ordens.\"tipoOrdemId\" " +
			    	    "WHERE ordens.id = " + id_ordens + " " +
			    	    "AND ordens.ativo = true " +
			    	    "AND ordens.situacao IN ('O', 'V') " +
			    	    "AND tipos_ordem.carregamento = true",
			    	    new String[] {});
		if (rsDados.next()) {
			return true;
		}
		return false;
	}

	//gravar aqui o numero da ordem para capturar na transação RS 20230626
	public static boolean gravaOrdemMemo(int id_ordens_cliente,String ip, int porta, String endereco, Comando ca,ConexaoUnisystem c, String completa, int id_bicos) throws Exception {

				String repstran = ca.EnviaComando(ip, porta, endereco,"TN");
				LOG.info("GravaMemo - "+repstran);
				int tamanho = repstran.length();
				if(tamanho < 6){
					LOG.info("ERRROOOOO"+repstran);
					return true;
				}
				String numtran = repstran.substring(6, 10); //Recebe o numero de transacao para colocar
				int transacao = Integer.parseInt(numtran)+1;
				try {
					//String sql =
						//	"insert into supervisorio.transacoes_ordens_cliente(id_ordens_clientes, transacao, dt_inclusao, tipo,id_bicos) "+
						//			"values (?,?,curdate(),?,?)";
					String sql = "insert into transacoes_ordens_cliente(\"ordemClienteId\", transacao, tipo, \"bicoId\") "
							+ "values ("+id_ordens_cliente+", "+transacao+", '" + completa + "', "+id_bicos+")";
					//String[] sParametros = new String[4];
					//sParametros[0] = Integer.toString(id_ordens_cliente); //id_ordens_clientes
					//sParametros[1] = Integer.toString(transacao); //transacao
					//sParametros[2] = completa;//tipo
					//sParametros[3] = Integer.toString(id_bicos);//tipo
					c.getConexao().executaSql(sql, new String[0]);
					//LOG.info("GRAVANDO DADOS NA TABELA "+sql+"("+sParametros[0]+","+sParametros[1]+","+sParametros[2]+")");
					LOG.info("GRAVANDO DADOS NA TABELA "+sql+"("+id_ordens_cliente+","+transacao+","+completa+")");
				} catch (Exception e) {
					LOG.error("Falha no registro da ocorrencia",e);
				}

			return true;
	}
	public static String getDataHora(ConexaoUnisystem c) throws Exception {
		return c.getConexao().getData().substring(8, 10) + "/" + c.getConexao().getData().substring(5, 7) + " "
				+ c.getConexao().getHora().substring(0, 5);
	}

	public static String bolaPreta(String ordem, ConexaoUnisystem c) {
		String msg = "";
		try {
			VistoriaVeiculo v = new VistoriaVeiculo(Integer.parseInt(ordem), false, c);
			if ("S".equals(v.getSorteio_ja_realizado())) {
				if ("S".equals(v.getFoi_sorteado())) {
					msg = "BOLA PRETA";
				} else {
					msg = "BOLA VERDE";
				}
			} else if ("S".equals(v.getSorteado())) {
				msg = "BOLA PRETA";

				new ImprimirVistoriaVeiculo(Integer.parseInt(ordem), c);
				ProcessBuilder pb = new ProcessBuilder(
						new String[] { "/usr/bin/lp", "-d", "NotasFiscais", "/tmp/vistoria_" + ordem + ".pdf" });

				pb.redirectErrorStream(true);
				pb.start();
			} else if ("S".equals(v.getOrdem_existe())) {
				msg = "BOLA VERDE";
			} else {
				msg = "ORDEM NAO ENCONTRADA";
			}
		} catch (Exception e) {
			msg = e.getLocalizedMessage();
			LOG.info(e.getMessage());
			LOG.info("\n-------------------------------\n");
		}
		LOG.info(msg);
		return msg;
	}

	public static String[] contextualizarBico(int id_bicos, String id_produtos_1, String id_produtos_2, String ip,
			String id_produtos) {
		String[] paramBico = { Integer.toString(id_bicos), id_produtos_1, id_produtos_2 };
		if (IP_192_168_10_22.equals(ip)) {
			if (("4".equals(id_produtos)) || ("7".equals(id_produtos))) {
				paramBico[0] = "25";
				paramBico[1] = "4";
				paramBico[2] = "5";
			} else {
				paramBico[0] = "7";
				paramBico[1] = "15";
				paramBico[2] = "5";
			}
		} else if (IP_192_168_10_26.equals(ip)) {
			if (("4".equals(id_produtos)) || ("7".equals(id_produtos))) {
				paramBico[0] = "26";
				paramBico[1] = "4";
				paramBico[2] = "5";
			} else {
				paramBico[0] = "11";
				paramBico[1] = "15";
				paramBico[2] = "5";
			}
		}
		return paramBico;
	}

	public static void main(String[] args) {
		System.out.printf("Entrou");
		try {
			final String ip =args[0];
			LOG = initLogger(ip);

			LOG.info("UNIBRASPE Brasileira de Petroleo S/A");
			LOG.info("Remote Control Accuload v. 8.0.3 - Iniciado as: " + getHoraMinutoSegundoAtual());
			LOG.info("BICO: " + ip + "");

			//ConexaoSupervisorio c = new ConexaoSupervisorio();
			ConexaoUnisystem c = new ConexaoUnisystem();
			
			/**
			 * Recuperação dos dados para a paremetrização da execução com base na tabela de 
			 * bicos do Supervisório e do IP dessa execução
			 */
			/*ResultSet rsDados = c.getConexao().getRegistros(
					"select id_bicos, porta, endereco, percentual_completar, id_produtos1, id_produtos2 from bicos where ip = '"
							+ ip + "' and ativo = 'S' order by id_bicos desc limit 1",
					new String[0]);*/
			ResultSet rsDados = c.getConexao().getRegistros(
					"select id, porta, endereco, percentual_completar, \"produto1Id\", \"produto2Id\" from bicos where ip = '"
							+ ip + "' and ativo = true order by id desc limit 1",
					new String[0]);

			int id_bicos = 0;
			int porta = 7734;
			double percentual_completar = 0.0D;
			String endereco = "01";
			String id_produtos_1 = "";
			String id_produtos_2 = "";
			String idProdutoCompart = "";
			
			boolean produtounico = false;
			while (rsDados.next()) {
				id_bicos = rsDados.getInt(1);
				porta = rsDados.getInt(2);
				endereco = rsDados.getString(3);
				percentual_completar = rsDados.getDouble(4) / 100.0D;
				id_produtos_1 = rsDados.getString(5);
				id_produtos_2 = rsDados.getString(6);
			}
			if (id_produtos_2.equals("0")) {
				produtounico = true;
				LOG.info("PRODUTO UNICO");
			} else {
				LOG.info("PRODUTO NAO UNICO");
			}
			Comando ca = new Comando();
			/*Retirar depois*/
//			String resp = ca.EnviaComando(ip, porta, endereco, "PV SY 738");
//
//			if (resp.substring(13, 14).equals("3")) {
//				// alarm reset
//				ca.EnviaComando(ip, porta, endereco, "AR");
//
//				// remote stop
//				ca.EnviaComando(ip, porta, endereco, "SP");
//
//				// end batch
//				ca.EnviaComando(ip, porta, endereco, "EB");
//
//				// end transaction
//				ca.EnviaComando(ip, porta, endereco, "ET");
//
//				// release keyboard/display
//				ca.EnviaComando(ip, porta, endereco, "DA");
//
//				// mudo para poll & program
//
//				ca.EnviaComando(ip, porta, endereco, "PC SY 738 1");
//			} else {
//				// alarm reset
//				ca.EnviaComando(ip,porta, endereco, "AR");
//
//				// remote stop
//				ca.EnviaComando(ip, porta, endereco, "SP");
//
//				// end batch
//				ca.EnviaComando(ip, porta, endereco, "EB");
//
//				// end transaction
//				ca.EnviaComando(ip, porta, endereco, "ET");
//
//				// release keyboard/display
//				ca.EnviaComando(ip, porta, endereco, "DA");
//
//				// mudo para remote control
//				ca.EnviaComando(ip, porta, endereco, "PC SY 738 3");
//			}
//
//			// vejo se está em remote control
//			resp = ca.EnviaComando(ip, porta, endereco, "PV SY 738");
//			System.out.println(resp);
			/*Retirar depois*/



			int tempoEspera = 2000;

			/**
			 * Controle dos estágios da operação do equipamento. Atualmente os estágio são:
			 * 		0 - Aparentemente um tipo de espera de leitura
			 * 		1 - Leitura e validação do operador
			 * 		2 - Validação do carregamento, volume e receita
			 * 		3 - Dispara o comando para carregamento e atualiza a tabela ordens_clientes
			 * 		4 - Leitura dos volumes que serão carregados e atualiza a ordens_clientes
			 * 		5 - Completar o carregamento
			 * 		6 - Verifica a tela "P1" para retornar para o estágio zero
			 *		7 - Leitura do volume
			 *		8 - Retorna para o estágio ZERO - Em vistoria?
			 */
			int estagio = 0;
			
			int operador = 0;
			String senha = "";
			String receita = "";
			String ultimaTecla = "";
			int volume = 0;

			int completado = 0;
			int id_ordens_clientes = 0;
			String volume_batch_1 = "";
			String volume_batch_2 = "";
			String volume_batch = "";

			boolean inicio = true;
			
			final boolean tipoBicoBS = validarTipoBicoBS(ip,c.getConexao());

			/**
			 *		0 : Estado inicial da variável
			 *		1 : Atribuído pelos ESTÁGIO 1 ou 2 quando o operador informa F1 >> COMPLETAR
			 *		2 : Atribuído pelos ESTÁGIO 1 ou 2 quando o operador informa F2 >> VISTORIA
			 *		3 : Não existe
			 *		4 : Atribuí pelo ESTÁGIO 2 quando identifica-se que a senha informada 
			 *          trata-se de um número de ordem válido
			 */
			int tipo = 0;
			
			/**
			 * UNIBRPOSC-166 [DMD042] - Verificar se todas os compartimentos foram atuzalidados
			 */
			boolean isOcorreuVerificacaoCarga = false;
			
			/**
			 * UNIBRPOSC-248 - Recuperação da temperatura dos bicos de carregamento no SACC
			 */
			ZonedDateTime ultimaAvaliacao = null;
			boolean registrarTemperatura = false;
			// em segundos
			long intervaloEntreRegistroTemperatura = 60L;
			
			try {
				for (;;) {
					
					/**
					 * Estágio "INICIAL" - Diferente do estágio ZERO
					 * Execução da conclusão do CARREGAMENTO ou COMPLEMENTO
					 * 
					 * Se "estagio" > 0 e a última tecla for "S1"
					 *	Então
					 *		Se estágio é 4
					 *		Então
					 *			Se id_ordens_clientes > 0 e tiver dado em volume_batch
					 *			Então
					 *				Verifica os valores para volume_batch_1 e volume_batch_2
					 *				Identifica o tipo do carregamento
					 *				Recupera as temperaturas
					 *				Calcula a densidade
					 *				Executa atualizarOrdensClientes
					 *			Senão
					 *				Atualiza o bico e o operador da ordem do cliente
					 *		Estágio recebe ZERO
					 */
					if ((estagio > 0) && (ultimaTecla(ip, porta, endereco, ca).equals("S1"))) {
						
						if (estagio == 4) {
							
							if ((id_ordens_clientes > 0) && (volume_batch != null) && (volume_batch.length() > 0)) {
								
								if ((volume_batch_1 == null) || (volume_batch_1.length() < 1)) {
									volume_batch_1 = "0";
								}
								if ((volume_batch_1 == null) || (volume_batch_2.length() < 1)) {
									volume_batch_2 = "0";
								}
								
								// tipo == 0 : Estado inicial da variável
								String tipoProcedimento = tipo == 0 ? "CARREGAMENTO" : "COMPLEMENTO";

								LOG.info("SOLICITADO PARADA DE " + tipoProcedimento + " [id_ordens_clientes="
										+ id_ordens_clientes + ",tipo=" + tipo + ",id_bicos=" + id_bicos + ",receita="
										+ (receita != null ? receita : "") + ",id_produtos_1="
										+ (id_produtos_1 != null ? id_produtos_1 : "") + ",id_produtos_2="
										+ (id_produtos_2 != null ? id_produtos_2 : "") + ",completado=" + completado
										+ ",volume_batch=" + volume_batch + ",volume_batch_1=" + volume_batch_1
										+ ",volume_batch_2=" + volume_batch_2 + ",estagio=4,gk=S1]");

								String hora = getHoraMinutoSegundoAtual();

								c.getConexao().desconecta();

								double[] temperaturas = consultarTemperaturaCorrenteTransdutor(ip, porta, endereco,
										new Comando());

								c = new ConexaoUnisystem();

								double[] temperatura_densidade_tanque_produto_1 = consultarTemperaturaDensidadeTanqueAtivoCarregamentoPorProduto(
										id_produtos_1, c.getConexao());
								double[] temperatura_densidade_tanque_produto_2 = consultarTemperaturaDensidadeTanqueAtivoCarregamentoPorProduto(
										id_produtos_2, c.getConexao());

								atualizarOrdensClientes(0 == tipo, volume_batch, volume_batch_1, volume_batch_2,
										id_produtos_1, id_produtos_2, temperaturas,
										temperatura_densidade_tanque_produto_1, temperatura_densidade_tanque_produto_2,
										id_bicos, operador, id_ordens_clientes, c.getConexao(), hora, true);
								
								ultimaAvaliacao = null;
								
								LOG.info("TERMINO_ DE " + tipoProcedimento + " [id_ordens_clientes="
										+ id_ordens_clientes + ",senha=" + senha + ",gk=S1]");
								
							} else {
								
								/*String sql = "update sacc.ordens_clientes set id_bicos = " + id_bicos
										+ ", id_operadores = " + operador + " where id_ordens_clientes = "
										+ id_ordens_clientes;*/
								String sql = "UPDATE ordens_clientes SET " +
								        "\"bicoId\" = " + (id_bicos > 0 ? id_bicos : "NULL") + ", " +
								        "\"operadorId\" = " + (operador > 0 ? operador : "NULL") +
								        " WHERE id = " + id_ordens_clientes;
								c.getConexao().executaSql(sql, new String[0]);
								LOG.info("[DB] " + sql);
								
							} // END if ((id_ordens_clientes > 0) && (volume_batch != null) && (volume_batch.length() > 0))
							
							LOG.info("************************************************************************************************************************************************");
							
						} // END estagio == 4
						
						estagio = 0;
						
					}
					// END if ((estagio > 0) && (ultimaTecla(ip, porta, endereco, ca).equals("S1"))) {
					
					/**
					 * Estágio ZERO - Aparentemente um tipo de espera
					 * 
					 * Limpas as variáveis volume_batch, volume_batch_1 e volume_batch_2.
					 * 
					 * Se a variável "inicio" for True
					 * 		Então "inicio" recebe False
					 * 
					 * Se teclaStop for True
					 * 		Então 
					 * 			tipo = 0
					 * 			estágio recebe 1
					 * 		Senão
					 * 			inicio = True
					 * 			estágio recebe 0
					 * 
					 */
					if (estagio == 0) {
						volume_batch = "";
						volume_batch_1 = "";
						volume_batch_2 = "";
						if (inicio) {
							Thread.sleep(10000L);
							inicio = false;
						}
						if (teclaStop(ip, porta, endereco, ca)) {
							tipo = 0;
							estagio = 1;
						} else {
							inicio = true;
							estagio = 0;
						}
						/**
						 * UNIBRPOSC-166 [DMD042] - Verificar se todas os compartimentos foram atuzalidados
						 */
						isOcorreuVerificacaoCarga = false;
					}
					// END estagio == 0
					
					/**
					 * Estágio 1 - Leitura e validação do operador
					 * 
					 * Limpas as variáveis volume_batch, volume_batch_1 e volume_batch_2.
					 * 
					 * Se a resposta não for OPERADOR
					 * 	Então estágio recebe ZERO
					 * 
					 * Se a variável "tipo" <> 1 e a última tecla for "F1"
					 * 	Então a variável "tipo" recebe 1
					 * 
					 * Se a variável "tipo" <> 2 e a última tecla for "F2"
					 * 	Então a variável "tipo" recebe 2
					 * 
					 * Se o houve valor digitado para identificar o operador
					 * 	Então
					 * 
					 * 		A variável "especial" recebe true se o valor digitado for 777 ou 888 ou 999
					 * 			>> Há uma mensagem especial para cada um desses casos
					 * 
					 * 		Se "especial" for FALSE
					 * 		Então
					 * 			Se valor digitado > 0 e for um operador válido
					 * 			Então
					 * 				Se o operador NÃO confirmar as mensagens enviadas
					 * 				Então estágio recebe ZERO
					 * 				Senão 
					 * 					Operador foi validado
					 * 					Estágio recebe 2
					 * 			Senão
					 * 				Informa erro de operador inválido
					 * 				Estágio recebe ZERO
					 *  
					 * 		Senão estágio recebe ZERO
					 * 
					 * 	Senão estágio recebe ZERO
					 * 
					 */
					if (estagio == 1) {
						
						volume_batch = "";
						volume_batch_1 = "";
						volume_batch_2 = "";

						c.getConexao().desconecta();
						c = new ConexaoUnisystem();
						
						/**
						 * Se a resposta não for OPERADOR 
						 * Então estagio recebe zero 
						 */
						LOG.info("[OPERADOR?] ");

						if (!Pergunta(ip, porta, endereco, ca, getDataHora(c) + " OPERADOR?")) {
							estagio = 0;
						}
						
						/**
						 * Aguardando a leitura do equipamento
						 * Se a variável "tipo" <> 1 e a última tecla for "F1"
						 * 	Então tipo recebe 1
						 * Se a variável "tipo" <> 2 e a última tecla for "F2"
						 * 	Então tipo recebe 2
						 */						
						do {
							Thread.sleep(tempoEspera);
							if (mensagemTimeOut(ip, porta, endereco, ca)) {
								estagio = 0;
								break;
							}
							ultimaTecla = ultimaTecla(ip, porta, endereco, ca);
							if ((tipo != 1) && (ultimaTecla.equals("F1"))) {
								tipo = 1;
								LOG.info("DEFININDO COMPLETAR [estagio=" + estagio + ",gk=F1,tipo=1]");
							}
							if ((tipo != 2) && (ultimaTecla.equals("F2"))) {
								tipo = 2;
								LOG.info("DEFININDO VISTORIA [estagio=" + estagio + ",gk=F2,tipo=2]");
							}
						} while ((!dadosDigitadosPendentes(ip, porta, endereco, ca)) || (!ultimaTecla.equals("E1")));
						/*********************************************************************************************/
						
						String valorDigitado = valorDigitado(ip, porta, endereco, ca);
						LOG.info("LENDO DADOS DE OPERADOR [estagio=" + estagio + ",gk=E1,valorDigitado=" + valorDigitado
								+ "]");
						
						/**
						 * Verifica se houve valor informado
						 */
						if (!valorDigitado.equals("")) {
							
							operador = Integer.parseInt(valorDigitado);

							boolean especial = false;
							
							/**
							 * Verifica se trata-se dos valores 777 ou 888 ou 999
							 */
							if (operador == 777) {
								especial = true;
								LOG.info("ENVIANDO MENSAGEM ESPECIFICA OPERADOR [operador=" + operador + ",especial="
										+ especial + "]");
								MensagemEspecial(ip, porta, endereco, ca, "S10  20,1C 0,8888g/mL",
										"B100 25,3C 0,9999g/mL");
							}
							if (operador == 888) {
								especial = true;
								LOG.info("ENVIANDO MENSAGEM ESPECIFICA OPERADOR [operador=" + operador + ",especial="
										+ especial + "]");
								MensagemEspecial(ip, porta, endereco, ca, "S10  456.456.456.456.456",
										"B100 456.456.456.456.456");
							}
							if (operador == 999) {
								especial = true;
								LOG.info("ENVIANDO MENSAGEM ESPECIFICA OPERADOR [operador=" + operador + ",especial="
										+ especial + "]");
								MensagemEspecial(ip, porta, endereco, ca, "456.456.456.456.456", "");
							}
							/*****************************************************************************/
							
							if (!especial) {
								if ((operador > 0) && (validarOperador(operador, c))) {
									if (!Mensagem(ip, porta, endereco, ca, saudacao(c), nomeOperador(operador, c))) {
										estagio = 0;
									} else {
										estagio = 2;
										LOG.info("OPERADOR VALIDADO [operador=" + operador + ",estagio=" + estagio + "]");
									}
								} else {
									Erro(ip, porta, endereco, ca, "OPERADOR INVALIDO");
									estagio = 0;
									LOG.info("OPERADOR INVALIDO [operador=" + operador + ",estagio=" + estagio + "]");
								}
							} else {
								
								// Se for "operador especial" retorna o estágio para zero
								estagio = 0;
								
							} // if (!especial)
							
						} else {
							
							// Se não foi lido valor do equipamento o estágio retorna para ZERO
							estagio = 0;
							
						} // END if (!valorDigitado.equals(""))
					}
					// END estagio == 1
					
					/**
					 * Estágio 2 -Validação do carregamento, volume e receita
					 *  
					 * Limpas as variáveis volume_batch, volume_batch_1 e volume_batch_2.
					 * 
					 * Verifica a variável "tipo" e das RESPOSTAS
					 * 
					 * Dentro de um for(;;)
					 * 
					 * 		Se ocorrer time-out 
					 * 		Então estágio recebe ZERO
					 * 
					 * 		Leitura da ultimaTecla 
					 * 
					 * 		Se a ultimaTecla = "F1"		
					 * 		Então estágio recebe ZERO
					 * 
					 * 		Se "tipo" != 1 e ultimaTecla = "F1")
					 * 		Então
					 * 			"tipo" recebe 1
					 * 			Se a resposta não for "COMPLETAR SENHA"
					 *	 		Então estágio recebe ZERO
					 *
					 *		Se há dadosDigitadosPendentes() e ultimaTecla = "E1"
					 *		Então
					 *			recupera o "valorDigitado"
					 *			Se há valor no "valorDigitado"
					 *			Então
					 *				variável "senha" recebe o "valorDigitado"
					 *				Se a senha for de uma ordem de carregamento
					 *				Então
					 *					Se o "tipo" = 2
					 *					Então
					 *						Se não isOcorreuVerificacaoCarga 
					 *						Então
					 *							Se há compartimento para ser carregado
					 *							Então 
					 *								Se o operador "FINALIZA SEM CONCLUIR SENHAS?" 
					 *								Então
					 *									registra na tabela de ocorrências
					 *								Senão
					 *			 						estágio recebe 6
					 * 									break
					 *							isOcorreuVerificacaoCarga = true
					 *
					 * 						verifica se haverá inspeção (Bola Preta)
					 * 						estágio recebe 6
					 * 						break
					 * 
					 * 						verifica se haverá inspeção (Bola Preta)
					 * 						estágio recebe 6
					 * 						break
					 * 
					 * 					tipo recebe 4
					 * 					estágio recebe 8 
					 *				Tenta recuperar o id_ordens com base na senha do carregamento
					 *				Se há "senha" e o "tipo" <= 2 e id_ordens > 0 
					 *				Então
					 *					
					 *					Se "tipo" igual ZERO
					 *					Então
					 *						"percent" = 100.0D * ordens_clientes.qtd_carregada / ordens_clientes.quantidade
					 *   
					 *					Se "percent" < 99%
					 *					Então
					 *
					 *						"volume"     => ordens_clientes.quantidade
					 *						"completado" => ordens_clientes.qtd_completada"
					 *
					 *						Se "tipo" == 0 
					 *						Então "volume" -=  ordens_clientes.qtd_carregada
					 *
					 *						Recuperação da receita
					 *
					 *						Se "receita" == "0"
					 *						Então
					 *							Emite a mensagem "PRODUTO INVALIDO"
			 		 *						  	estágio recebe 2
					 *							break;
					 *
					 *						Se "volume" <= 0
					 *						Então
					 *							Emite a mensagem "VOLUME INVALIDO"
			 		 *						  	estágio recebe 2
					 *							break;
					 *
					 *						Se "tipo" == 0
					 *						Então 
					 *							Sistema gera mensagens contendo:
					 *								id_cliente + / + apelido do produto + "C" + placa  
					 *								número do compartimento + / + id_produto
					 *
					 *							Se o operador NÃO confirmar as mensagens
					 *							Então 
			 		 *							  	estágio recebe 0
					 *								break;
					 *
			 		 *						  	estágio recebe 3
					 *							break;
					 *
					 *						Se "tipo" == 1
					 *						Então 
			 		 *						  	estágio recebe 5
					 *							break;
					 *
					 *					Senão
					 *						Emite mensagem COMPARTIMENTO JA CARREGADO
					 *
					 *				Senão
					 *					emite mensagem "SENHA INVALIDA"
					 *				  	estágio recebe 2
					 *					break;
					 *
					 * 			Senão 
					 * 				estágio recebe ZERO
					 * 				break
					 *			
					 */
					if (estagio == 2) {
						
						volume_batch = "";
						volume_batch_1 = "";
						volume_batch_2 = "";
						
						/**
						 * Verificação da variável tipo e das RESPOSTAS
						 * Se "tipo" == 0 e a resposta NÃO for SENHA
						 * Então estágio recebe ZERO
						 * Se "tipo" == 1 e a resposta NÃO for COMPLETAR SENHA
						 * Então estágio recebe ZERO
						 * Se "tipo" == 2 e a resposta NÃO for VISTORIA
						 * Então estágio recebe ZERO
						 */
						// tipo == 0 : Estado inicial da variável
						if ((tipo == 0) && (!Pergunta(ip, porta, endereco, ca, getDataHora(c) + " SENHA?"))) {
							estagio = 0;
						}
						//	tipo == 1 : Atribuído pelos ESTÁGIO 1 ou 2 quando o operador informa F1 >> COMPLETAR												
						if ((tipo == 1) && (!Pergunta(ip, porta, endereco, ca, getDataHora(c) + " COMPLETAR SENHA?"))) {
							estagio = 0;
						}
						// tipo == 2 : Atribuído pelos ESTÁGIO 1 ou 2 quando o operador informa F2 >> VISTORIA
						if ((tipo == 2) && (!Pergunta(ip, porta, endereco, ca, getDataHora(c) + " VISTORIA?"))) {
							estagio = 0;
						}
						/**********************************************************************************************/
						
						for (;;) {
							
							Thread.sleep(tempoEspera);
							
							if (mensagemTimeOut(ip, porta, endereco, ca)) {
								estagio = 0;
								break;
							}
							
							/**
							 * Verificação relacionadas a última tecla
							 */
							ultimaTecla = ultimaTecla(ip, porta, endereco, ca);
								if (ultimaTecla.equals("S1")) {
								LOG.info("LENDO SAIDA SOLICITADA [estagio=" + estagio + ",gk=S1]");
								estagio = 0;
								break;
							}
							
							if ((tipo != 1) && (ultimaTecla.equals("F1"))) {
								tipo = 1;
								if (!Pergunta(ip, porta, endereco, ca, getDataHora(c) + " COMPLETAR SENHA?")) {
									estagio = 0;
								}
								LOG.info("DEFININDO COMPLETAR [estagio=" + estagio + ",gk=F1,tipo=" + tipo + "]");
							}
							
							if ((tipo != 2) && (ultimaTecla.equals("F2"))) {
								tipo = 2;
								if (!Pergunta(ip, porta, endereco, ca, getDataHora(c) + " VISTORIA?")) {
									estagio = 0;
								}
								LOG.info("DEFININDO VISTORIA [estagio=" + estagio + ",gk=F2,tipo=" + tipo + "]");
							}
							/************************************************************************************/
							
							if ((dadosDigitadosPendentes(ip, porta, endereco, ca)) && (ultimaTecla.equals("E1"))) {
								
								String valorDigitado = valorDigitado(ip, porta, endereco, ca);
								
								LOG.info("LENDO DADOS DE SENHA DO OPERADOR [estagio=" + estagio
										+ ",gk=E1,valorDigitado=" + valorDigitado + "]");
								
								if (!valorDigitado.equals("")) {
									
									/**
									 * Verifica se trata-se da senha do carregamento ou do id_ordens
									 */
									if (Integer.valueOf(valorDigitado).intValue() > 9999) {
										senha = Integer.valueOf(valorDigitado).toString();
										LOG.info("FORMATADO DADOS DE SENHA PELO SISTEMA COM VALOR DIGITADO SUPERIOR A 9999 [estagio="
													+ estagio + ",senha=" + senha + ",valorDigitado="
													+ valorDigitado + "]");
									} else {
										if (valorDigitado.length() > 4) {
											senha = valorDigitado.substring(5);
										} else {
											senha = valorDigitado;
										}
									}
									/*****************************************************************************************/
									
									/**
									 * Se a senha for um id_ordens válido
									 * Se o "tipo" = 2 
									 * Então 
									 * 		verifica se haverá inspeção (Bola Preta)
									 * 		estágio recebe 6
									 * <Senão>
									 * 		tipo recebe 4
									 * 		estágio recebe 8 
									 */
									if (ordemCarregamento(Integer.valueOf(senha).intValue(), c)) {
										
										LOG.info("ORDEM DE CARREGAMENTO " + (tipo == 2 ? "COM VISTORIA [tipo=2]" : ""));										
										// tipo == 2 : Atribuído pelos ESTÁGIO 1 ou 2 quando o operador informa F2 >> VISTORIA
										if (tipo == 2) {
											
											/**
											 * UNIBRPOSC-166 [DMD042] - Verificar se todas os compartimentos foram atuzalidados
											 * Esse trecho deverá retornar para a operação de carregamento ou retornar para o 
											 * estado que execute o sorteio da BP porém deverá registrar a ocorrência de compartimento
											 * sem carregar no SACC.
											 */
											if(!isOcorreuVerificacaoCarga) {
												
												LOG.info("Verificando se ha compartimentos sem combustivel");
												if(haCompartimentosIncompletos(Integer.valueOf(senha).intValue(), c)) {
													
													LOG.info("Verificando com o operador se a ordem devera ser finalizada ");
													if(Pergunta(ip, porta, endereco, ca, "FINALIZA SEM CONCLUIR SENHAS?")){
														LOG.info("Operador confirmou finalizacao da ordem com compartimentos vazios");
														registarOcorreciaCompartimentosVazios(Integer.valueOf(senha).intValue(), c);
													} else {
														// Caso o operador não confirme a conclusão com compartimentos vazios 
														estagio = 3; 
														break;
													} // END - Mensagem enviada para o operador
												
												} else {
													LOG.info("Todos compartimentos foram preenchidos");
												} // END if(haCompartimentosIncompletos(Integer.valueOf(senha).intValue(), c))
												
												isOcorreuVerificacaoCarga = true;
												
											} // END if(!isOcorreuVerificacaoCarga)
											/**********************************************************************************/
											
											String msg = "";
											msg = bolaPreta(senha, c);

											Mensagem(ip, porta, endereco, ca, senha, msg);

											estagio = 6;
											LOG.info("[id_ordens_clientes=" + id_ordens_clientes + ",estagio=" + estagio
													+ ",senha=" + (senha != null ? senha : "") + ",tipo=" + tipo
													+ ",id_bicos=" + id_bicos + ",receita="
													+ (receita != null ? receita : "") + ",id_produtos_1="
													+ (id_produtos_1 != null ? id_produtos_1 : "") + ",id_produtos_2="
													+ (id_produtos_2 != null ? id_produtos_2 : "") + ",completado="
													+ completado + ",volume_batch="
													+ (volume_batch != null ? volume_batch : "") + "]");

											break;
										}
										tipo = 4;
										estagio = 8;
										LOG.info("ORDEM DE CARREGAMENTO QUE ASSUME TIPO VISTORIA E VAI PARA ESTAGIO 8");

										break;
									}
									/**************************************************************************************/
									
									LOG.info("DADOS DE SENHA INDO PARA VALIDACAO PELO SISTEMA [estagio="+
												estagio+",senha="+senha+",id_bicos="+id_bicos+"]");
									
									id_ordens_clientes = senha.length() > 0 ? validarSenha(senha, c, id_bicos) : 0;
									
									/**
									 * Se há "senha" e o "tipo" <= 2 e id_ordens_clientes > 0
									 */
									if ((senha.length() > 0) && (tipo <= 2) && (id_ordens_clientes > 0)) {

										ArrayList<String> dados = dadosSenha(senha, c);
										idProdutoCompart = ((String) dados.get(4)).toString();
										
										LOG.info("DADOS DE SENHA VALIDADA PELO SISTEMA [estagio="+
													estagio+",senha="+senha+",id_ordens_clientes="+id_ordens_clientes+
													",id_produtos="+idProdutoCompart+",id_bicos="+id_bicos+",tipo="+tipo+"]");
										
										double percent = 0.0D;
										
										/**
										 * Calcula valor da variável "percent"
										 * percent = 100.0D * ordens_clientes.qtd_carregada / ordens_clientes.quantidade   
										 * Onde:
										 * 		dados.get(8) => ordens_clientes.quantidade
										 * 		dados.get(10) => ordens_clientes.qtd_carregada
										 * tipo == 0 : Estado inicial da variável
										 */
										if (tipo == 0) {
											
											percent = 100.0D * (Double.parseDouble(((String) dados.get(10)).toString())
													/ Double.parseDouble(((String) dados.get(8)).toString()));
											
											LOG.info("TIPO CARREGAMENTO COM PERCENT AFERIDO [id_ordens_clientes="
													+ id_ordens_clientes + ",percent=" + percent + "] 100 * " +
													Double.parseDouble(((String) dados.get(10)).toString()) + "/"
													+ Double.parseDouble(((String) dados.get(8)).toString()));
										}
										/**********************************************************************************/
										
										/**
										 * Há volume a ser carregado
										 */
										if (percent < 99.0D) {
											
											// dados.get(8) => ordens_clientes.quantidade
											volume = Integer.parseInt(((String) dados.get(8)).toString());
											
											// dados.get(11) => ordens_clientes.qtd_completada" 
											completado = Integer.parseInt(((String) dados.get(11)).toString());
											
											/**
											 * Se p "tipo" for igual ZERO
											 * o "volume" é subtraído da ordens_clientes.qtd_carregada 
											 * tipo == 0 : Estado inicial da variável
											 */
											if (tipo == 0) {
												
												// dados.get(10) => ordens_clientes.qtd_carregada
												volume -= Integer.parseInt(((String) dados.get(10)).toString());
												
												LOG.info("[DB] DADOS PREPARATIVOS TIPO CARREGAMENTO [id_ordens_clientes="
														+ id_ordens_clientes + ",senha=" + senha + ",volume=" + volume
														+ ",quantidade=" + volume + ",qtd_carregada="
														+ ((String) dados.get(10)).toString() + "]");
												
											} else if (tipo == 1) {
												 //	tipo == 1 : Atribuído pelos ESTÁGIO 1 ou 2 quando o operador informa F1 >> COMPLETAR												
												LOG.info("[DB] DADOS PREPARATIVOS TIPO COMPLEMENTO [id_ordens_clientes=" + id_ordens_clientes
														+ ",senha=" + senha + ",volume=" + volume + ",percent="
														+ percent + ",qtd_completada=" + completado + "]");
											} else {
												LOG.info("[DB] DADOS PREPARATIVOS TIPO VISTORIA [id_ordens_clientes=" + id_ordens_clientes
														+ ",senha=" + senha + "]");
											}
											/*************************************************************************************/
											
											/**
											 * Recuperação da receita
											 */
											if ((IP_192_168_10_22.equals(ip)) || (IP_192_168_10_26.equals(ip))) {
												
												String[] paramsBico = contextualizarBico(id_bicos, id_produtos_1,
														id_produtos_2, ip, idProdutoCompart);

												id_bicos = Integer.parseInt(paramsBico[0]);
												id_produtos_1 = paramsBico[1];
												id_produtos_2 = paramsBico[2];
												receita = receitaBico(id_bicos, ip, idProdutoCompart, c);
												
											} else if(tipoBicoBS) {
												receita = receitaBicoBS(id_bicos, ip, idProdutoCompart, c);
											} else {
												receita = receitaBico(ip, idProdutoCompart, c);
											}
											/*************************************************************************************/
											
											/**
											 * Receita não foi localizada
											 */
											if (receita.equals("0")) {
												
												LOG.info("PRODUTO INVALIDO [ip="+ip+
															",id_bicos="+id_bicos+",id_produtos="+idProdutoCompart+
															",receita="+receita+",tipoBicoBS="+tipoBicoBS+"]");
												Erro(ip, porta, endereco, ca, "PRODUTO INVALIDO");
												estagio = 2;
												break;
												
											}
											/*************************************************************************************/
											
											/**
											 * Validação do volume informado 
											 */
											if (volume <= 0) {
												
												LOG.info("VOLUME INVALIDO [ip="+ip+",id_bicos="+id_bicos+
														",id_produtos="+idProdutoCompart+
														",receita="+receita+",tipoBicoBS="+tipoBicoBS+"]");
												Erro(ip, porta, endereco, ca, "VOLUME INVALIDO");
												estagio = 2;
												break;
												
											}
											/*************************************************************************************/
											
											LOG.info("DADOS PREPARATIVOS COM REC. VOL. VALIDADOS [id_ordens_clientes=" + id_ordens_clientes
													+ ",estagio=" + estagio + ",senha=" + senha + ",tipo=" + tipo
													+ ",id_bicos=" + id_bicos + ",ip="+ip+",receita="
													+ (receita != null ? receita : "") + ",tipoBicoBS="+tipoBicoBS+",id_produtos="+idProdutoCompart+",id_produtos_1="
													+ (id_produtos_1 != null ? id_produtos_1 : "") + ",id_produtos_2="
													+ (id_produtos_2 != null ? id_produtos_2 : "") + ",qtd_completada="
													+ completado + ",volume=" + volume + "]");
											
											/**
											 *	tipo == 0 : Estado inicial da variável
											 */
											if (tipo == 0) {
												
												/**
												 * Mensagem contendo:
												 * id_cliente + / + apelido do produto + "C" + placa  
												 */
												String m1 = ((String) dados.get(3)).toString().substring(0,
														Math.min(14, ((String) dados.get(3)).toString().length()))
														+ " / " + ((String) dados.get(6)).toString() + " C"
														+ ((String) dados.get(7)).toString();

												/**
												 * Mensagem contendo:
												 * número do compartimento + / + id_produto
												 */
												String m2 = FormatarNumero.formatar(
														Integer.parseInt(((String) dados.get(8)).toString()), 0) + " / "
														+ ((String) dados.get(5)).toString();
												
												LOG.info("TIPO CARREGAMENTO - ENVIANDO MENSAGEM AO OPERADOR "
														+ (m1 != null ? m1 : "") + " " + (m2 != null ? m2 : ""));
												
												/**
												 * Caso o Operador não confirme as mensagens
												 * estágio recebe ZERO
												 */
												if (!Mensagem(ip, porta, endereco, ca,
														((String) dados.get(3)).toString().substring(0,
																Math.min(14,
																		((String) dados.get(3)).toString().length()))
																+ " / " + ((String) dados.get(6)).toString() + " C"
																+ ((String) dados.get(7)).toString(),

														FormatarNumero.formatar(
																Integer.parseInt(((String) dados.get(8)).toString()), 0)
																+ " / " + ((String) dados.get(5)).toString())) {
													estagio = 0;
													break;
												}



												LOG.info("TIPO CARREGAMENTO - INDO PARA ESTAGIO 3 [id_ordens_clientes="
														+ id_ordens_clientes + ",id_produtos="+idProdutoCompart+",senha=" + senha +
														",volume=" + volume + "]");

												estagio = 3;
												break;
												
											} // END if (tipo == 0)
											/*************************************************************************************/
											
											/**
											 *	tipo == 1 : Atribuído pelos ESTÁGIO 1 ou 2 quando o operador informa F1 >> COMPLETAR
											 */
											if (tipo == 1) {
												
												// 
												LOG.info("TIPO COMPLEMENTO - INDO PARA ESTAGIO 5 [id_ordens_clientes="
														+ id_ordens_clientes + ",id_produtos="+idProdutoCompart+",senha=" + senha + "]");
												estagio = 5;
												break;
												
											} // END if (tipo == 1)
											/*************************************************************************************/
											
										} else {
											
											// Variável percent >= 99% 
											LOG.info("COMPARTIMENTO JA FOI CARREGADO [id_ordens_clientes="+id_ordens_clientes+
														",senha="+senha+",percent="+percent+"]");
											Erro(ip, porta, endereco, ca, "COMPARTIMENTO JA CARREGADO");
											
										} // END if (percent < 99.0D)
										
									} else {
										
										// não há "senha" ou "tipo" > 2 ou id_ordens <= 0
										LOG.info("SENHA INVALIDA - [senha=" + senha + ",tipo=" + tipo + ",estagio="
												+ estagio + ",id_ordens_clientes=" + id_ordens_clientes + "]");
										Erro(ip, porta, endereco, ca, "SENHA INVALIDA");
										LOG.info("Senha invalida " + senha);
										estagio = 2;
										break;
										
									} // END if ((senha.length() > 0) && (tipo <= 2) && (id_ordens_clientes > 0))
									
								} else {
									
									// Se o valorDigitado for branco 
									estagio = 0;
									break;
									
								} // END if (!valorDigitado.equals(""))
								
							} // END if ((dadosDigitadosPendentes(ip, porta, endereco, ca)) && (ultimaTecla.equals("E1"))) 
							
						} // for (;;)
						
					}
					// END estagio == 2
					
					/**
					 * Estágio 3 - Dispara o comando para carregamento e atualiza a tabela ordens_clientes.
					 * Utilizado também para completar o tanque. 
					 * Limpas as variáveis volume_batch, volume_batch_1 e volume_batch_2.
					 * Verifica a variável tipoBicoBS e o retorno do métodos métodos CarregarBS ou Carregar
					 * 		Atualiza o bico e o operador da orden do cliente 
					 * 		Retorna para o estágio 4 
					 * Senão 
					 * 		Emite mensagem de alerta
					 * 		Retorna para o estágio zero
					 */
					if (estagio == 3) {
						
						volume_batch = "";
						volume_batch_1 = "";
						volume_batch_2 = "";
						String horaInicio = getHoraMinutoSegundoAtual();

						LOG.info("PREPARAR PARA CARREGAR [estagio=3,id_ordens_clientes=" + id_ordens_clientes
								+ ",senha=" + senha + ",id_produtos="+idProdutoCompart+",tipo=" + tipo + ",id_bicos=" + id_bicos + ",receita=" + receita
								+ ",volume=" + volume + ",tipoBicoBS="+tipoBicoBS+"]");

						System.out.println("Ordem cliente = "+id_ordens_clientes);
						if(gravaOrdemMemo(id_ordens_clientes,ip,porta,endereco,ca,c,"Carregamento",id_bicos)){
							System.out.println("Gravou o numero da ordem no carregamento");
						}

						if (tipoBicoBS && CarregarBS(id_bicos, ip, porta, endereco, ca, receita, volume, c, idProdutoCompart)) {

							String sql = new String("update sacc.ordens_clientes set id_bicos = "
									+ Integer.toString(id_bicos) + " , id_operadores = " + Integer.toString(operador)
									+ ", data_inicio =  '"+java.time.LocalDate.now()+"' "
									+" , hora_inicio = '"+horaInicio+"' "
									+ " where id_ordens_clientes = " + Integer.toString(id_ordens_clientes)+" " +
									"  and data_inicio is null ") ;
							LOG.info("[DB] " + sql);
							c.getConexao().executaSql(sql, new String[0]);

							sql = new String("update sacc.ordens_clientes set id_bicos = "
									+ Integer.toString(id_bicos) + " , id_operadores = " + Integer.toString(operador)
									+ " where id_ordens_clientes = " + Integer.toString(id_ordens_clientes) +" "
									+ "  and data_inicio is not null ");

							LOG.info("[DB - UPDATE DT] " + sql);
							c.getConexao().executaSql(sql, new String[0]);
							estagio = 4;
							
						} else if (!tipoBicoBS && Carregar(id_bicos, ip, porta, endereco, ca, receita, volume, c)) {

							String sql = new String("update sacc.ordens_clientes set id_bicos = "
									+ Integer.toString(id_bicos) + " , id_operadores = " + Integer.toString(operador)
									+ ", data_inicio = '"+java.time.LocalDate.now()+"' "
									+" , hora_inicio = '"+horaInicio+"' "
									+ " where id_ordens_clientes = " + Integer.toString(id_ordens_clientes)
									+ "  and data_inicio is null ") ;
							LOG.info("[DB] " + sql);

							c.getConexao().executaSql(sql, new String[0]);
							sql = new String("update sacc.ordens_clientes set id_bicos = "
									+ Integer.toString(id_bicos) + " , id_operadores = " + Integer.toString(operador)
									+ " where id_ordens_clientes = " + Integer.toString(id_ordens_clientes)) +" "
									+ "  and data_inicio is not null ";

							LOG.info("[DB - UPDATE DT] " + sql);
							c.getConexao().executaSql(sql, new String[0]);
							estagio = 4;
							
						} else {
							
							LOG.info("ERRO AO TENTAR CARREGAR");
							Erro(ip, porta, endereco, ca, "ERRO AO TENTAR CARREGAR");
							estagio = 0;
							
						}
					}
					// END estagio == 3
					
					/**
					 * Estágio 4 - Leitura dos volumes que serão carragados e atualiza a ordens_clientes 
					 * Se a tecla informada no equipamento for "P1"
					 * 		Então 
					 * 			Retorna para o estágio zero
					 * 			Executa atualizarOrdensClientes
					 */
					if (estagio == 4) {
						
						int volumeTotal = 0;
						
						/**
						 * UNIBRPOSC-248 - Recuperação da temperatura dos bicos de carregamento no SACC
						 */
						if(ultimaAvaliacao == null) {
							LOG.info("Sem registro da ultima avaliacao da temperatura");
							registrarTemperatura = true;
						} else {
							ZonedDateTime horaAtual = ZonedDateTime.now(ZoneId.of("America/Fortaleza"));
							LOG.info(String.format("Hora atual - %s", horaAtual));
							LOG.info(String.format("Ultima avaliacao - %s", ultimaAvaliacao));
							long diff = ChronoUnit.SECONDS.between(ultimaAvaliacao,horaAtual);
							LOG.info(String.format("Diferenca calculada - %s", diff));
							registrarTemperatura = (diff >= intervaloEntreRegistroTemperatura); 
							
						}
						if(registrarTemperatura) {
							ultimaAvaliacao = ZonedDateTime.now(ZoneId.of("America/Fortaleza"));
							double[] temperaturas = 
									consultarTemperaturaCorrenteTransdutor(ip, porta, endereco, new Comando());
							LOG.info(String.format(
										"Registrando a temperatura para a ordem do cliente %d - temperatura 1 %f - temperatura 2 %f", 
										id_ordens_clientes,
										temperaturas[0],
										temperaturas[1]
									)
							);
							//TODO incluir rotina para a gravação da temperatura
							registarTemperaturaOrdem(id_ordens_clientes,temperaturas,c);
						}
						/*********************************************************************************************/
						
						/**
						 * Verifica se trata-se de produto único 
						 * para o cálculo do volume total
						 */
						if (produtounico) {
							
							Thread.sleep(tempoEspera * 3);
							String resposta = ca.EnviaComando(ip, porta, endereco, "RB");
							if (resposta.startsWith("*01RB")) {
								volume_batch_1 = resposta.substring(21).trim();
							} else {
								LOG.info("[ERRO ACCULOAD] " + resposta + " [volume_batch_1="
										+ (volume_batch_1 != null ? volume_batch_1 : "") + "]");
							}
							volume_batch_2 = "0";
							volumeTotal = Integer.valueOf(volume_batch_1.length() <= 0 ? "0" : volume_batch_1).intValue();
							LOG.info("CARREGANDO PRODUTO UNICO [id_ordens_clientes=" + id_ordens_clientes + ",senha="
									+ senha + ",tipo=" + tipo + ",volume_batch_1=" + volume_batch_1
									+ ",volume_batch_2=0,voltot_volume_batch=" + volumeTotal + "]");
						} else {
							
							Thread.sleep(tempoEspera * 3);
							String resposta = ca.EnviaComando(ip, porta, endereco, "RB P1");
							if (resposta.startsWith("*01RB")) {
								volume_batch_1 = resposta.substring(21).trim();
							}
							Thread.sleep(tempoEspera * 3);
							resposta = ca.EnviaComando(ip, porta, endereco, "RB P2");
							if (resposta.startsWith("*01RB")) {
								volume_batch_2 = resposta.substring(21).trim();
							}
							volumeTotal = Integer.valueOf(volume_batch_1.length() <= 0 ? "0" : volume_batch_1).intValue()
									+ Integer.valueOf(volume_batch_2.length() <= 0 ? "0" : volume_batch_2).intValue();
							LOG.info("CARREGANDO PRODUTO [id_ordens_clientes=" + id_ordens_clientes + ",senha=" + senha
									+ ",tipo=" + tipo + ",volume_batch_1=" + volume_batch_1 + ",volume_batch_2="
									+ volume_batch_2 + ",voltot_volume_batch=" + volumeTotal + "]");
							
						} // END if (produtounico)
						
						volume_batch = Integer.toString(volumeTotal);
						
						/**
						 * Verifica a última tecla e atualiza os dados da ordem do cliente
						 */
						if (ultimaTecla(ip, porta, endereco, ca).equals("P1")) {
							
							estagio = 0;
							
							if ((id_ordens_clientes > 0) && (volume_batch != null) && (volume_batch.length() > 0)) {
								
								if ((volume_batch_1 == null) || (volume_batch_1.length() < 1)) {
									volume_batch_1 = "0";
								}
								if ((volume_batch_1 == null) || (volume_batch_2.length() < 1)) {
									volume_batch_2 = "0";
								}
								
								// tipo == 0 : Estado inicial da variável
								String tipoProcedimento = tipo == 0 ? "CARREGAMENTO" : "COMPLEMENTO";

								LOG.info("SOLICITADO PARADA DE " + tipoProcedimento + " [id_ordens_clientes="
										+ id_ordens_clientes + ",senha=" + senha + ",volume_batch=" + volume_batch
										+ ",volume_batch_1=" + volume_batch_1 + ",volume_batch_2=" + volume_batch_2
										+ ",estagio=4,gk=P1]");
								
								/**
								 * Se a senha tiver quatro caracteres (é uma senha da tabela ordens_clientes)
								 * Recupera as temperatura, calcula a densidade e atualiza e ordem do cliente
								 */
								if (senha.length() <= 4) {
									
									String hora = getHoraMinutoSegundoAtual();

									c.getConexao().desconecta();

									double[] temperaturas = consultarTemperaturaCorrenteTransdutor(ip, porta, endereco,
											new Comando());

									c = new ConexaoUnisystem();

									double[] temperatura_densidade_tanque_produto_1 = consultarTemperaturaDensidadeTanqueAtivoCarregamentoPorProduto(
											id_produtos_1, c.getConexao());
									double[] temperatura_densidade_tanque_produto_2 = consultarTemperaturaDensidadeTanqueAtivoCarregamentoPorProduto(
											id_produtos_2, c.getConexao());

									atualizarOrdensClientes(0 == tipo, volume_batch, volume_batch_1, volume_batch_2,
											id_produtos_1, id_produtos_2, temperaturas,
											temperatura_densidade_tanque_produto_1,
											temperatura_densidade_tanque_produto_2, id_bicos, operador,
											id_ordens_clientes, c.getConexao(), hora, true);

									ultimaAvaliacao = null;

									LOG.info("TERMINO DE " + tipoProcedimento + " [id_ordens_clientes="
											+ id_ordens_clientes + ",senha=" + senha + ",gk=P1]");
									
								} // END senha.length() <= 4)
								
							} // END if ((id_ordens_clientes > 0) && (volume_batch != null) && (volume_batch.length() > 0))
							
							LOG.info("************************************************************************************************************************************************");
							
						} // END if (ultimaTecla(ip, porta, endereco, ca).equals("P1"))
						
					}
					// END estagio == 4
					
					/**
					 * Estágio 5 - Completar o carregamento
					 * 
					 * Limpas as variáveis volume_batch, volume_batch_1 e volume_batch_2.
					 * 
					 * Retorna o estágio para zero
					 * 		Se não for para completar o volume.
					 * 		Se não for informado nenhum valor no equipamento.
					 * 
					 * Retorna para o estágio 2
					 * 		Se o valor informado for MAIOR que o percentual a ser completado
					 * 		Se o valor informado for abaixo de ZERO
					 * 
					 * Retorna para o estágio 3
					 * 		Se o valor informado for um volume válido
					 * 
					 * Se a variável "receita" for "1" e o produto NÃO for único
					 * 		Então atribui na "receita" o valor do método completarCom  
					 */
					if (estagio == 5) {
						volume_batch = "";
						volume_batch_1 = "";
						volume_batch_2 = "";
						
						/**
						 * Se não for para completrar o volume volta para o estágio zero 
						 */
						if (!Pergunta(ip, porta, endereco, ca, getDataHora(c) + " COMPLETAR VOLUME?")) {
							estagio = 0;
						}						
						LOG.info("TIPO COMPLETAR - OPERADOR AFIRMOU COMPLETAR O VOLUME [id_ordens_clientes="
								+ id_ordens_clientes + ",estagio=5]");
						
						/**
						 * Aguardando a leitura do equipamento
						 */						
						do {
							Thread.sleep(tempoEspera);
							if (mensagemTimeOut(ip, porta, endereco, ca)) {
								estagio = 0;
								break;
							}
							ultimaTecla = ultimaTecla(ip, porta, endereco, ca);
							if (ultimaTecla.equals("S1")) {
								estagio = 0;
								break;
							}
						} while ((!dadosDigitadosPendentes(ip, porta, endereco, ca)) || (!ultimaTecla.equals("E1")));
						/*********************************************************************************************/
						
						/**
						 * Se o valor digitado for branco 
						 * 	Então o estágio retorna para zero
						 * 	Senão
						 * 		Recupera o valor informado e divide pelo volume
						 * 		Se o cálculo for maior que o percentual para completar
						 * 		Então 
						 * 			Emite mensagem de alerta
						 * 			Retorna o estágio para 2
						 *		Senão
						 *			Se não é um produto único e a receita é igual a "1"
						 *			Então executa o completarCom()
						 *			Se o valor informao é menor que zero
						 *			Então
						 *				Emite mensagem
						 *				Retorna o estágio para 2
						 *			Senão
						 *				Retorna para o estágio 3
						 */
						String valorDigitado = valorDigitado(ip, porta, endereco, ca);
						if (!valorDigitado.equals("")) {
							
							LOG.info("TIPO COMPLETAR - PREVIA CALCULO VOLUME [id_ordens_clientes=" + id_ordens_clientes
									+ ",senha=" + senha 
									+ ",calc=" + (completado + Double.parseDouble(valorDigitado))
									+ ",completado=" + completado 
									+ ",valorDigitado=" + valorDigitado 
									+ ",volume=" + volume + "]");

							double calc = completado + Double.parseDouble(valorDigitado);
							calc /= volume;

							LOG.info("TIPO COMPLETAR - RESULTADO CALCULO VOLUME - [id_ordens_clientes="
									+ id_ordens_clientes + ",senha=" + senha + ",percentual_completar="
									+ percentual_completar + ",calc=" + calc + ",dbl_calc=" +
									Double.toString(calc) + "]");

							if (calc > percentual_completar) {
								Erro(ip, porta, endereco, ca, "VOLUME NAO PERMITIDO");
								estagio = 2;
							} else {
								volume = Integer.parseInt(valorDigitado);
								if (!produtoUnico(ip, c)) {
									if (receita.equals("1")) {
										receita = completarCom(ip, c);
									}
								}
								if (volume <= 0) {
									Erro(ip, porta, endereco, ca, "VOLUME INVALIDO");
									estagio = 2;
								} else {

									//gravando o numero da ordem para capturar a transacao no momento do completa
									if(gravaOrdemMemo(id_ordens_clientes,ip,porta,endereco,ca,c,"completa",id_bicos)){
										System.out.println("Gravou o numero da ordem no completa");
									}

									LOG.info("TIPO COMPLETAR COM VOLUMES VALIDOS E PERMITIDOS - [id_ordens_clientes="
											+ id_ordens_clientes + ",senha=" + senha + ",tipo=" + tipo + ",id_bicos="
											+ id_bicos + ",receita=" + receita + ",volume=" + volume + "]");
									estagio = 3;
								}
							}
						} else {
							estagio = 0;
						}
					}
					// END estagio == 5
					
					/**
					 * Estágio 6 - Verifica a tela "P1" para retornar para o estágio zero
					 * Limpas as variáveis volume_batch, volume_batch_1 e volume_batch_2.
					 * Verifica se a última tecla foi a P1, caso afirmativo retorna para o estagio = 0 
					 */
					if (estagio == 6) {
						volume_batch = "";
						volume_batch_1 = "";
						volume_batch_2 = "";
						if (ultimaTecla(ip, porta, endereco, ca).equals("P1")) {
							estagio = 0;
						}
					}
					// END estagio == 6
					
					/**
					 * Estágio 7 - Leitura do volume
					 * 
					 * Limpas as variáveis volume_batch, volume_batch_1 e volume_batch_2.
					 * 
					 * Perguta se é uma descarga
					 * Se não for uma descarga 
					 * Então estágio recebe ZERO
					 * 
					 * Lê o valor digitado no equipamento.
					 * 
					 * Se "valorDigitado" foi informado
					 * Então
					 * 		converte para um inteiro "volume"
					 * 		Se "volume" <= ZERO
					 * 		Então 
					 * 			Exibe mensagem "VOLUME INVALIDO"
					 * 			Estágio recebe o valor 2
					 * 		Senão
					 * 			Estágio recebe o valor 3
					 * Senão 
					 * 		Estágio recebe ZERO
					 * 
					 */
					if (estagio == 7) {
						
						volume_batch = "";
						volume_batch_1 = "";
						volume_batch_2 = "";
						
						/**
						 * Se não for uma descarga retorno para o estágio zero
						 */
						if (!Pergunta(ip, porta, endereco, ca, getDataHora(c) + " DESCARGA?")) {
							estagio = 0;
						}
						LOG.info("ORDEM DE DESCARGA -OPERADOR AFIRMOU ORDEM DE DESCARGA");
						
						/**
						 * Aguardando a leitura do equipamento
						 */
						do {
							Thread.sleep(tempoEspera);
							if (mensagemTimeOut(ip, porta, endereco, ca)) {
								estagio = 0;
								break;
							}
							ultimaTecla = ultimaTecla(ip, porta, endereco, ca);
							if (ultimaTecla.equals("S1")) {
								estagio = 0;
								break;
							}
						} while ((!dadosDigitadosPendentes(ip, porta, endereco, ca)) || (!ultimaTecla.equals("E1")));
						/*********************************************************************************************/
						
						/**
						 * Recupera o valor digitado no equipamento.
						 * Se for vazio 
						 * 		então volta o estágio para zero
						 * Se o valor digitado for abaixo de zero
						 * 		então emite mensagem de erro e retorna para o estágio 2
						 * 		senão retorna para o estágio 3
						 */
						String valorDigitado = valorDigitado(ip, porta, endereco, ca);
						if (!valorDigitado.equals("")) {
							volume = Integer.parseInt(valorDigitado);
							LOG.info("ORDEM DE DESCARGA - VOLUME INFORMADO PELO OPERADOR [id_ordens_clientes="
									+ id_ordens_clientes + ",senha=" + (senha != null ? senha : "") + ",volume="
									+ volume + ",tipo=" + tipo + "]");
							if (volume <= 0) {
								Erro(ip, porta, endereco, ca, "VOLUME INVALIDO");
								estagio = 2;
							} else {
								estagio = 3;
							}
						} else {
							estagio = 0;
						}
					}
					// END estagio == 7
					
					/**
					 * Estágio 8 - Retorna para o estágio ZERO - Em vistoria? 
					 * Limpas as variáveis volume_batch, volume_batch_1 e volume_batch_2.
					 * Retorna para o estágio zero    
					 */
					if (estagio == 8) {
						volume_batch = "";
						volume_batch_1 = "";
						volume_batch_2 = "";
						LOG.info("ORDEM DE CARREGAMENTO - VAI PARA ESTAGIO 0 EM VISTORIA [estagio=8,tipo=4,volume_batch=,volume_batch_1=,volume_batch_2=]");
						estagio = 0;
					}
					// END estagio == 8
					
				}
				// END for(;;)
			} catch (Exception e) {
				volume_batch = "";
				volume_batch_1 = "";
				volume_batch_2 = "";
				estagio = 0;
				LOG.error("BICO: " + ip + " ERRO: " + e.getLocalizedMessage(),e);
				Erro(ip, porta, endereco, ca, e.getLocalizedMessage());
			}
		} catch (Exception e) {
			LOG.error("ERRO GERAL: " + e.getLocalizedMessage(),e);
		}
		
	}// END main()
	/**************************************************************************************************/
	
	/**
	 * UNIBRPOSC-166 [DMD042] - Caso o operador confirme a conclusão da ordem com compartimentos
	 * vazios efetua-se um registro dessa ocorrência no SACC
	 */
	protected static void registarOcorreciaCompartimentosVazios(int idOrdens, ConexaoUnisystem c) {
		try {
			//String sql = 
				//	"insert into sacc.ocorrencia_ordens(id_ordens,descricao,tipo) "+
					//"values (?,'CARREGAMENTO LIBERADO PELO OPERADOR SEM UTILIZAR TODAS AS SENHAS',1)";
			//String[] sParametros = new String[1];
			//sParametros[0] = Integer.toString(idOrdens);
			String sql = "INSERT INTO ocorrencia_ordens(\"ordemId\", descricao, tipo) " +
		             "VALUES (" + idOrdens + ", 'CARREGAMENTO LIBERADO PELO OPERADOR SEM UTILIZAR TODAS AS SENHAS', 1)";
			c.getConexao().executaSql(sql, new String[] {});
		} catch (Exception e) {
			LOG.error("Falha no registro da ocorrencia",e);
		}
	}
	/**************************************************************************************************/

	/**
	 * UNIBRPOSC-166 [DMD042] - Verifica se todos os compartimentos foram carregados
	 */
	protected static boolean haCompartimentosIncompletos(int idOrdens, ConexaoUnisystem c) {
		boolean retorno = false;
		try {
			LOG.info("Iniciou haCompartimentosIncompletos -> idOrdens "+idOrdens);
			/*String[] sParametros = new String[1];
			sParametros[0] = Integer.toString(idOrdens);
			String sql = 
					"select " +
					"  ordens_clientes.id_ordens " +          // 1
					" ,sum(ordens_clientes.quantidade) " +    // 2
					" ,sum(ordens_clientes.qtd_carregada) " + // 3
					"from sacc.ordens_clientes " + 
					"     left outer join sacc.ordens " + 
					"       on ordens.id_ordens = ordens_clientes.id_ordens " +
					"     left outer join sacc.veiculos_compartimentos " + 
					"       on veiculos_compartimentos.id_veiculos_compartimentos = " +
					"          ordens_clientes.id_veiculos_compartimentos "+ 
					"where ordens_clientes.id_ordens = ? " +
					"      and ordens_clientes.ativo = 'S' " + 
					"      and ordens.situacao = 'O' " + 
					"      and ordens.ativo = 'S' " + 
					"      and veiculos_compartimentos.ativo = 'S' " +
					"group by ordens_clientes.id_ordens"; 
			LOG.info("haCompartimentosIncompletos -> sql "+sql);*/
			String sql = 
				    "SELECT " +
				    "    ordens_clientes.\"ordemId\", " +
				    "    SUM(ordens_clientes.quantidade), " +
				    "    SUM(ordens_clientes.qtd_carregada) " +
				    "FROM " +
				    "    ordens_clientes " +
				    "LEFT OUTER JOIN ordens ON " +
				    "    ordens.id = ordens_clientes.\"ordemId\" " +
				    "LEFT OUTER JOIN veiculos_compartimentos ON " +
				    "    veiculos_compartimentos.id = ordens_clientes.\"veiculoCompartimentoId\" " +
				    "WHERE " +
				    "    ordens_clientes.\"ordemId\" = " + idOrdens + " " +
				    "    AND ordens_clientes.ativo = true " +
				    "    AND ordens.situacao = 'O' " +
				    "    AND ordens.ativo = true " +
				    "    AND veiculos_compartimentos.ativo = true " +
				    "GROUP BY " +
				    "    ordens_clientes.\"ordemId\"";
			LOG.info("haCompartimentosIncompletos -> sql " + sql);
			//ResultSet rsDados = c.getConexao().getRegistros(sql,sParametros);
			ResultSet rsDados = c.getConexao().getRegistros(sql, new String[] {});
			if(rsDados.next()) {
				double percent = 100.0D * rsDados.getDouble(3) / rsDados.getDouble(2);
				LOG.info(String.format("Execucao do haCompartimentosIncompletos - percentual calculado = %f", percent));
				retorno = (percent < 99.0D);
			}
			rsDados.close();
		} catch (Exception e) {
			LOG.error("Falha na execucao haCompartimentosIncompletos ",e);
		}
		return retorno;
	}
	/**************************************************************************************************/
	
	/**
	 * UNIBRPOSC-248 - Recuperação da temperatura dos bicos de carregamento no SACC
	 */
	protected static void registarTemperaturaOrdem(int idOrdensCliente, 
			                                       double[] temperaturas,
			                                       ConexaoUnisystem c) {
		try {
			/*String sql = 
				"insert into sacc.ordens_carregamento_tempos "
				+ "  (id_ordens_clientes "
				+ "   ,id_produtos_1 "
				+ "   ,temperatura_produto_1 "
				+ "   ,id_produtos_2 "
				+ "   ,temperatura_produto_2 "
				+ "   ,id_veiculos_compartimentos) " 
				+ "select " 
				+ "  ordens_clientes.id_ordens_clientes "
				+ "  ,ordens_clientes.id_produtos_1 "
				+ "  ,?  "
				+ "  ,ordens_clientes.id_produtos_2 "
				+ "  ,? "
				+ "  ,ordens_clientes.id_veiculos_compartimentos "
				+ "from sacc.ordens_clientes "
				+ "where ordens_clientes.id_ordens_clientes = ?";
			String[] sParametros = new String[3];
			sParametros[0] = Double.toString(temperaturas[0]);
			sParametros[1] = Double.toString(temperaturas[1]);
			sParametros[2] = Integer.toString(idOrdensCliente);*/
			String sql = "INSERT INTO ordens_carregamento_tempos "
		            + "  (\"ordemClienteId\", \"produto1Id\", temperatura_produto_1, \"produto2Id\", temperatura_produto_2, \"veiculoCompartimentoId\") "
		            + "SELECT "
		            + "  ordens_clientes.id, "
		            + "  ordens_clientes.\"produto1Id\", "
		            + "  " + temperaturas[0] + ", "
		            + "  ordens_clientes.\"produto2Id\", "
		            + "  " + temperaturas[1] + ", "
		            + "  ordens_clientes.\"veiculoCompartimentoId\" "
		            + "FROM ordens_clientes "
		            + "WHERE ordens_clientes.id = " + idOrdensCliente;
			LOG.info(
					String.format(
							"EXEC >> registarTemperaturaOrdem para idOrdensCliente = %d e temperaturas = %f e %f ",
							idOrdensCliente,
							temperaturas[0],
							temperaturas[1]
						)
					);
			c.getConexao().executaSql(sql, new String[0]);
		} catch (Exception e) {
			LOG.error("Falha no registro da temperatura da ordem",e);
		}
	}
	/**************************************************************************************************/

	public static String getHoraMinutoSegundoAtual() {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
		return formatter.format(ZonedDateTime.now(ZoneId.of("America/Fortaleza")));
	}
	/**************************************************************************************************/

	private static double formatarTemperaturaCorrenteTransdutor(String temperatura) {
		BigDecimal bd = new BigDecimal(temperatura.replace('+', ' ').replace(',', '.').trim());
		bd = bd.setScale(1, 1);
		return bd.doubleValue();
	}
	/**************************************************************************************************/

	private static double formatarDouble(String densidade, int casasDecimais) {
		BigDecimal bd = new BigDecimal(densidade);
		bd = bd.setScale(casasDecimais, 1);
		return bd.doubleValue();
	}
	/**************************************************************************************************/

	public static double[] consultarTemperaturaCorrenteTransdutor(String ip, int porta, String endereco, Comando ca) {
		double tmpP1 = 0.0D;
		double tmpP2 = 0.0D;
		String resposta = "";
		try {
			resposta = ca.EnviaComando(ip, porta, endereco, "RD T");
		} catch (Exception ex) {
			LOG.info(
					"[EX] consultarTemperaturaCorrenteTransdutor: nao foi possivel recuperar os valores de temperatura do transdutor.");
		}
		try {
			tmpP1 = formatarTemperaturaCorrenteTransdutor(resposta.substring(8, 15));
		} catch (Exception ex) {
			LOG.debug(
					"[EX] consultarTemperaturaCorrenteTransdutor: mantendo valor zero na temperatura do produto 1 [temperatura_produto_1="
							+ tmpP1 + "]");
		}
		try {
			tmpP2 = formatarTemperaturaCorrenteTransdutor(resposta.substring(16, 23));
		} catch (Exception ex) {
			LOG.debug(
					"[EX] consultarTemperaturaCorrenteTransdutor: mantendo valor zero na temperatura do produto 2 [temperatura_produto_2="
							+ tmpP2 + "]");
		}
		LOG.debug("[AC] termino consultarTemperaturaCorrenteTransdutor [temperatura_produto_1=" + tmpP1
				+ ",temperatura_produto_2 = " + tmpP2 + "]");
		return new double[] { tmpP1, tmpP2 };
	}
	/**************************************************************************************************/

	public static double[] consultarTemperaturaDensidadeTanqueAtivoCarregamentoPorProduto(String id_produto,
			Conexao conexao) {
		double[] temperaturaDensidade = { 0.0D, 0.0D };
		try {
			LOG.debug(
					"[DB] consultarDensidadeTanqueAtivoCarregamentoPorProduto SELECT temperatura,densidade FROM sacc.tanques WHERE id_tanques =  (SELECT ptq.id_tanques FROM ( SELECT id_produtos, MAX(CONCAT(data_parametros,' ',hora_parametros)) AS datahora,id_tipos_de_ordem FROM sacc.parametros_tanque WHERE data_parametros BETWEEN DATE_ADD(CURDATE(), INTERVAL -3 DAY) AND CURDATE() AND id_produtos = ? AND id_tipos_de_ordem = 3 GROUP BY id_produtos,id_tipos_de_ordem) PreQuery , sacc.parametros_tanque ptq  WHERE PreQuery.datahora = CONCAT(ptq.data_parametros,' ',ptq.hora_parametros)  AND ptq.id_produtos = PreQuery.id_produtos  AND ptq.id_tipos_de_ordem = PreQuery.id_tipos_de_ordem)");
			String[] valor = conexao.valorChaveValorUnicoRegistro(
				    "SELECT temperatura, densidade FROM tanques WHERE id = (" +
				    "SELECT ptq.\"tanqueId\" FROM (" +
				    "    SELECT \"produtoId\", MAX(data_parametro || ' ' || hora_parametro) as datahora, \"tipoOrdemId\" " +
				    "    FROM parametros_tanque " +
				    "    WHERE data_parametro BETWEEN CURRENT_DATE - interval '3 days' AND CURRENT_DATE " +
				    "        AND \"produtoId\" = ? AND \"tipoOrdemId\" = 3 " +
				    "    GROUP BY \"produtoId\", \"tipoOrdemId\" " +
				    ") as PreQuery, parametros_tanque ptq " +
				    "WHERE PreQuery.datahora = ptq.data_parametro || ' ' || ptq.hora_parametro " +
				    "    AND ptq.\"produtoId\" = PreQuery.\"produtoId\" " +
				    "    AND ptq.\"tipoOrdemId\" = PreQuery.\"tipoOrdemId\")",
				    new String[] { id_produto });
			temperaturaDensidade[0] = formatarDouble(valor[0], 1);
			temperaturaDensidade[1] = formatarDouble(valor[1], 4);
		} catch (Exception e) {
			LOG.debug(
					"[EX] consultarDensidadeTanqueAtivoCarregamentoPorProduto: mantendo valor zero na densidade [id_produto="
							+ id_produto + ",temperatura=" + temperaturaDensidade[0] + ",densidade = "
							+ temperaturaDensidade[1] + "]");
			return temperaturaDensidade;
		}
		LOG.info("[DB] termino consultarDensidadeTanqueAtivoCarregamentoPorProduto [id_produto=" + id_produto
				+ ",temperatura=" + temperaturaDensidade[0] + ",densidade = " + temperaturaDensidade[1]);
		return temperaturaDensidade;
	}
	/**************************************************************************************************/

	public static void atualizarOrdensClientes(boolean carregamento, String volume_batch, String volume_batch_1,
			String volume_batch_2, String id_produtos_1, String id_produtos_2, double[] temperaturas,
			double[] produto_densidade_tanque_produto_1, double[] produto_densidade_tanque_produto_2, int id_bicos,
			int operador, int id_ordens_clientes, Conexao conexao, String hora, boolean modoCarregamentoSomativo)
			throws Exception {
		LOG.info("[DB] atualizarOrdensClientes [id_ordens_clientes=" + id_ordens_clientes + ",carregamento="
				+ carregamento + ",id_bicos=" + id_bicos + ",operador=" + operador + ",hora=" + hora + ",volume_batch="
				+ volume_batch + ",id_produtos_1=" + id_produtos_1 + ",volume_batch_1=" + volume_batch_1
				+ ",temperaturas[0]=" + temperaturas[0] + ",temperatura_tanque_1="
				+ produto_densidade_tanque_produto_1[0] + ",densidade_tanque_1=" + produto_densidade_tanque_produto_1[1]
				+ ",id_produtos_2=" + id_produtos_2 + ",volume_batch_2=" + volume_batch_2 + ",temperaturas[1]="
				+ temperaturas[1] + ",temperatura_tanque_2=" + produto_densidade_tanque_produto_2[0]
				+ ",densidade_tanque_2=" + produto_densidade_tanque_produto_2[1] + "]");

		/*String[] params = { Double.toString(temperaturas[0]), //0
				Double.toString(temperaturas[1]),//1
				Double.toString(produto_densidade_tanque_produto_1[0]),//2
				Double.toString(produto_densidade_tanque_produto_1[1]),//3
				Double.toString(produto_densidade_tanque_produto_2[0]),//4
				Double.toString(produto_densidade_tanque_produto_2[1]), //5
				id_produtos_1, //6
				id_produtos_2,//7
				Integer.toString(id_bicos),//8
				Integer.toString(operador), //9
				hora , //10
				volume_batch, //11
				volume_batch_1,//12
				volume_batch_2, //13
				Integer.toString(id_ordens_clientes) }; //14*/
		if (carregamento) {
			if (modoCarregamentoSomativo) {
				LOG.info(
						"[DB] atualizarOrdensClientes por carregamento somativo UPDATE sacc.ordens_clientes SET temperatura_produtos_1 = ?, temperatura_produtos_2 = ?, temperatura_tanque_produtos_1 = ?, densidade_tanque_produtos_1 = ?, temperatura_tanque_produtos_2 = ?, densidade_tanque_produtos_2 = ?, id_produtos_1 = ?, id_produtos_2 = ?, id_bicos = ?, id_operadores = ?, hora_carregada = ?, qtd_carregada = qtd_carregada + ?, qtd_carregada_1 = qtd_carregada_1 + ?, qtd_carregada_2 = qtd_carregada_2 + ? WHERE id_ordens_clientes = ?");
				/*conexao.executaSql(
						"UPDATE sacc.ordens_clientes SET temperatura_produtos_1 = ?, " +
								"temperatura_produtos_2 = ?, " +
								"temperatura_tanque_produtos_1 = ?, " +
								"densidade_tanque_produtos_1 = ?, " +
								"temperatura_tanque_produtos_2 = ?, " +
								"densidade_tanque_produtos_2 = ?, " +
								"id_produtos_1 = ?, " +
								"id_produtos_2 = ?, " +
								"id_bicos = ?, " +
								"id_operadores = ?, " +
								"hora_carregada = ?, " +
								"qtd_carregada = qtd_carregada + ?, " +
								"qtd_carregada_1 = qtd_carregada_1 + ?, " +
								"qtd_carregada_2 = qtd_carregada_2 + ? " +
								"WHERE id_ordens_clientes = ?",
						params);*/
				
				conexao.executaSql("UPDATE ordens_clientes SET " +
				        "temperatura_produtos_1 = " + temperaturas[0] + ", " +
				        "temperatura_produtos_2 = " + temperaturas[1] + ", " +
				        "temperatura_tanque_produtos_1 = " + produto_densidade_tanque_produto_1[0] + ", " +
				        "densidade_tanque_produtos_1 = " + produto_densidade_tanque_produto_1[1] + ", " +
				        "temperatura_tanque_produtos_2 = " + produto_densidade_tanque_produto_2[0] + ", " +
				        "densidade_tanque_produtos_2 = " + produto_densidade_tanque_produto_2[1] + ", " +
				        "\"produto1Id\" = " + (!id_produtos_1.isEmpty() && Integer.parseInt(id_produtos_1) > 0 ? id_produtos_1 : "\"produto1Id\"") + ", " +
				        "\"produto2Id\" = " + (!id_produtos_2.isEmpty() && Integer.parseInt(id_produtos_2) > 0 ? id_produtos_2 : "\"produto2Id\"") + ", " +
				        "\"bicoId\" = " + (id_bicos > 0 ? id_bicos : "NULL") + ", " +
				        "\"operadorId\" = " + (operador > 0 ? operador : "NULL")  + ", " +
				        "hora_carregada = CAST('" + hora + "' AS time), " +
				        "qtd_carregada = qtd_carregada + " + (volume_batch.isEmpty() ? 0 : volume_batch) + ", " +
				        "qtd_carregada_1 = qtd_carregada_1 + " + (volume_batch_1.isEmpty() ? 0 : volume_batch_1) + ", " +
				        "qtd_carregada_2 = qtd_carregada_2 + " + (volume_batch_2.isEmpty() ? 0 : volume_batch_2) +
				        " WHERE id = " + id_ordens_clientes, new String[] {});
				
			} else {
				LOG.info(
						"[DB] atualizarOrdensClientes por carregamento UPDATE sacc.ordens_clientes SET temperatura_produtos_1 = ?, temperatura_produtos_2 = ?, temperatura_tanque_produtos_1 = ?, densidade_tanque_produtos_1 = ?, temperatura_tanque_produtos_2 = ?, densidade_tanque_produtos_2 = ?, id_produtos_1 = ?, id_produtos_2 = ?, id_bicos = ?, id_operadores = ?, hora_carregada = ?, qtd_carregada = ?, qtd_carregada_1 = ?, qtd_carregada_2 = ? WHERE id_ordens_clientes = ?");
				/*conexao.executaSql(
					    "UPDATE sacc.ordens_clientes SET " +
					    "temperatura_produtos_1 = ?, " +
					    "temperatura_produtos_2 = ?, " +
					    "temperatura_tanque_produtos_1 = ?, " +
					    "densidade_tanque_produtos_1 = ?, " +
					    "temperatura_tanque_produtos_2 = ?, " +
					    "densidade_tanque_produtos_2 = ?, " +
					    "id_produtos_1 = ?, " +
					    "id_produtos_2 = ?, " +
					    "id_bicos = ?, " +
					    "id_operadores = ?, " +
					    "hora_carregada = ?, " +
					    "qtd_carregada = ?, " +
					    "qtd_carregada_1 = ?, " +
					    "qtd_carregada_2 = ? " +
					    "WHERE id_ordens_clientes = ?",
					    params);*/
				conexao.executaSql(
					    "UPDATE ordens_clientes SET " +
					        "temperatura_produtos_1 = " + temperaturas[0] + ", " +
					        "temperatura_produtos_2 = " + temperaturas[1] + ", " +
					        "temperatura_tanque_produtos_1 = " + produto_densidade_tanque_produto_1[0] + ", " +
					        "densidade_tanque_produtos_1 = " + produto_densidade_tanque_produto_1[1] + ", " +
					        "temperatura_tanque_produtos_2 = " + produto_densidade_tanque_produto_2[0] + ", " +
					        "densidade_tanque_produtos_2 = " + produto_densidade_tanque_produto_2[1] + ", " +
					        "\"produto1Id\" = " + (!id_produtos_1.isEmpty() && Integer.parseInt(id_produtos_1) > 0 ? id_produtos_1 : "\"produto1Id\"") + ", " +
					        "\"produto2Id\" = " + (!id_produtos_2.isEmpty() && Integer.parseInt(id_produtos_2) > 0 ? id_produtos_2 : "\"produto2Id\"") + ", " +
					        "\"bicoId\" = " + (id_bicos > 0 ? id_bicos : "NULL") + ", " +
					        "\"operadorId\" = " + (operador > 0 ? operador : "NULL")  + ", " +
					        "hora_carregada = CAST('" + hora + "' AS time), " +
					        "qtd_carregada = " + (volume_batch.isEmpty() ? 0 : volume_batch) + ", " +
					        "qtd_carregada_1 = " + (volume_batch_1.isEmpty() ? 0 : volume_batch_1) + ", " +
					        "qtd_carregada_2 = " + (volume_batch_2.isEmpty() ? 0 : volume_batch_2) + " " +
					    "WHERE id = " + id_ordens_clientes,
					    new String[] {});
			}
		} else {
			LOG.info(
					"[DB] atualizarOrdensClientes por complemento UPDATE sacc.ordens_clientes SET temperatura_produtos_1 = ?, temperatura_produtos_2 = ?, temperatura_tanque_produtos_1 = ?, densidade_tanque_produtos_1 = ?, temperatura_tanque_produtos_2 = ?, densidade_tanque_produtos_2 = ?, id_produtos_1 = ?, id_produtos_2 = ?, id_bicos = ?, id_operadores = ?, hora_completada = ?, qtd_completada = qtd_completada + ?, qtd_completada_1 = qtd_completada_1 + ?, qtd_completada_2 = qtd_completada_2 + ? WHERE id_ordens_clientes = ?");
			/*conexao.executaSql(
				    "UPDATE sacc.ordens_clientes SET " +
				        "temperatura_produtos_1 = ?, " +
				        "temperatura_produtos_2 = ?, " +
				        "temperatura_tanque_produtos_1 = ?, " +
				        "densidade_tanque_produtos_1 = ?, " +
				        "temperatura_tanque_produtos_2 = ?, " +
				        "densidade_tanque_produtos_2 = ?, " +
				        "id_produtos_1 = ?, " +
				        "id_produtos_2 = ?, " +
				        "id_bicos = ?, " +
				        "id_operadores = ?, " +
				        "hora_completada = ?, " +
				        "qtd_completada = qtd_completada + ?, " +
				        "qtd_completada_1 = qtd_completada_1 + ?, " +
				        "qtd_completada_2 = qtd_completada_2 + ? " +
				    "WHERE id_ordens_clientes = ?",
				    params);*/
			conexao.executaSql(
				    "UPDATE ordens_clientes SET " +
				        "temperatura_produtos_1 = " + temperaturas[0] + ", " +
				        "temperatura_produtos_2 = " + temperaturas[1] + ", " +
				        "temperatura_tanque_produtos_1 = " + produto_densidade_tanque_produto_1[0] + ", " +
				        "densidade_tanque_produtos_1 = " + produto_densidade_tanque_produto_1[1] + ", " +
				        "temperatura_tanque_produtos_2 = " + produto_densidade_tanque_produto_2[0] + ", " +
				        "densidade_tanque_produtos_2 = " + produto_densidade_tanque_produto_2[1] + ", " +
				        "\"produto1Id\" = " + (!id_produtos_1.isEmpty() && Integer.parseInt(id_produtos_1) > 0 ? id_produtos_1 : "\"produto1Id\"") + ", " +
				        "\"produto2Id\" = " + (!id_produtos_2.isEmpty() && Integer.parseInt(id_produtos_2) > 0 ? id_produtos_2 : "\"produto2Id\"") + ", " +
				        "\"bicoId\" = " + (id_bicos > 0 ? id_bicos : "NULL") + ", " +
				        "\"operadorId\" = " + (operador > 0 ? operador : "NULL")  + ", " +
				        "hora_completada = CAST('" + hora + "' AS time), " +
				        "qtd_completada = qtd_completada + " + (volume_batch.isEmpty() ? 0 : volume_batch) + ", " +
				        "qtd_completada_1 = qtd_completada_1 + " + (volume_batch_1.isEmpty() ? 0 : volume_batch_1) + ", " +
				        "qtd_completada_2 = qtd_completada_2 + " + (volume_batch_2.isEmpty() ? 0 : volume_batch_2) + " " +
				    "WHERE id = " + id_ordens_clientes,
				    new String[] {});
		}
	}
	/**************************************************************************************************/
	
	public static boolean validarTipoBicoBS(String ip, Conexao conexao) {
		boolean bicoBS = false;
		try {
			//ResultSet rs = conexao.getRegistros("select EXISTS(select * from bicos where ip ='"+ip+"' and ((id_produtos1 = "+RemoteControlAccuload.ID_S500+" and id_produtos2 = "+RemoteControlAccuload.ID_B100+") or (id_produtos1 = "+RemoteControlAccuload.ID_S10+" and id_produtos2 = "+RemoteControlAccuload.ID_B100+")))", new String[] {});
			ResultSet rs = conexao.getRegistros("select EXISTS(select * from bicos where ip ='" + ip + "' and ((\"produto1Id\" = " + RemoteControlAccuload.ID_S500 + " and \"produto2Id\" = " + RemoteControlAccuload.ID_B100 + ") or (\"produto1Id\" = " + RemoteControlAccuload.ID_S10 + " and \"produto2Id\" = " + RemoteControlAccuload.ID_B100 + ")))", new String[] {});
			while(rs.next()) {
				bicoBS = rs.getBoolean(1);
				break;
			}
		} catch(Exception ex) {
			
		}
		return bicoBS;
	}
	/**************************************************************************************************/
	
	public static boolean validarExistenciaProdutoBSVariavel(int id_bicos,int id_produto,Conexao conexao) {
		boolean bicoBS = false;
		try {
			ResultSet rs = conexao.getRegistros("select EXISTS(select * from bicos_produtos_percentual_accuload where id_bicos = "+id_bicos+" and id_produtos = " + id_produto + ")", new String[] {});
			while(rs.next()) {
				bicoBS = rs.getBoolean(1);
				break;
			}
		} catch(Exception ex) {
			
		}
		return bicoBS;
	}
	/**************************************************************************************************/
	
	public static ProdutoPercentualAccuload consultarProdutoPercentualAccuload(int id_bicos,int id_produto,Conexao conexao) throws Exception {
		ProdutoPercentualAccuload ppa = new ProdutoPercentualAccuload();
		//ResultSet rs = conexao.getRegistros("select * from bicos_produtos_percentual_accuload where id_bicos = "+id_bicos+" and id_produtos = " + id_produto + " limit 1", new String[] {});
		ResultSet rs = conexao.getRegistros("select * from bicos_produtos_percentual_accuload where \"bicoId\" = "
				+ id_bicos + " and \"produtoId\" = " + id_produto + " limit 1", new String[] {});
		if(rs.next() == false) {
			LOG.info("[EX] consultarProdutoPercentualAccuload - produto BS nao localizado [id_produto"+id_produto+",id_bicos="+id_bicos+"]");
			throw new NoSuchElementException("nao foi localizado produto percentual");
		} else {
			/*ppa.setId_bicos_produtos_percentual_accuload(rs.getString(1));
			ppa.setId_bicos(rs.getInt(2));
			ppa.setId_produtos(rs.getString(3));
			ppa.setId_produtos_1(rs.getString(4));
			ppa.setId_produtos_2(rs.getString(5));
			ppa.setPercentual_produtos_1(rs.getString(6));
			ppa.setPercentual_produtos_2(rs.getString(7));*/
			ppa.setId_bicos_produtos_percentual_accuload(rs.getString("id"));
			ppa.setId_bicos(rs.getInt("bicoId"));
			ppa.setId_produtos(rs.getString("produtoId"));
			ppa.setId_produtos_1(rs.getString("produto1Id"));
			ppa.setId_produtos_2(rs.getString("produto2Id"));
			ppa.setPercentual_produtos_1(rs.getString("percentual_produtos_1"));
			ppa.setPercentual_produtos_2(rs.getString("percentual_produtos_2"));
		}
		return ppa;
	}
	/**************************************************************************************************/
	
}