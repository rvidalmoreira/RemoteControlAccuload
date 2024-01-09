package bd;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.commons.lang.StringUtils;

import formatar.FormatarTexto;

/**
 * Classe que implementa uma conexão com o banco de dados.
 * 
 * @author d0d0
 * @since 06-01-2017
 */
public class Conexao {

	/**
	 * Instância de conexão
	 */
	private Connection c;

	/**
	 * Conecta com o banco
	 * 
	 * @param host     Host
	 * @param port     porta
	 * @param database banco de dados
	 * @param username usuário
	 * @param password senha
	 * @throws Exception caso ocorra alguma exceção.
	 */
	public void conecta(String url, String username, String password) throws Exception {
	    Class.forName("org.postgresql.Driver");
	    this.c = DriverManager.getConnection(url, username, password);
	}

	/**
	 * Desfaz conexão com o banco.
	 * 
	 * @throws Exception Caso ocorra alguma exceção,
	 */
	public void desconecta() throws Exception {
		this.c.close();
	}

	/**
	 * Executa uma query no banco.
	 * 
	 * @param sql
	 * @param parametros
	 * @throws Exception
	 */
	public void executaSql(String sql, String[] parametros) throws Exception {
		PreparedStatement ps = this.c.prepareStatement(sql);

		for (int i = 0; i < parametros.length; i++) {
			ps.setString(i + 1, FormatarTexto.semAcentos(parametros[i].toUpperCase()));
		}

		ps.execute();

		ps.close();
	}

	/**
	 * Obtém o último id de auto incremento da instância.
	 * 
	 * @return último id incluído.
	 * @throws Exception Caso ocorra alguma exceção.
	 */
	public long getUltimoId() throws Exception {
		long ultimo = 0;

		PreparedStatement ps = this.c.prepareStatement("select last_insert_id()");

		ps.executeQuery();

		ResultSet rs = ps.getResultSet();

		while (rs.next()) {
			ultimo = Integer.parseInt(rs.getString(1));
		}

		rs.close();
		ps.close();

		return ultimo;
	}

	/**
	 * Obtém os registros solictados de acordo com a query
	 * 
	 * @param sql        Query de seleção.
	 * @param parametros Parâmetros passados a query.
	 * @return Registros resultados do select.
	 * @throws Exception Caso ocorra alguma exceção.
	 */
	public ResultSet getRegistros(String sql, String[] parametros) throws Exception {
		PreparedStatement ps = this.c.prepareStatement(sql);

		for (int i = 0; i < parametros.length; i++) {
			ps.setString(i + 1, formatar.FormatarTexto.semAcentos(parametros[i].toUpperCase()));
		}

		ps.executeQuery();

		ResultSet rs = ps.getResultSet();

		return rs;
	}

	/**
	 * Obtém a data do banco.
	 * 
	 * @return A data do banco.
	 * @throws Exception Caso ocorra alguma exceção.
	 */
	public String getData() throws Exception {
		ResultSet rsRegistros = this.getRegistros("select date(now())", new String[0]);

		while (rsRegistros.next()) {
			return rsRegistros.getString(1);
		}

		return "0000-00-00";
	}

	/**
	 * Obtém o dia da semana do banco.
	 * 
	 * @return Dia da semana.
	 * @throws Exception Caso ocorra algua exceção.
	 */
	public String getDiaDaSemana() throws Exception {
		//ResultSet rsRegistros = this.getRegistros("select date_format(date(now()), '%w')", new String[0]);
		ResultSet rsRegistros = this.getRegistros("SELECT EXTRACT(DOW FROM CURRENT_DATE)", new String[0]);

		while (rsRegistros.next()) {
			String sValor = rsRegistros.getString(1);

			if (sValor.equals("0")) {
				return "Domingo";
			}

			if (sValor.equals("1")) {
				return "Segunda";
			}

			if (sValor.equals("2")) {
				return "Terça";
			}

			if (sValor.equals("3")) {
				return "Quarta";
			}

			if (sValor.equals("4")) {
				return "Quinta";
			}

			if (sValor.equals("5")) {
				return "Sexta";
			}

			if (sValor.equals("6")) {
				return "Sábado";
			}
		}

		return "";
	}

	/**
	 * Obtém a hora atual do banco.
	 * 
	 * @return hora atual.
	 * @throws Exception Caso ocorra alguma exceção.
	 */
	public String getHora() throws Exception {
		//ResultSet rsRegistros = this.getRegistros("select time(now())", new String[0]);
		ResultSet rsRegistros = this.getRegistros("SELECT TO_CHAR(CURRENT_TIMESTAMP, 'HH24:MI:SS')", new String[0]);
		
		while (rsRegistros.next()) {
			return rsRegistros.getString(1);
		}

		return "00:00:00";
	}

	public String valor(String sql) {
		String retorno = "";
		try {
			PreparedStatement ps = this.c.prepareStatement(sql);

			ps.executeQuery();

			ResultSet rs = ps.getResultSet();
			while (rs.next()) {
				retorno = rs.getString(1);
			}
			ps.close();

			return retorno;
		} catch (Exception localException) {
		}
		return retorno;
	}

	public String valor(String sql, String[] parametros) {
		String retorno = "";
		try {
			PreparedStatement ps = this.c.prepareStatement(sql);
			for (int i = 0; i < parametros.length; i++) {
				ps.setString(i + 1, FormatarTexto.semAcentos(parametros[i].toUpperCase()));
			}
			ps.executeQuery();

			ResultSet rs = ps.getResultSet();
			while (rs.next()) {
				retorno = rs.getString(1);
			}
			ps.close();

			return retorno;
		} catch (Exception localException) {
		}
		return retorno;
	}

	public String[] valorChaveValorUnicoRegistro(String sql, String[] parametros) {
		String[] retorno = { "", "" };
		try {
			PreparedStatement ps = this.c.prepareStatement(sql);
			for (int i = 0; i < parametros.length; i++) {
				if (isNumeric(parametros[i]) && isInteger(parametros[i])) {
					ps.setInt(i + 1, Integer.parseInt(parametros[i])); 
				} else {
					ps.setString(i + 1, FormatarTexto.semAcentos(parametros[i].toUpperCase())); 
				}
			}
			ps.executeQuery();

			ResultSet rs = ps.getResultSet();
			if (rs.next()) {
				retorno[0] = rs.getString(1);
				retorno[1] = rs.getString(2);
			}
			ps.close();

			return retorno;
		} catch (Exception localException) {
		}
		return retorno;
	}
	
	public static boolean isNumeric(String str) {
	    return StringUtils.isNumeric(str);
	}
	
	public static boolean isInteger(String str) {
	    try {
	        Integer.parseInt(str);
	        return true;
	    } catch (NumberFormatException e) {
	        return false;
	    }
	}
}