package bd;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Classe que implementa conex�o com o banco  do supervis�rio. 
 * @author d0d0
 * @since 06-01-2017
 */
public class ConexaoSupervisorio {
	//private static final Logger LOG = Logger.getLogger(ConexaoSupervisorio.class);
	protected static Properties PROPERTIES;
		
	static {
		PROPERTIES = loadPropertiesFile("connection.properties");
	}

	protected static Properties loadPropertiesFile(String filePath) {
		Properties prop = new Properties();
		try (InputStream resourceAsStream = ConexaoSupervisorio.class.getClassLoader().getResourceAsStream(filePath)) {
			prop.load(resourceAsStream);
		} catch (IOException e) {
			//LOG.error("Arquivo de propriedades nao carregado : " + filePath);
		}
		return prop;
	}
	
	
	private String ip = "";

	/**
	 * Conex�o com o bd
	 */
    private final Conexao c = new Conexao();
    
    

    /**
     * Cria conex�o com o banco.
     * @throws Exception
     */
    public ConexaoSupervisorio() throws Exception {
    	String username = PROPERTIES.getProperty("supervisorio.user");
    	String password = PROPERTIES.getProperty("supervisorio.password");
    	String url = PROPERTIES.getProperty("supervisorio.jdbc.url");
        c.conecta(url, username, password);
    }

    /**
     * Obt�m a conex�o com o banco.
     * @return Conex�o com o banco.
     */
    public Conexao getConexao() {
        return this.c;
    }

    /**
     * Obtem o ip
     * @return o ip
     */
	public String getIp() {
		return ip;
	}

	/**
	 * Atribui um ip
	 * @param ip O ip
	 */
	public void setIp(String ip) {
		this.ip = ip;
	}
}