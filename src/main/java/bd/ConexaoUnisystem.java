package bd;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Classe que implementa conexão com o banco  do supervisório. 
 * @author d0d0
 * @since 06-01-2017
 */
public class ConexaoUnisystem {
	//private static final Logger LOG = Logger.getLogger(ConexaoUnisystem.class);
	protected static Properties PROPERTIES;
		
	static {
		PROPERTIES = loadPropertiesFile("connection.properties");
	}

	protected static Properties loadPropertiesFile(String filePath) {
		Properties prop = new Properties();
		try (InputStream resourceAsStream = ConexaoUnisystem.class.getClassLoader().getResourceAsStream(filePath)) {
			prop.load(resourceAsStream);
		} catch (IOException e) {
			//LOG.error("Arquivo de propriedades nao carregado : " + filePath);
		}
		return prop;
	}
	
	
	private String ip = "";

	/**
	 * Conexão com o bd
	 */
    private final Conexao c = new Conexao();
    
    

    /**
     * Cria conexão com o banco.
     * @throws Exception
     */
    public ConexaoUnisystem() throws Exception {
    	String username = PROPERTIES.getProperty("unisystem.user");
    	String password = PROPERTIES.getProperty("unisystem.password");
    	String url = PROPERTIES.getProperty("unisystem.jdbc.url");
        c.conecta(url, username, password);
    }

    /**
     * Obtém a conexão com o banco.
     * @return Conexão com o banco.
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