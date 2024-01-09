package remotecontrolaccuload;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
/**
 * Classe que implementa o protoco serial do comando a ser enviado ao Accuload.
 * @author d0d0
 * @since 06-01-2017
 *
 */
public class Comando {
    private Socket s;
    private PrintWriter envio;
    private BufferedReader retorno;
   
    /**
     * Envia comando ao Accuload
     * @param ip Ip do accuload
     * @param porta Porta do Accuload
     * @param endereco Endereço do Accuload
     * @param comando Comando a ser enviado.
     * @return Resposta do acculaod.
     */
    public String EnviaComando(String ip, int porta, String endereco, String comando) {
        String resposta = "";

        try {
            s = new Socket(ip, porta);

            envio = new PrintWriter(s.getOutputStream(), true);
            envio.print("*" + endereco + comando + "\r\n");
            envio.flush();

            retorno = new BufferedReader(new InputStreamReader(s.getInputStream()));

            int contador = 300; // 300 * 10 = 3 segundos

            while (!retorno.ready() && contador > 0) {
                // aguardando a resposta do accuload
                contador--;

                Thread.sleep(10);
            }

            if (contador <= 0) {
                resposta = "";
            } else {
                resposta = retorno.readLine();
            }

            // fechar
            envio.close();
            retorno.close();

            s.close();
        } catch (Exception e) {
        	RemoteControlAccuload.LOG.info(ip + " ERRO COMANDO: " + e.getLocalizedMessage());
        }

        //System.out.println("IP: " + ip + " COMANDO: " + comando + " RESPOSTA: " + resposta);

        return resposta;
    }
}