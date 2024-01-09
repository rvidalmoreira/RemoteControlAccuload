package formatar;

import java.text.NumberFormat;
import java.util.Locale;
import java.util.regex.Pattern;

public class FormatarNumero {

    public static String formatar(double valor, int casas) {
        NumberFormat nfBrasil = NumberFormat.getNumberInstance(new Locale("pt", "BR"));

        nfBrasil.setMinimumFractionDigits(casas);
        nfBrasil.setMaximumFractionDigits(casas);

        return nfBrasil.format(valor);
    }
    
    /**
     * Atribui formatação de 'PARA' branco.
     * @param valor Valor inserido
     * @return Retorna valor decimal branco
     */
    public static String decimalFromBanco(String valor) {
        int parte_inteira = (int) Double.parseDouble(valor);

        if (parte_inteira == Double.parseDouble(valor)) {
            return String.valueOf(parte_inteira);
        } else {
            return valor.replaceAll(Pattern.quote("."), ",");
        }
    }
}