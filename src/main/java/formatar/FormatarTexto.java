package formatar;

import java.text.Normalizer;

public class FormatarTexto {

    public static String semAcentos(String valor) {
        return Normalizer.normalize(valor, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");
    }

    public static String somenteNumeros(String valor) {
        if(valor == null) {
            valor = "";
        }

        return valor.replaceAll("[^0-9]", "");
    }

    public static String espacos(String texto, int quantidade, char alinhamento, char caracter) {
        if(texto == null) {
            texto = "";
        }

        while (texto.length() < quantidade) {
            switch (alinhamento) {
                case 'E': // esquerda
                    texto = texto + caracter;
                    break;
                case 'C': // centro
                    texto = caracter + texto + caracter;
                    break;
                case 'D': // direita
                    texto = caracter + texto;
                    break;
            }
        }

        return texto.substring(0, quantidade);
    }

    public static String cpf(String cpf) {
        // 000.000.000-00
        cpf = somenteNumeros(cpf);

        if (cpf.length() == 11) {
            cpf = cpf.substring(0, 3) + "." + cpf.substring(3, 6) + "." + cpf.substring(6, 9) + "-" + cpf.substring(9, 11);
        } else {
            return "";
        }

        return cpf;
    }

    public static String cnpj(String cnpj) {
        // 00.000.000/0000-00
        cnpj = somenteNumeros(cnpj);

        if (cnpj.length() == 14) {
            cnpj = cnpj.substring(0, 2) + "." + cnpj.substring(2, 5) + "." + cnpj.substring(5, 8) + "/" + cnpj.substring(8, 12) + "-" + cnpj.substring(12, 14);
        } else {
            return "";
        }

        return cnpj;
    }
}