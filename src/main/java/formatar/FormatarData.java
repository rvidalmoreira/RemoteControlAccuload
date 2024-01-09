package formatar;

public class FormatarData {

    public static String DataGravar(String data) {
        // pega a data no formato dd/mm/aaaa e converte para aaaa-mm-dd
        data = FormatarTexto.somenteNumeros(data);

        if (data.length() == 8) {
            data = data.substring(4, 8) + "-" + data.substring(2, 4) + "-" + data.substring(0, 2);
        } else {
            return "0000-00-00";
        }

        return data;
    }

    public static String DataLer(String data) {
        // pega a data no formato aaaa-mm-dd e converte para dd/mm/aaaa
        if (data == null) {
            return "00/00/0000";
        }

        data = FormatarTexto.somenteNumeros(data);

        if (data.length() == 8) {
            data = data.substring(6, 8) + "/" + data.substring(4, 6) + "/" + data.substring(0, 4);
        } else {
            return "00/00/0000";
        }

        return data;
    }

    public static String DataExibir(String data) {
        // pega a data no formato dd/mm/aaaa e coloca as "/"
        data = FormatarTexto.somenteNumeros(data);

        if (data.length() == 8) {
            data = data.substring(0, 2) + "/" + data.substring(2, 4) + "/" + data.substring(4, 8);
        } else {
            return "00/00/0000";
        }

        return data;
    }
}