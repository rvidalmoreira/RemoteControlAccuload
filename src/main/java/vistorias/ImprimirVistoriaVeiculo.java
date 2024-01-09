package vistorias;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.ResultSet;

import com.itextpdf.text.Document;
import com.itextpdf.text.Font;
import com.itextpdf.text.Font.FontFamily;
import com.itextpdf.text.Image;
import com.itextpdf.text.PageSize;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.pdf.PdfPCell;
import com.itextpdf.text.pdf.PdfPTable;
import com.itextpdf.text.pdf.PdfWriter;

import bd.ConexaoUnisystem;
import formatar.FormatarData;
import formatar.FormatarNumero;
import formatar.FormatarTexto;


/**
 * Classe que implementa o Imprimir a vistoria do veiculo
 * 
 * @author everton
 * @since 16-12-2016
 * @version 1.0
 */
public class ImprimirVistoriaVeiculo {

	/**
	 * Captura os dados e imprime na tela
	 * 
	 * @param id
	 *            Id do relatorio
	 * @throws Exception
	 */
	public ImprimirVistoriaVeiculo(int id, ConexaoUnisystem minhaConexao) throws Exception {

		//String[] parametros = new String[1]; // parâmetros da query

		//parametros[0] = String.valueOf(id);

		//ResultSet retorno = minhaConexao.getConexao().getRegistros("select tipos_de_ordem.descricao, "
			//	+ "veiculos.placa, veiculos.tipo, veiculos.renavam, ordens.id_veiculos, "
			//	+ "pessoas.nome_razao_social, pessoas.apelido_nome_fantasia, "
			//	+ "ordens.id_motoristas, motoristas.registro_cnh, motoristas.categoria_cnh, "
			//	+ "ordens.data_vistoria_veiculo, ordens.hora_vistoria_veiculo, ordens.sequencial_da_vistoria_veiculo "
			//	+ "from sacc.ordens " + "left outer join sacc.veiculos on veiculos.id_veiculos = ordens.id_veiculos "
			//	+ "left outer join sacc.motoristas on motoristas.id_motoristas = ordens.id_motoristas "
			//	+ "left outer join sacc.pessoas on pessoas.id_pessoas = motoristas.id_pessoas "
			//	+ "left outer join sacc.tipos_de_ordem on tipos_de_ordem.id_tipos_de_ordem = ordens.id_tipos_de_ordem "
			//	+ "where ordens.id_ordens = ?", parametros);
		
		ResultSet retorno = minhaConexao.getConexao().getRegistros("select tipos_ordem.descricao, "
				+ "veiculos.placa, veiculos.tipo, veiculos.renavam, ordens.\"veiculoId\", "
				+ "pessoas.nome_razao_social, pessoas.apelido_nome_fantasia, "
				+ "ordens.\"motoristaId\", motoristas.registro_cnh, motoristas.categoria_cnh, "
				+ "ordens.data_vistoria_veiculo, ordens.hora_vistoria_veiculo, ordens.sequencial_da_vistoria_veiculo "
				+ "from ordens " + "left outer join veiculos on veiculos.id = ordens.\"veiculoId\" "
				+ "left outer join motoristas on motoristas.id = ordens.\"motoristaId\" "
				+ "left outer join pessoas on pessoas.id = motoristas.\"pessoaId\" "
				+ "left outer join tipos_ordem on tipos_ordem.id = ordens.\"tipoOrdemId\" "
				+ "where ordens.id = "+id, new String[0]);
		
		if (!retorno.next()) {
			System.out.println("Erro sem dados para impressao do relatorio");
			return;
		}

		String[] lista = new String[13];

		for (int i = 0; i < 13; i++) {
			lista[i] = retorno.getString(i + 1);
		}

		// fontes
		Font f1 = new Font(FontFamily.HELVETICA, 8, Font.BOLD);
		Font f2 = new Font(FontFamily.HELVETICA, 8, Font.NORMAL);
		Font f3 = new Font(FontFamily.COURIER, 6, Font.ITALIC);
		Font f5 = new Font(FontFamily.HELVETICA, 8, Font.NORMAL);
		Font f6 = new Font(FontFamily.COURIER, 8, Font.NORMAL);

		// códigos de barras
		GeradorDeCodigoDeBarras cb_ordem = new GeradorDeCodigoDeBarras("cb", String.valueOf(String.format("%09d", id)));

		Image logo = null;

		try {
			logo = Image.getInstance("/var/www/html/portalunibraspe/images/logos_clientes/unibraspe.jpg");
		} catch (Exception e) {
			System.out.println(e);
		}

		Image codbar = Image.getInstance(cb_ordem.getPathFile());

		// documento
		Document doc = new Document(PageSize.A4, 20, 20, 10, 10);

		FileOutputStream os = new FileOutputStream("/tmp/vistoria_" + id + ".pdf");

		PdfWriter.getInstance(doc, os);

		doc.open();

		// cabeçalho
		PdfPTable cabecalho = new PdfPTable(new float[] { 0.2f, 0.6f, 0.2f });
		cabecalho.setWidthPercentage(100);

		PdfPCell cabecalho1 = new PdfPCell();
		cabecalho1.setBorderWidth(0);
		cabecalho1.addElement(logo);

		PdfPCell cabecalho2 = new PdfPCell();
		cabecalho2.setBorderWidth(0);
		cabecalho2.addElement(new Paragraph("UNIBRASPE - BRASILEIRA DE PETRÓLEO S/A", f1));
		cabecalho2.addElement(new Paragraph("VISTORIA DE VEÍCULOS (" + lista[0] + ")", f2));
		cabecalho2.addElement(
				new Paragraph("\nSORTEIO REALIZADO EM " + FormatarData.DataLer(lista[10]) + " ÀS " + lista[11], f2));

		PdfPCell cabecalho3 = new PdfPCell();
		cabecalho3.setBorderWidth(0);
		cabecalho3.addElement(new Paragraph("  VISTORIA " + lista[12] + "\n\n", f6));
		cabecalho3.addElement(codbar);

		cabecalho.addCell(cabecalho1);
		cabecalho.addCell(cabecalho2);
		cabecalho.addCell(cabecalho3);

		doc.add(cabecalho);

		// apagar código de barras
		File cb = new File(cb_ordem.getPathFile());

		cb.deleteOnExit();

		//String[] parametros1 = new String[1]; // parâmetros da query

		//parametros1[0] = String.valueOf(id);

		//ResultSet retorno1 = minhaConexao.getConexao()
			//	.getRegistros("select ordens_clientes.id_clientes, "
				//		+ "pessoas.nome_razao_social, pessoas.apelido_nome_fantasia " + "from sacc.ordens_clientes "
					//	+ "left outer join sacc.clientes on clientes.id_clientes = ordens_clientes.id_clientes "
					//	+ "left outer join sacc.pessoas on pessoas.id_pessoas = clientes.id_pessoas "
					//	+ "where ordens_clientes.id_ordens = ? limit 1", parametros1);
		
		ResultSet retorno1 = minhaConexao.getConexao()
				.getRegistros("select ordens_clientes.\"clienteId\", "
						+ "pessoas.nome_razao_social, pessoas.apelido_nome_fantasia " + "from ordens_clientes "
						+ "left outer join clientes on clientes.id = ordens_clientes.\"clienteId\" "
						+ "left outer join pessoas on pessoas.id = clientes.\"pessoaId\" "
						+ "where ordens_clientes.\"ordemId\" =" +id+ " limit 1", new String[0]);

		retorno1.next();

		String[] lista1 = new String[3];

		for (int i = 0; i < 3; i++) {
			lista1[i] = retorno1.getString(i + 1);
		}

		// cliente
		PdfPTable cliente = new PdfPTable(new float[] { 0.1f, 0.9f });
		cliente.setWidthPercentage(100);

		PdfPCell cliente1 = new PdfPCell();
		cliente1.setBorderWidth(0);

		cliente1.addElement(new Paragraph("CLIENTE:", f2));
		cliente1.addElement(new Paragraph("VEÍCULO:", f2));
		cliente1.addElement(new Paragraph("MOTORISTA:", f2));

		PdfPCell cliente2 = new PdfPCell();
		cliente2.setBorderWidth(0);

		cliente2.addElement(new Paragraph(
				FormatarTexto.espacos(lista1[2] + " / " + lista1[1], 50, 'E', ' ').trim() + " (" + lista1[0] + ")",
				f1)); // cliente
		cliente2.addElement(
				new Paragraph(FormatarTexto.espacos(lista[1] + " / " + lista[2] + " / " + lista[3], 50, 'E', ' ').trim()
						+ " (" + lista[4] + ")", f1)); // veículo

		if (!"".equals(lista[6])) {
			cliente2.addElement(new Paragraph(FormatarTexto.espacos(lista[6] + " / " + lista[5], 50, 'E', ' ').trim()
					+ " / " + lista[8] + "/" + lista[9] + " (" + lista[7] + ")", f1)); // motorista
		} else {
			cliente2.addElement(new Paragraph(FormatarTexto.espacos(lista[5], 50, 'E', ' ').trim() + " / " + lista[8]
					+ "/" + lista[9] + " (" + lista[7] + ")", f1)); // motorista
		}

		cliente.addCell(cliente1);
		cliente.addCell(cliente2);

		cliente.setSpacingAfter(10);

		doc.add(cliente);

		//String[] parametros4 = new String[1]; // parâmetros da query

		//parametros4[0] = String.valueOf(id);

		//ResultSet retorno4 = minhaConexao.getConexao().getRegistros(
			//	"select veiculos.numero_compartimentos, veiculos_compartimentos.id_veiculos, veiculos.placa "
				//		+ "from sacc.ordens_clientes "
					//	+ "left outer join sacc.veiculos_compartimentos on veiculos_compartimentos.id_veiculos_compartimentos = ordens_clientes.id_veiculos_compartimentos "
					//	+ "left outer join sacc.veiculos on veiculos.id_veiculos = veiculos_compartimentos.id_veiculos "
					//	+ "where ordens_clientes.id_ordens = ? " + "group by veiculos_compartimentos.id_veiculos "
					//	+ "order by ordens_clientes.id_ordens_clientes",
				//parametros4);
		
		ResultSet retorno4 = minhaConexao.getConexao().getRegistros(
				"select veiculos.numero_compartimentos, veiculos_compartimentos.\"veiculoId\", veiculos.placa "
						+ "from ordens_clientes "
						+ "left outer join veiculos_compartimentos on veiculos_compartimentos.id = ordens_clientes.\"veiculoCompartimentoId\" "
						+ "left outer join veiculos on veiculos.id = veiculos_compartimentos.\"veiculoId\" "
						+ "where ordens_clientes.\"ordemId\" = "+id+" "
						+ "group by veiculos_compartimentos.\"veiculoId\", veiculos.numero_compartimentos, veiculos.placa, ordens_clientes.id "
						+ "order by ordens_clientes.id",
						new String[0]);

		Double total;

		while (retorno4.next()) {

			String[] lista4 = new String[3];

			for (int i = 0; i < 3; i++) {
				lista4[i] = retorno4.getString(i + 1);
			}

			int num_compartimentos = Integer.parseInt(lista4[0]);
			// compartimentos
			PdfPTable compartimentos = new PdfPTable(
					new float[] { 0.1f, 0.08f, 0.08f, 0.08f, 0.08f, 0.08f, 0.08f, 0.08f, 0.08f, 0.08f, 0.08f, 0.1f });
			compartimentos.setWidthPercentage(100);

			// placa
			PdfPCell compartimentos1 = new PdfPCell(new Paragraph(
					lista4[2]
							+ "\n-----------------\nPRODUTO\n-----------------\nVOLUME\n-----------------\nNÍVEL (A)\n-----------------\nCARREG (V)\n\n-----------------\nCARREG (D)\n\n-----------------\nCORREÇÃO\n\n-----------------\nTUBULAÇÃO\n\n",
					f5));
			compartimentos1.setHorizontalAlignment(PdfPCell.ALIGN_CENTER);

			compartimentos.addCell(compartimentos1);

			PdfPCell compartimentos2;

			total = 0.0;

			for (int j = 1; j <= 10; j++) {
				if (num_compartimentos >= j) {
					String celula;

					//ResultSet retorno5 = minhaConexao.getConexao()
						//	.getRegistros("select ordens_clientes.id_produtos, produtos.apelido, "
							//		+ "ordens_clientes.quantidade, veiculos.placa, veiculos_compartimentos.compartimento, "
							//		+ "if(veiculos_compartimentos.nivel1_capacidade_l = ordens_clientes.quantidade, '1', if(veiculos_compartimentos.nivel2_capacidade_l = ordens_clientes.quantidade, '2', if(veiculos_compartimentos.nivel3_capacidade_l = ordens_clientes.quantidade, '3', if(veiculos_compartimentos.nivel4_capacidade_l = ordens_clientes.quantidade, '4', if(veiculos_compartimentos.nivel5_capacidade_l = ordens_clientes.quantidade, '5', NULL))))) as nivel, "
							//		+ "if(veiculos_compartimentos.nivel1_capacidade_l = ordens_clientes.quantidade, veiculos_compartimentos.nivel1_espaco_cheio_mm, if(veiculos_compartimentos.nivel2_capacidade_l = ordens_clientes.quantidade, veiculos_compartimentos.nivel2_espaco_cheio_mm, if(veiculos_compartimentos.nivel3_capacidade_l = ordens_clientes.quantidade, veiculos_compartimentos.nivel3_espaco_cheio_mm, if(veiculos_compartimentos.nivel4_capacidade_l = ordens_clientes.quantidade, veiculos_compartimentos.nivel4_espaco_cheio_mm, if(veiculos_compartimentos.nivel5_capacidade_l = ordens_clientes.quantidade, veiculos_compartimentos.nivel5_espaco_cheio_mm, NULL))))) as altura "
							//		+ "from sacc.ordens_clientes "
							//		+ "left outer join sacc.produtos on produtos.id_produtos = ordens_clientes.id_produtos "
							//		+ "left outer join sacc.veiculos_compartimentos on veiculos_compartimentos.id_veiculos_compartimentos = ordens_clientes.id_veiculos_compartimentos "
							//		+ "left outer join sacc.veiculos on veiculos.id_veiculos = veiculos_compartimentos.id_veiculos "
							//		+ "where ordens_clientes.id_ordens = " + id
							//		+ " and veiculos_compartimentos.id_veiculos = " + lista4[1]
							//		+ " and veiculos_compartimentos.compartimento = " + j + " "
							//		+ "order by ordens_clientes.id_ordens_clientes", new String[0]);
					
					ResultSet retorno5 = minhaConexao.getConexao().getRegistros("SELECT ordens_clientes.\"produtoId\", produtos.apelido, ordens_clientes.quantidade, veiculos.placa, veiculos_compartimentos.compartimento, " +
						    "CASE WHEN veiculos_compartimentos.nivel1_capacidade_l = ordens_clientes.quantidade THEN '1' " +
						    "WHEN veiculos_compartimentos.nivel2_capacidade_l = ordens_clientes.quantidade THEN '2' " +
						    "WHEN veiculos_compartimentos.nivel3_capacidade_l = ordens_clientes.quantidade THEN '3' " +
						    "WHEN veiculos_compartimentos.nivel4_capacidade_l = ordens_clientes.quantidade THEN '4' " +
						    "WHEN veiculos_compartimentos.nivel5_capacidade_l = ordens_clientes.quantidade THEN '5' " +
						    "ELSE 0 END AS nivel, " +
						    "CASE WHEN veiculos_compartimentos.nivel1_capacidade_l = ordens_clientes.quantidade THEN veiculos_compartimentos.nivel1_espaco_cheio_mm " +
						    "WHEN veiculos_compartimentos.nivel2_capacidade_l = ordens_clientes.quantidade THEN veiculos_compartimentos.nivel2_espaco_cheio_mm " +
						    "WHEN veiculos_compartimentos.nivel3_capacidade_l = ordens_clientes.quantidade THEN veiculos_compartimentos.nivel3_espaco_cheio_mm " +
						    "WHEN veiculos_compartimentos.nivel4_capacidade_l = ordens_clientes.quantidade THEN veiculos_compartimentos.nivel4_espaco_cheio_mm " +
						    "WHEN veiculos_compartimentos.nivel5_capacidade_l = ordens_clientes.quantidade THEN veiculos_compartimentos.nivel5_espaco_cheio_mm " +
						    "ELSE 0 END AS altura " +
						    "FROM ordens_clientes " +
						    "LEFT OUTER JOIN produtos ON produtos.id = ordens_clientes.\"produtoId\" " +
						    "LEFT OUTER JOIN veiculos_compartimentos ON veiculos_compartimentos.id = ordens_clientes.\"veiculoCompartimentoId\" " +
						    "LEFT OUTER JOIN veiculos ON veiculos.id = veiculos_compartimentos.\"veiculoId\" " +
						    "WHERE ordens_clientes.\"ordemId\" = " + id +
						    " AND veiculos_compartimentos.\"veiculoId\" = " + lista4[1] +
						    " AND veiculos_compartimentos.compartimento = " + j + " " +
						    "ORDER BY ordens_clientes.id", new String[0]);

					if (retorno5.next()) {
						String[] lista5 = new String[7];

						for (int i = 0; i < 7; i++) {
							lista5[i] = retorno5.getString(i + 1);
						}

						celula = "C" + String.format("%02d", j) + "N" + lista5[5];
						celula += "\n---------------\n" + lista5[1]; // produto
						celula += "\n---------------\n" + lista5[2]; // quantidade
						celula += "\n---------------\n" + lista5[6]; // altura
																		// (a)
						celula += "\n---------------\n"; // altura (v)
						celula += "\n\n---------------\n"; // altura (d)
						celula += "\n\n---------------\n"; // correção
						celula += "\n\n---------------\n"; // tubulação

						total += Double.parseDouble(lista5[2]);
					} else {
						celula = "C" + String.format("%02d", j) + "N0";
						celula += "\n\n" + "VAZIO";
					}

					compartimentos2 = new PdfPCell(new Paragraph(celula, f5));
					compartimentos2.setHorizontalAlignment(PdfPCell.ALIGN_CENTER);
				} else {
					compartimentos2 = new PdfPCell(new Paragraph("", f5));
				}

				compartimentos.addCell(compartimentos2);
			}

			// total
			PdfPCell compartimentos3 = new PdfPCell(
					new Paragraph("TOTAL\n\n\n\n" + FormatarNumero.decimalFromBanco(Double.toString(total)), f5));
			compartimentos3.setHorizontalAlignment(PdfPCell.ALIGN_CENTER);

			compartimentos.addCell(compartimentos3);

			doc.add(compartimentos);

			doc.add(new Paragraph(
					"CARREG CONFORME [ SIM ] [ NÃO ]                NÍVEL CONFORME [ SIM ] [ NÃO ]                TUBULAÇÃO CONFORME [ SIM ] [ NÃO ]\n\n",
					f2));
		}

		// data e hora
		doc.add(new Paragraph(
				"VISTORIADO EM ______ / ______ / ____________ ÀS ______ : ______    POR ____________________________________________\n\n",
				f6));

		// observações
		doc.add(new Paragraph(
				"OBSERVAÇÕES   _____________________________________________________________________________________________________\n\n               _____________________________________________________________________________________________________\n\n               _____________________________________________________________________________________________________\n\n",
				f6));

		// reinspeção
		doc.add(new Paragraph(
				"REINSPEÇÃO EM ______ / ______ / ____________ VERIFICADO / LIBERADO POR ____________________________________________",
				f6));

		// assinaturas
		PdfPTable assinaturas = new PdfPTable(new float[] { 0.25f, 0.25f, 0.25f, 0.25f });
		assinaturas.setWidthPercentage(100);

		PdfPCell assinaturas1 = new PdfPCell(new Paragraph("________________\nMOTORISTA", f5));
		assinaturas1.setBorderWidth(0);
		assinaturas1.setHorizontalAlignment(PdfPCell.ALIGN_CENTER);

		PdfPCell assinaturas2 = new PdfPCell(new Paragraph("________________\nOPERADOR", f5));
		assinaturas2.setBorderWidth(0);
		assinaturas2.setHorizontalAlignment(PdfPCell.ALIGN_CENTER);

		PdfPCell assinaturas3 = new PdfPCell(new Paragraph("________________\nVISTORIADOR", f5));
		assinaturas3.setBorderWidth(0);
		assinaturas3.setHorizontalAlignment(PdfPCell.ALIGN_CENTER);

		PdfPCell assinaturas4 = new PdfPCell(new Paragraph("________________\nPROGRAMADOR", f5));
		assinaturas4.setBorderWidth(0);
		assinaturas4.setHorizontalAlignment(PdfPCell.ALIGN_CENTER);

		assinaturas.addCell(assinaturas1);
		assinaturas.addCell(assinaturas2);
		assinaturas.addCell(assinaturas3);
		assinaturas.addCell(assinaturas4);

		assinaturas.setSpacingBefore(50);

		doc.add(assinaturas);

		// rodapé
		doc.add(new Paragraph("Arquivo gerado em: " + minhaConexao.getConexao().getData() + " "
				+ minhaConexao.getConexao().getHora() + " por: Accuload ", f3));

		if (doc != null) {
			doc.close();
		}

		if (os != null) {
			os.close();
		}

		// File pdf = new File("/tmp/_vistoria_" + id + ".pdf");

	}
}