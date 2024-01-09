package vistorias;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import org.krysalis.barcode4j.impl.code39.Code39Bean;
import org.krysalis.barcode4j.output.bitmap.BitmapCanvasProvider;
import org.krysalis.barcode4j.tools.UnitConv;

/**
 * Classe que implementa o Gerador de Codigo de Barras
 * @author everton
 * @since 15-12-2016
 * @version 1.0
 */
public class GeradorDeCodigoDeBarras {

	/**
	 * Pasta
	 */
    private String pasta = "/tmp/";
    /**
     * Tipo
     */
    private String tipo = "";
    /**
     * Codigo
     */
    private String codigo = "";
    
    /**
     * Atribui o valor do arquivo
     * @return Retona o valor em forma de testo
     */
    public String getPathFile() {
        
        
        return this.pasta + "/" + this.tipo + "_" + this.codigo + ".jpg";
    }
    
    /**
     * Gera um novo código de barras par ao numero informado
     * @param tipo Tipo do codigo
     * @param codigo Codigo
     */
    public GeradorDeCodigoDeBarras(String tipo, String codigo) {
        this.tipo = tipo;
        this.codigo = codigo;
        
        try {
            // criar pasta tmp
            
            //Create the barcode bean
            Code39Bean bean = new Code39Bean();

            final int dpi = 150;

            //Configure the barcode generator
            bean.setModuleWidth(UnitConv.in2mm(1.0f / dpi)); //makes the narrow bar 
            //width exactly one pixel
            bean.setWideFactor(3);
            bean.doQuietZone(false);
            bean.setBarHeight(5);

            //Open output file
            File outputFile = new File(this.pasta + "/" + this.tipo + "_" + this.codigo + ".jpg");
            
            OutputStream out = new FileOutputStream(outputFile);
            
            try {
                //Set up the canvas provider for monochrome JPEG output 
                BitmapCanvasProvider canvas = new BitmapCanvasProvider(out, "image/jpeg", dpi, BufferedImage.TYPE_BYTE_BINARY, false, 0);

                //Generate the barcode
                bean.generateBarcode(canvas, codigo);
                bean.setFontSize(0);

                //Signal end of generation
                canvas.finish();
            } finally {
                out.close();
            }
            
            outputFile.deleteOnExit();
        } catch (Exception e) {
        }
    }
}