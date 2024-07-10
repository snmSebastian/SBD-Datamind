import fitz  # PyMuPDF
from PIL import Image

def pdf_page_to_png(pdf_path, page_number, output_path):
    # Abrir el archivo PDF
    document = fitz.open(pdf_path)
    
    # Obtener la página específica (notar que la numeración empieza en 0)
    page = document.load_page(page_number - 1)
    
    # Convertir la página a una imagen en formato Pixmap
    pix = page.get_pixmap()
    
    # Guardar la imagen como PNG
    image = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    image.save(output_path, "PNG")
    
    print(f'Página {page_number} del PDF guardada como imagen PNG en {output_path}')

# Ruta al archivo PDF
pdf_path = r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Sebastian\archivo.pdf'

# Número de la página que deseas extraer (por ejemplo, la página 1)
page_number = 1

# Ruta donde se guardará la imagen PNG
output_path = r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Sebastian\archivosalida.png'

pdf_page_to_png(pdf_path, page_number, output_path)

