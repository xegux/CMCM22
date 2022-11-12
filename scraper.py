import pandas as pd
import numpy as np
import tabula


# Read PDF into list of DataFrame
df = tabula.read_pdf("trafficcount.pdf", output_format='dataframe', pages='7', lattice=False)
print(df)


# Read remote PDF into list of DataFrame
# dataframe_2 = tabula.read_pdf("https://github.com/tabulapdf/tabula-java/raw/master/src/test/resources/technology/tabula/arabic.pdf")

# Convert PDF into CSV file
# tabula.convert_into("sept22.pdf", "output.csv", output_format="csv", pages='1,2')