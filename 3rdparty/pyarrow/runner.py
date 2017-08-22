""" PyArrow Integration runner """

import sys
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def csv_to_parquet(in_file: str, out_file: str):
   print("reading csv...")
   csv = pd.read_csv(in_file)

   print("converting to table...")
   table = pa.Table.from_pandas(csv)

   print("writing parquet...")
   pq.write_table(table, out_file)

   print("done.")

if __name__ == "__main__":
   if(len(sys.argv) != 4):
      print("specify 3 parameters: mode, input, output")
   else:
      mode: str = sys.argv[1]
      in_file: str = sys.argv[2]
      out_file: str = sys.argv[3]

      print("mode: {}, in: [{}], out: [{}]".format(mode, in_file, out_file))

      if(mode == "out"):
         csv_to_parquet(in_file, out_file)
      else:
         print("unknown mode")

