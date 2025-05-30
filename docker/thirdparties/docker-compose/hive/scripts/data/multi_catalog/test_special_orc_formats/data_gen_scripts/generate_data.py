import pyorc
import random

# When using an older version of pyorc (e.g., pyorc-0.3.0),
# if there are null values in the data, a present stream will be generated for the top levelstruct column.
# However, this behavior does not occur in newer versions of pyorc (e.g., pyorc-0.10.0) or in ORC files generated by tools like Hive or Spark.

# Define the schema for the ORC file
schema = "struct<id:int,name:string,age:int,salary:float,is_active:boolean>"

# Open the file and write ORC data
with open("data.orc.orc", "wb") as file:
    with pyorc.Writer(file, schema) as writer:
        for i in range(10000):
            # Generate each row of data, with a 10% chance of null for each column
            id = i if random.random() > 0.1 else None
            name = f"user_{i}" if random.random() > 0.1 else None
            age = 20 + (i % 40) if random.random() > 0.1 else None
            salary = 50000.0 + (i * 100.0) if random.random() > 0.1 else None
            is_active = i % 2 == 0 if random.random() > 0.1 else None
            # Write the data
            writer.write((id, name, age, salary, is_active))

print("ORC file generated: data.orc.orc")
