import json
import os
import sys 

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])
 
 
from gard_expertise_data_generator import gard_expertise_generator

# import your expertise code here

def main(max_items: int = 200) -> None:

    # Replace with your MySQL connection here
    #mysql_connection = DBConnection().mysql_conn()
    #if mysql_connection is None:
    #    raise RuntimeError("Failed to create MySQL connection.")

    try:
        for index, data in enumerate(gard_expertise_generator(mysql_connection), start=1):

            print(f"\n===== item #{index} | gardId={data.get('gardId')} =====")
            print(json.dumps(data, indent=2, ensure_ascii=False))

            #
            #
            # Pass the data into your expertise method here.
            #
            #
            resultJson = YourCode.method(data)
            print(json.dumps(resultJson, indent=2, ensure_ascii=False))
                  
            if index >= max_items:
                break
    finally:
        mysql_connection.close()


if __name__ == "__main__":
    main()
