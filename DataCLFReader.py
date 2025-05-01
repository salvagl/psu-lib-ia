import pandas as pd
import re
import os
from tqdm import tqdm #>>> para mostrar barras de progreso

class DataCLFReader:
    def logs_to_df(self,logfile, output_dir, errors_file):
        """
        Parse log file and save parsed data into DataFrame, then save DataFrame to Parquet files.

        Args:
            logfile (str): Path to the log file.
            output_dir (str): Directory to save Parquet files.
            errors_file (str): Path to the file to store parsing errors.

        Returns:
            dataframe
        """ 
        # Define combined regex pattern for parsing log lines
        regex = r'^(?P<client>\S+) \S+ (?P<userid>\S+) \[(?P<datetime>[^\]]+)\] "(?P<method>[A-Z]+) (?P<request>[^ "]+)? HTTP/[0-9.]+" (?P<status>[0-9]{3}) (?P<size>[0-9]+|-) "(?P<referrer>[^"]*)" "(?P<useragent>[^"]*)'

        # Define column names for parsed log data
        columns = ['client', 'userid', 'datetime', 'method', 'request', 'status', 'size', 'referer', 'user_agent']

        df=None

        with open(logfile) as source_file:
            linenumber = 0
            parsed_lines = []
            
            for line in tqdm(source_file):
                try:
                    log_line = re.findall(regex, line)[0]
                    parsed_lines.append(log_line)
                except Exception as e:
                    with open(errors_file, 'at') as errfile:
                        print((line, str(e)), file=errfile)
                    continue
                linenumber += 1
                if linenumber % 250_000 == 0:
                    df = pd.DataFrame(parsed_lines, columns=columns)
                    df.to_parquet(f'{output_dir}/file_{linenumber}.parquet')
                    parsed_lines.clear()
            else:
                df = pd.DataFrame(parsed_lines, columns=columns)
                df.to_parquet(f'{output_dir}/file_{linenumber}.parquet')
                parsed_lines.clear()
        
        df = pd.read_parquet(output_dir)

        return df