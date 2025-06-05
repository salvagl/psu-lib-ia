import re
import pandas as pd
from tqdm import tqdm

class DataCLFReader:
    def logs_to_df(self, logfile, output_dir, errors_file):
        regex = r'^(?P<client>\S+) \S+ (?P<userid>\S+) \[(?P<datetime>[^\]]+)\] "(?P<method>[A-Z]+) (?P<request>[^ "]+)? HTTP/[0-9.]+" (?P<status>[0-9]{3}) (?P<size>[0-9]+|-) "(?P<referrer>[^"]*)" "(?P<useragent>[^"]*)"'

        parsed_lines = []
        malformed_lines = []
        linenumber = 0

        with open(logfile, encoding='utf-8', errors='replace') as source_file:
            for line in tqdm(source_file):
                match = re.match(regex, line)
                if match:
                    group = match.groupdict()
                    parsed_lines.append({
                        'client': group['client'],
                        'userid': group['userid'],
                        'datetime': group['datetime'],
                        'method': group['method'],
                        'request': group['request'],
                        'status': int(group['status']),
                        'size_in_bytes': int(group['size']) if group['size'].isdigit() else 0,
                        'referer': group['referrer'],
                        'user_agent': group['useragent'],
                        'parsed_ok': 1,
                        'raw_request': line.strip()
                    })
                else:
                    malformed_lines.append({
                        'client': self._extract_ip(line),
                        'userid': '-',
                        'datetime': self._extract_datetime(line),
                        'method': 'unknown',
                        'request': 'unknown',
                        'status': -100,
                        'size_in_bytes': -100,
                        'referer': 'unknown',
                        'user_agent': 'unknown',
                        'parsed_ok': 0,
                        'raw_request': line.strip()
                    })
                    with open(errors_file, 'at') as errfile:
                        print(line.strip(), file=errfile)

                linenumber += 1
                if linenumber % 250_000 == 0:
                    self._save_chunk(parsed_lines + malformed_lines, output_dir, linenumber)
                    parsed_lines.clear()
                    malformed_lines.clear()

            # guardar lo que queda
            if parsed_lines or malformed_lines:
                self._save_chunk(parsed_lines + malformed_lines, output_dir, linenumber)

        df = pd.read_parquet(output_dir)
        return df

    def _save_chunk(self, data, output_dir, linenumber):
        df = pd.DataFrame(data)
        df.to_parquet(f'{output_dir}/file_{linenumber}.parquet', index=False)

    def _extract_ip(self, line):
        # Intenta extraer la IP al principio
        match = re.match(r'^(\d+\.\d+\.\d+\.\d+)', line)
        return match.group(1) if match else 'unknown'

    def _extract_datetime(self, line):
        # Intenta extraer la fecha entre corchetes
        match = re.search(r'\[(.*?)\]', line)
        return match.group(1) if match else 'unknown'
