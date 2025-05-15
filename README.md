# Getting Started


Previous: 
- how to extract a batch-size from a giant dataset. Example to get 200MB of logs: head -c 200M access.log | sed '$d' > extracto.log


0. Install UV from : https://docs.astral.sh/uv/guides/projects/

1. Clone repo: git clone https://github.com/salvagl/psu-lib-ia.git

2. Require .env with the following env.var:
   - IPINFO_TOKEN=<token of ipinfo.io>
   
3. Run de example app: uv run main.py

4. Install D-Tale : uv pip install dtail (installed with simple "uv add" result in an error by kaleido dependency)

