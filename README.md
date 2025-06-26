# CooperUnify

Aplicação web em Python para processar arquivos CSV e Excel seguindo regras de conciliação. Permite upload de múltiplos arquivos Matera (`*.Matera.csv`), arquivos Dock (`*.Dock.xlsx`) e do arquivo `Relatório Contas e Cartões (de para).xlsm`. Após o processamento, é gerado um Excel com seis planilhas de resultados.

## Execução local

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Acesse `http://localhost:8000` no navegador para enviar os arquivos.

## Utilizando Docker

```bash
docker build -t cooperunify .
docker run -p 8000:80 cooperunify
```

Depois acesse `http://localhost:8000`.
