import os
import re
import pandas as pd
import numpy as np
from typing import List
from fastapi import FastAPI, Request, UploadFile, File
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from tempfile import NamedTemporaryFile

app = FastAPI()

# directory for Jinja2 templates
templates = Jinja2Templates(directory="app/templates")

def extract_date(filename: str) -> str:
    """Extract a date in YYYY-MM-DD or YYYYMMDD format from the filename."""
    match = re.search(r"(\d{4}[\-_]?\d{2}[\-_]?\d{2})", filename)
    if not match:
        return ""
    digits = match.group(1).replace("_", "-")
    if '-' in digits:
        if digits.count('-') == 2:
            return digits
        digits = digits.replace('-', '')
    return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"

def process_matera(file: UploadFile) -> pd.DataFrame:
    df = pd.read_csv(file.file, dtype=str)
    file.file.seek(0)
    df['nVlrLanc'] = pd.to_numeric(df.get('nVlrLanc'), errors='coerce').fillna(0)
    df['sCpf_Cnpj'] = df.get('sCpf_Cnpj', '').astype(str)
    if 'nHistorico' in df.columns:
        df.loc[df['nHistorico'] == '9001', 'nVlrLanc'] *= -1
    df['date_doc'] = extract_date(file.filename)
    df.rename(columns={'sCpf_Cnpj': 'CPF'}, inplace=True)
    return df

def process_dock(file: UploadFile) -> pd.DataFrame:
    df = pd.read_excel(file.file, engine='openpyxl')
    file.file.seek(0)
    first_row = df[df.get('Unnamed: 2').notna()].index[0]
    df = df.iloc[first_row:]
    df.columns = df.iloc[0]
    df = df[1:]
    df = df.loc[:, ~df.columns.isna()]
    if 'Id Tipo Transacao' in df.columns and 'Valor' in df.columns:
        df.loc[df['Id Tipo Transacao'].isin([30224, 30350]), 'Valor'] *= -1
    df['date_doc'] = extract_date(file.filename)
    return df

def process_depara(file: UploadFile) -> pd.DataFrame:
    df = pd.read_excel(file.file, engine='openpyxl')
    file.file.seek(0)
    first_row = df[df.get('Unnamed: 2').notna()].index[0]
    df = df.iloc[first_row:]
    df.columns = df.iloc[0]
    df = df[1:]
    df = df.loc[:, ~df.columns.isna()]
    return df

def generate_report(matera_files: List[UploadFile], dock_files: List[UploadFile], depara_file: UploadFile) -> str:
    matera_all = pd.concat([process_matera(f) for f in matera_files], ignore_index=True)
    dock_all = pd.concat([process_dock(f) for f in dock_files], ignore_index=True)
    depara = process_depara(depara_file)
    dock_all = pd.merge(dock_all, depara[['Id Conta', 'CPF', 'Nome', 'Status Conta', 'Data Cadastramento']], on='Id Conta', how='left')
    # ensure types
    dock_all['CPF'] = dock_all['CPF'].astype(str)
    matera_all['CPF'] = matera_all['CPF'].astype(str)
    # comparisons grouped by date
    m_sum = matera_all.groupby(['CPF', 'date_doc'])['nVlrLanc'].sum().reset_index()
    d_sum = dock_all.groupby(['CPF', 'date_doc'])['Valor'].sum().reset_index()
    comp = pd.merge(m_sum, d_sum, on=['CPF', 'date_doc'], how='outer').fillna(0)
    comp['difference'] = comp['nVlrLanc'] - comp['Valor']
    comparison_df_filtered_321 = comp[comp['difference'] != 0]
    # comparisons without date
    m_sum2 = matera_all.groupby('CPF')['nVlrLanc'].sum().reset_index()
    d_sum2 = dock_all.groupby('CPF')['Valor'].sum().reset_index()
    comp2 = pd.merge(m_sum2, d_sum2, on='CPF', how='outer').fillna(0)
    comp2['difference'] = comp2['nVlrLanc'] - comp2['Valor']
    comparison_df_filtered_591 = comp2[comp2['difference'] != 0]
    cpfs_321 = set(comparison_df_filtered_321['CPF'])
    cpfs_591 = set(comparison_df_filtered_591['CPF'])
    exclusive_cpfs = cpfs_591 - cpfs_321
    se_matam_dock = dock_all[dock_all['CPF'].isin(exclusive_cpfs)]
    se_matam_matera = matera_all[matera_all['CPF'].isin(exclusive_cpfs)]
    summary_80_grouped = se_matam_dock.groupby('CPF').agg({
        'Id Conta': lambda x: ', '.join(x.astype(str).unique()),
        'Nome': 'first'
    }).reset_index()
    nao_se_matam_dock = dock_all[dock_all['CPF'].isin(cpfs_321)]
    nao_se_matam_matera = matera_all[matera_all['CPF'].isin(cpfs_321)]
    summary_321_grouped = nao_se_matam_dock.groupby('CPF').agg({
        'Id Conta': lambda x: ', '.join(x.astype(str).unique()),
        'Nome': 'first'
    }).reset_index()
    with NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
        with pd.ExcelWriter(tmp.name, engine='openpyxl') as writer:
            se_matam_dock.to_excel(writer, sheet_name='se_matam_dock', index=False)
            se_matam_matera.to_excel(writer, sheet_name='se_matam_matera', index=False)
            summary_80_grouped.to_excel(writer, sheet_name='summary_80_grouped', index=False)
            nao_se_matam_dock.to_excel(writer, sheet_name='nao_se_matam_dock', index=False)
            nao_se_matam_matera.to_excel(writer, sheet_name='nao_se_matam_matera', index=False)
            summary_321_grouped.to_excel(writer, sheet_name='summary_321_grouped', index=False)
        return tmp.name

@app.get('/', response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse('index.html', {'request': request})

@app.post('/process')
async def process(matera_files: List[UploadFile] = File(...),
                  dock_files: List[UploadFile] = File(...),
                  depara_file: UploadFile = File(...)):
    output_path = generate_report(matera_files, dock_files, depara_file)
    return FileResponse(output_path, filename='resultado.xlsx')
