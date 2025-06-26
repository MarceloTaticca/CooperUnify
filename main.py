import os
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO


def read_matera(uploaded_file: st.runtime.uploaded_file_manager.UploadedFile):
    date = uploaded_file.name[:10]
    df = pd.read_csv(uploaded_file, delimiter=';')
    df['nVlrLanc'] = df['nVlrLanc'].str.replace(',', '.', regex=False).astype('float64')
    df['sCpf_Cnpj'] = df['sCpf_Cnpj'].astype(str).str.replace(r'[.\-]', '', regex=True)
    df['nVlrLanc'] = np.where(df['nHistorico'] == 9001, -df['nVlrLanc'], df['nVlrLanc'])
    df['date_doc'] = date
    df.rename(columns={'sCpf_Cnpj': 'CPF'}, inplace=True)
    return df


def read_dock(uploaded_file: st.runtime.uploaded_file_manager.UploadedFile):
    date = uploaded_file.name[:10]
    df = pd.read_excel(uploaded_file)
    start_idx = df[df['Unnamed: 2'].notna()].index[0]
    df = df.iloc[start_idx:].reset_index(drop=True)
    df.columns = df.iloc[0]
    df = df.iloc[1:].reset_index(drop=True)
    df = df.loc[:, df.columns.notna()]
    df['Valor'] = np.where(df['Id Tipo Transacao'].isin([30224, 30350]), -df['Valor'], df['Valor'])
    df['date_doc'] = date
    return df


def load_depara(path: str):
    if not os.path.exists(path):
        return None
    depara = pd.read_excel(path)
    start_index = depara[depara['Unnamed: 2'].notna()].index[0]
    depara = depara.iloc[start_index:].reset_index(drop=True)
    depara.columns = depara.iloc[0]
    depara = depara.iloc[1:].reset_index(drop=True)
    cols_to_drop = depara.columns[depara.columns.isna()]
    depara = depara.drop(columns=cols_to_drop)
    return depara


def process_data(matera: pd.DataFrame, dock: pd.DataFrame):
    depara = load_depara('Relatório Contás e Cartões (de para).xlsm')
    if depara is not None:
        dock = dock.merge(
            depara[['Id Conta', 'CPF', 'Nome', 'Status Conta', 'Data Cadastramento']],
            on='Id Conta',
            how='left'
        )

    matera_sum = matera.groupby('CPF')['nVlrLanc'].sum().reset_index()
    matera_sum.rename(columns={'nVlrLanc': 'sum_nVlrLanc'}, inplace=True)
    merged_sum = dock.groupby('CPF')['Valor'].sum().reset_index()
    merged_sum.rename(columns={'Valor': 'sum_value'}, inplace=True)
    comparison_df = pd.merge(matera_sum, merged_sum, on='CPF', how='outer')
    comparison_df['sum_nVlrLanc'] = comparison_df['sum_nVlrLanc'].fillna(0)
    comparison_df['sum_value'] = comparison_df['sum_value'].fillna(0)
    comparison_df['difference'] = (comparison_df['sum_nVlrLanc'] - comparison_df['sum_value']).round(2)
    comparison_df['CPF_or_CPF'] = comparison_df['CPF']
    comparison_df = comparison_df.drop(columns=['CPF'])
    comparison_df_filtered_321 = comparison_df[comparison_df['difference'] != 0]

    matera_sum2 = (
        matera.groupby(['CPF', 'date_doc'])['nVlrLanc']
        .sum()
        .reset_index()
        .rename(columns={'nVlrLanc': 'sum_nVlrLanc'})
    )
    dock_sum2 = (
        dock.groupby(['CPF', 'date_doc'])['Valor']
        .sum()
        .reset_index()
        .rename(columns={'Valor': 'sum_value'})
    )
    comparison_df2 = pd.merge(matera_sum2, dock_sum2, on=['CPF', 'date_doc'], how='outer')
    comparison_df2[['sum_nVlrLanc', 'sum_value']] = comparison_df2[['sum_nVlrLanc', 'sum_value']].fillna(0)
    comparison_df2['difference'] = (
        comparison_df2['sum_nVlrLanc'] - comparison_df2['sum_value']
    ).round(2)
    comparison_df_filtered_591 = comparison_df2[comparison_df2['difference'] != 0]

    unique_cpf_591 = comparison_df_filtered_591['CPF'].unique()
    unique_cpf_321 = comparison_df_filtered_321['CPF_or_CPF'].unique()
    cpfs_not_in_321 = np.setdiff1d(unique_cpf_591, unique_cpf_321)

    se_matam_dock = dock[dock['CPF'].isin(cpfs_not_in_321)]
    se_matam_matera = matera[matera['CPF'].isin(cpfs_not_in_321)]
    summary_80 = se_matam_dock[['Id Conta', 'CPF', 'Nome']]
    summary_80_grouped = summary_80.groupby('CPF').agg(
        Nome=('Nome', 'first'),
        Id_Contas=('Id Conta', lambda x: list(x.unique()))
    ).reset_index()

    nao_se_matam_dock = dock[dock['CPF'].isin(comparison_df_filtered_321['CPF_or_CPF'].tolist())]
    nao_se_matam_matera = matera[matera['CPF'].isin(comparison_df_filtered_321['CPF_or_CPF'].tolist())]
    summary_321 = nao_se_matam_dock[['Id Conta', 'CPF', 'Nome']]
    summary_321_grouped = summary_321.groupby('CPF').agg(
        Nome=('Nome', 'first'),
        Id_Contas=('Id Conta', lambda x: list(x.unique()))
    ).reset_index()

    return {
        'se_matam_dock': se_matam_dock,
        'se_matam_matera': se_matam_matera,
        'summary_80_grouped': summary_80_grouped,
        'nao_se_matam_dock': nao_se_matam_dock,
        'nao_se_matam_matera': nao_se_matam_matera,
        'summary_321_grouped': summary_321_grouped,
    }


def to_excel(dfs):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        dfs['se_matam_dock'].to_excel(writer, sheet_name='se_matam_dock', index=False)
        dfs['se_matam_matera'].to_excel(writer, sheet_name='se_matam_matera', index=False)
        dfs['summary_80_grouped'].to_excel(writer, sheet_name='summary_80_grouped', index=False)
        dfs['nao_se_matam_dock'].to_excel(writer, sheet_name='nao_se_matam_dock', index=False)
        dfs['nao_se_matam_matera'].to_excel(writer, sheet_name='nao_se_matam_matera', index=False)
        dfs['summary_321_grouped'].to_excel(writer, sheet_name='summary_321_grouped', index=False)
    output.seek(0)
    return output


st.title('CooperCard Micro Serviço')
col1, col2 = st.columns(2)
with col1:
    matera_file = st.file_uploader('Upload Matera.csv', type='csv')
with col2:
    dock_file = st.file_uploader('Upload Dock.xlsx', type=['xlsx', 'xlsm'])

if st.button('Start'):
    if matera_file is None or dock_file is None:
        st.error('Por favor, envie ambos os arquivos.')
    else:
        matera_df = read_matera(matera_file)
        dock_df = read_dock(dock_file)
        result = process_data(matera_df, dock_df)
        excel_bytes = to_excel(result)
        st.download_button('Download Excel', excel_bytes, file_name='resultado.xlsx')
