"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel
import os,glob
import pandas as pd
import zipfile

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    def cargar_datos(carpeta_entrada):
        def descomprimir_archivos(carpeta_entrada):
            archivos_zip = glob.glob(os.path.join(carpeta_entrada, "*.zip"))
            for archivo_zip in archivos_zip:
                with zipfile.ZipFile(archivo_zip, "r") as zip_ref:
                    for archivo_info in zip_ref.infolist():
                        with zip_ref.open(archivo_info) as archivo:
                            yield pd.read_csv(archivo)

        dataframes = [df for df in descomprimir_archivos(carpeta_entrada)]
        return pd.concat(dataframes, ignore_index=True)
    
    def procesar_clientes(df):
        columnas = [
            "client_id",
            "age",
            "job",
            "marital",
            "education",
            "credit_default",
            "mortgage",
        ]
        clientes = df[columnas].copy()
        clientes["job"] = clientes["job"].str.replace(".", "", regex=False).str.replace("-", "_", regex=False)
        clientes["education"] = clientes["education"].str.replace(".", "_", regex=False).replace("unknown", pd.NA)
        clientes["credit_default"] = clientes["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
        clientes["mortgage"] = clientes["mortgage"].apply(lambda x: 1 if x == "yes" else 0)
        return clientes
    
    def procesar_campañas(df):
        columnas = [
            "client_id",
            "number_contacts",
            "contact_duration",
            "previous_campaign_contacts",
            "previous_outcome",
            "campaign_outcome",
            "last_contact_date",
        ]
        campañas = df.copy()
        meses = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,"jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}
        campañas["month"] = campañas["month"].str.lower().map(meses)
        campañas["last_contact_date"] = pd.to_datetime(
            "2022-" + campañas["month"].astype(str).str.zfill(2) + "-" + campañas["day"].astype(str).str.zfill(2),
            format="%Y-%m-%d"
        )
        campañas["previous_outcome"] = campañas["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
        campañas["campaign_outcome"] = campañas["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)
        return campañas[columnas]

    def procesar_economia(df):
        columnas = ["client_id", "cons_price_idx", "euribor_three_months"]
        economia = df[columnas].copy()
        return economia

    def guardar_datos(clientes, campañas, economia, carpeta_salida):
        if not os.path.exists(carpeta_salida):
            os.makedirs(carpeta_salida)
        clientes.to_csv(os.path.join(carpeta_salida, "client.csv"), index=False)
        campañas.to_csv(os.path.join(carpeta_salida, "campaign.csv"), index=False)
        economia.to_csv(os.path.join(carpeta_salida, "economics.csv"), index=False)

    # Cargar datos
    df = cargar_datos("files/input")

    # Procesar datos
    clientes = procesar_clientes(df)
    campañas = procesar_campañas(df)
    economia = procesar_economia(df)

    # Guardar datos
    print("Guardando los archivos procesados en la carpeta de salida...")
    guardar_datos(clientes, campañas, economia, "files/output")

clean_campaign_data()