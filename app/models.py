from sqlalchemy import (
    Column,
    String,
    Integer,
    Float,
    Boolean,
    Date,
    DateTime,
    Text,
    UniqueConstraint,
    ForeignKey,
    Numeric,
    BigInteger

)

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from app.database import Base


# ============================================================
# MOTORN﻿ET — NUOVO
# ============================================================

class MnetMarche(Base):
    __tablename__ = "mnet_marche"

    acronimo = Column(String, primary_key=True)
    nome = Column(String, nullable=False)
    logo = Column(Text, nullable=False)
    utile = Column(Boolean, nullable=False, default=True)


class MnetModelli(Base):
    __tablename__ = "mnet_modelli"

    codice_modello = Column(String, primary_key=True)
    marca_acronimo = Column(String, nullable=False, index=True)

    descrizione = Column(String, nullable=False)

    inizio_produzione = Column(Date)
    fine_produzione = Column(Date)
    inizio_commercializzazione = Column(Date)
    fine_commercializzazione = Column(Date)

    gruppo_storico_codice = Column(String)
    gruppo_storico_descrizione = Column(String)
    serie_gamma_codice = Column(String)
    serie_gamma_descrizione = Column(String)

    foto = Column(String)
    default_img = Column(String(1000))

    ultima_modifica = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class MnetAllestimenti(Base):
    __tablename__ = "mnet_allestimenti"

    codice_motornet_uni = Column(String, primary_key=True)
    codice_modello = Column(String, nullable=False, index=True)

    nome = Column(String, nullable=False)

    data_da = Column(Date)
    data_a = Column(Date)

    ultima_modifica = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class MnetDettagli(Base):
    __tablename__ = "mnet_dettagli"

    codice_motornet_uni = Column(String, primary_key=True)

    alimentazione = Column(Text)
    cilindrata = Column(Integer)
    hp = Column(Integer)
    kw = Column(Integer)

    euro = Column(Text)
    trazione = Column(Text)
    tipo_cambio = Column(Text)

    prezzo_listino = Column(Float)
    prezzo_accessori = Column(Float)
    data_listino = Column(Date)

    foto = Column(Text)

    ultima_modifica = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


# ============================================================
# MOTORN﻿ET — USATO
# ============================================================

class MnetMarcaUsato(Base):
    __tablename__ = "mnet_marche_usato"

    acronimo = Column(String, primary_key=True)
    nome = Column(String, nullable=False)
    logo = Column(String)


class MnetAnniUsato(Base):
    __tablename__ = "mnet_anni_usato"

    id = Column(Integer, primary_key=True, autoincrement=True)

    marca_acronimo = Column(String(10), nullable=False, index=True)
    anno = Column(Integer, nullable=False, index=True)
    mese = Column(Integer, nullable=False, index=True)

    created_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
    )

    __table_args__ = (
        UniqueConstraint(
            "marca_acronimo",
            "anno",
            "mese",
            name="uq_marca_anno_mese",
        ),
    )


class MnetModelloUsato(Base):
    __tablename__ = "mnet_modelli_usato"

    marca_acronimo = Column(String, primary_key=True)
    codice_desc_modello = Column(String, primary_key=True)

    codice_modello = Column(String)
    descrizione = Column(String)
    descrizione_dettagliata = Column(Text)

    gruppo_storico = Column(String)
    serie_gamma = Column(String)

    inizio_produzione = Column(Date)
    fine_produzione = Column(Date)
    inizio_commercializzazione = Column(Date)
    fine_commercializzazione = Column(Date)

    segmento = Column(String)
    tipo = Column(String)

    created_at = Column(Date)


class MnetAllestimentoUsato(Base):
    __tablename__ = "mnet_allestimenti_usato"

    codice_motornet_uni = Column(String, primary_key=True)

    codice_modello = Column(String, nullable=False, index=True)
    acronimo_marca = Column(String, nullable=False)

    codice_eurotax = Column(String)
    versione = Column(String)

    inizio_produzione = Column(Date)
    fine_produzione = Column(Date)
    inizio_commercializzazione = Column(Date)
    fine_commercializzazione = Column(Date)

    alimentazione = Column(String)
    cambio = Column(String)
    trazione = Column(String)

    cilindrata = Column(Integer)
    kw = Column(Integer)
    cv = Column(Integer)

    created_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
    )


class MnetDettaglioUsato(Base):
    __tablename__ = "mnet_dettagli_usato"

    codice_motornet_uni = Column(String, primary_key=True)

    modello = Column(String)
    allestimento = Column(String)
    immagine = Column(String)

    codice_costruttore = Column(String)
    descrizione_breve = Column(String)

    prezzo_listino = Column(Float)
    prezzo_accessori = Column(Float)
    data_listino = Column(Date)

    marca_nome = Column(String)
    marca_acronimo = Column(String)

    segmento = Column(String)
    tipo = Column(String)

    alimentazione = Column(String)
    cilindrata = Column(Integer)
    hp = Column(Integer)
    kw = Column(Integer)

    emissioni_co2 = Column(Float)
    consumo_medio = Column(Float)

    cambio = Column(String)
    trazione = Column(String)

    lunghezza = Column(Integer)
    larghezza = Column(Integer)
    altezza = Column(Integer)

    porte = Column(Integer)
    posti = Column(Integer)

    peso = Column(Integer)

    paese_prod = Column(String)
    neo_patentati = Column(Boolean)

# ============================================================
# VIC USATO (vcom)
# ============================================================

class MnetVcomMarche(Base):
    __tablename__ = "mnet_vcom_marche"

    acronimo = Column(Text, primary_key=True)
    nome = Column(Text, nullable=False)
    logo = Column(Text)

    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class MnetVcomModelli(Base):
    __tablename__ = "mnet_vcom_modelli"

    codice_modello = Column(Text, primary_key=True)     # es: CIT0957
    marca_acronimo = Column(Text, nullable=False, index=True)
    descrizione = Column(Text, nullable=False)

    gruppo_storico_codice = Column(Text)
    gruppo_storico_descrizione = Column(Text)

    serie_gamma_codice = Column(Text)
    serie_gamma_descrizione = Column(Text)

    inizio_produzione = Column(Date)
    fine_produzione = Column(Date)

    inizio_commercializzazione = Column(Date)
    fine_commercializzazione = Column(Date)

    modello_breve_carrozzeria = Column(Text)
    foto = Column(Text)
    prezzo_minimo = Column(Numeric)

    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class MnetVcomVersioni(Base):
    __tablename__ = "mnet_vcom_versioni"

    codice_motornet_uni = Column(Text, primary_key=True)  # es: C000799
    codice_modello = Column(
        Text,
        ForeignKey("mnet_vcom_modelli.codice_modello", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    nome = Column(Text, nullable=False)

    data_da = Column(Date)
    data_a = Column(Date)

    inizio_produzione = Column(Date)
    fine_produzione = Column(Date)

    marca_acronimo = Column(Text)

    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class MnetVcomDettagli(Base):
    __tablename__ = "mnet_vcom_dettagli"

    codice_motornet_uni = Column(
        Text,
        ForeignKey("mnet_vcom_versioni.codice_motornet_uni", ondelete="CASCADE"),
        primary_key=True,
    )

    # --- Marca / modello ---
    marca_acronimo = Column(Text)
    marca_nome = Column(Text)

    codice_modello = Column(Text)
    descrizione_modello = Column(Text)

    allestimento = Column(Text)
    immagine = Column(Text)

    codice_costruttore = Column(Text)
    codice_motore = Column(Text)

    # --- Alimentazione / tipo ---
    alimentazione_codice = Column(Text)
    alimentazione_descrizione = Column(Text)

    tipo_codice = Column(Text)
    tipo_descrizione = Column(Text)

    categoria_codice = Column(Text)
    categoria_descrizione = Column(Text)

    # --- Motore / prestazioni ---
    cilindrata = Column(Integer)
    hp = Column(Integer)
    kw = Column(Integer)
    euro = Column(Text)

    # --- Prezzi ---
    prezzo_listino = Column(Numeric)
    prezzo_accessori = Column(Numeric)
    data_listino = Column(Date)

    # --- Trasmissione ---
    cambio_codice = Column(Text)
    cambio_descrizione = Column(Text)

    trazione_codice = Column(Text)
    trazione_descrizione = Column(Text)

    # --- Dimensioni ---
    lunghezza = Column(Numeric)
    larghezza = Column(Numeric)
    altezza = Column(Numeric)
    passo = Column(Numeric)

    porte = Column(Integer)
    posti = Column(Integer)

    # --- Elettrico / autonomia ---
    autonomia_media = Column(Numeric)
    autonomia_massima = Column(Numeric)

    # --- Pesi ---
    peso = Column(Numeric)
    peso_vuoto = Column(Numeric)
    peso_totale_terra = Column(Numeric)

    portata = Column(Numeric)

    accessi_disponibili = Column(BigInteger)

    accessori_serie = Column(JSONB)
    accessori_opzionali = Column(JSONB)

    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class MnetVcomSyncState(Base):
    __tablename__ = "mnet_vcom_sync_state"

    job_name = Column(Text, primary_key=True)
    last_key = Column(Text)
    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    
class MnetVcomSyncError(Base):
    __tablename__ = "mnet_vcom_sync_errors"

    job_name = Column(Text, nullable=False)
    key = Column(Text, nullable=False)
    error = Column(Text, nullable=False)

    created_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
    )


class MnetVcomSyncState(Base):
    __tablename__ = "mnet_vcom_sync_state"

    job_name = Column(Text, primary_key=True)
    last_key = Column(Text)

    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

class MnetVcomSyncError(Base):
    __tablename__ = "mnet_vcom_sync_errors"

    job_name = Column(Text, nullable=False)
    key = Column(Text, nullable=False)
    error = Column(Text, nullable=False)

    created_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
    )