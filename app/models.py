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
    BigInteger,
    TIMESTAMP,
    CheckConstraint,


)

from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.sql import func
from app.database import Base


# ============================================================
# MOTORN﻿ET — NUOVO
# ============================================================

class MnetMarche(Base):
    __tablename__ = "mnet_marche"
    __table_args__ = {"schema": "public"}


    acronimo = Column(String, primary_key=True)
    nome = Column(String, nullable=False)
    logo = Column(Text, nullable=False)
    utile = Column(Boolean, nullable=False, default=True)


class MnetModelli(Base):
    __tablename__ = "mnet_modelli"
    __table_args__ = {"schema": "public"}


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
    __table_args__ = {"schema": "public"}


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
    __table_args__ = {"schema": "public"}

    codice_motornet_uni = Column(String, primary_key=True)

    alimentazione = Column(Text)
    cilindrata = Column(Integer)
    hp = Column(Integer)
    kw = Column(Integer)
    euro = Column(Text)

    consumo_medio = Column(Float)
    consumo_urbano = Column(Float)
    consumo_extraurbano = Column(Float)
    emissioni_co2 = Column(Text)

    tipo_cambio = Column(Text)
    trazione = Column(Text)
    porte = Column(Integer)
    posti = Column(Integer)
    lunghezza = Column(Integer)
    larghezza = Column(Integer)
    altezza = Column(Integer)
    altezza_minima = Column(Integer)

    peso = Column(Integer)
    peso_vuoto = Column(Text)
    peso_potenza = Column(Text)
    portata = Column(Integer)

    velocita = Column(Integer)
    accelerazione = Column(Float)

    bagagliaio = Column(Text)
    descrizione_breve = Column(Text)
    foto = Column(Text)
    prezzo_listino = Column(Float)
    prezzo_accessori = Column(Float)
    data_listino = Column(Date)

    neo_patentati = Column(Boolean)
    architettura = Column(Text)
    coppia = Column(Text)
    coppia_ibrido = Column(Text)
    coppia_totale = Column(Text)

    numero_giri = Column(Integer)
    numero_giri_ibrido = Column(Integer)
    numero_giri_totale = Column(Integer)

    valvole = Column(Integer)
    passo = Column(Integer)

    cilindri = Column(Text)
    cavalli_fiscali = Column(Integer)

    pneumatici_anteriori = Column(Text)
    pneumatici_posteriori = Column(Text)

    massa_p_carico = Column(Text)
    indice_carico = Column(Text)
    codice_velocita = Column(Text)

    cap_serb_litri = Column(Integer)
    cap_serb_kg = Column(Float)

    paese_prod = Column(Text)
    tipo_guida = Column(Text)
    tipo_motore = Column(Text)
    descrizione_motore = Column(Text)

    cambio_descrizione = Column(Text)
    nome_cambio = Column(Text)
    marce = Column(Text)

    codice_costruttore = Column(String)
    modello_breve_carrozzeria = Column(Text)

    tipo = Column(Text)
    tipo_descrizione = Column(Text)
    segmento = Column(Text)
    segmento_descrizione = Column(Text)

    garanzia_km = Column(Integer)
    garanzia_tempo = Column(Integer)
    guado = Column(Integer)
    pendenza_max = Column(Integer)
    sosp_pneum = Column(Boolean)

    tipo_batteria = Column(Text)
    traino = Column(Integer)
    volumi = Column(Text)

    cavalli_ibrido = Column(Integer)
    cavalli_totale = Column(Integer)
    potenza_ibrido = Column(Integer)
    potenza_totale = Column(Integer)

    motore_elettrico = Column(Text)
    motore_ibrido = Column(Text)
    capacita_nominale_batteria = Column(Float)
    capacita_netta_batteria = Column(Float)
    cavalli_elettrico_max = Column(Integer)
    cavalli_elettrico_boost_max = Column(Integer)
    potenza_elettrico_max = Column(Integer)
    potenza_elettrico_boost_max = Column(Integer)

    autonomia_media = Column(Float)
    autonomia_massima = Column(Float)

    equipaggiamento = Column(Text)
    hc = Column(Text)
    nox = Column(Text)
    pm10 = Column(Text)
    wltp = Column(Text)

    ridotte = Column(Boolean)

    freni = Column(Text)

    ultima_modifica = Column(TIMESTAMP, default=func.now(), onupdate=func.now())



# ============================================================
# MOTORN﻿ET — USATO
# ============================================================

class MnetMarcaUsato(Base):
    __tablename__ = "mnet_marche_usato"
    __table_args__ = {"schema": "public"}

    acronimo = Column(String, primary_key=True)
    nome = Column(String, nullable=False)
    logo = Column(String)

class MnetAnniUsato(Base):
    __tablename__ = "mnet_anni_usato"
    __table_args__ = (
        UniqueConstraint("marca_acronimo", "anno", "mese", name="uq_marca_anno_mese"),
        {"schema": "public"},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)

    marca_acronimo = Column(
        String(10),
        ForeignKey("public.mnet_marche_usato.acronimo"),
        nullable=False,
        index=True,
    )

    anno = Column(Integer, nullable=False, index=True)
    mese = Column(Integer, nullable=False, index=True)

    created_at = Column(DateTime, server_default=func.now())


class MnetModelloUsato(Base):
    __tablename__ = "mnet_modelli_usato"
    __table_args__ = {"schema": "public"}

    # 🔑 CHIAVE REALE
    codice_modello = Column(String, primary_key=True)

    # attributi
    marca_acronimo = Column(
        String,
        ForeignKey("public.mnet_marche_usato.acronimo"),
        nullable=False,
        index=True,
    )

    codice_desc_modello = Column(String, nullable=False)
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

    # relazione corretta
    allestimenti = relationship(
        "MnetAllestimentoUsato",
        back_populates="modello",
        cascade="all, delete-orphan",
    )

class MnetAllestimentoUsato(Base):
    __tablename__ = "mnet_allestimenti_usato"
    __table_args__ = {"schema": "public"}

    codice_motornet_uni = Column(String, primary_key=True)

    codice_modello = Column(
        String,
        ForeignKey("public.mnet_modelli_usato.codice_modello", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

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

    created_at = Column(DateTime, server_default=func.now())

    modello = relationship("MnetModelloUsato", back_populates="allestimenti")


class MnetDettaglioUsato(Base):
    __tablename__ = "mnet_dettagli_usato"
    __table_args__ = {"schema": "public"}

    codice_motornet_uni = Column(String, primary_key=True)

    # Identificazione e immagini
    modello = Column(String)
    allestimento = Column(String)
    immagine = Column(String)
    codice_costruttore = Column(String)
    codice_motore = Column(String)
    descrizione_breve = Column(String)

    # Prezzi e data
    prezzo_listino = Column(Float)
    prezzo_accessori = Column(Float)
    data_listino = Column(Date)

    # Marca e gamma
    marca_nome = Column(String)
    marca_acronimo = Column(String)
    gamma_codice = Column(String)
    gamma_descrizione = Column(String)
    gruppo_storico = Column(String)
    serie_gamma = Column(String)
    categoria = Column(String)
    segmento = Column(String)
    tipo = Column(String)

    # Motore
    tipo_motore = Column(String)
    descrizione_motore = Column(String)
    euro = Column(String)
    cilindrata = Column(Integer)
    cavalli_fiscali = Column(Integer)
    hp = Column(Integer)
    kw = Column(Integer)

    # Emissioni e consumi
    emissioni_co2 = Column(Float)
    emissioni_urbe = Column(Float)
    emissioni_extraurb = Column(Float)
    consumo_urbano = Column(Float)
    consumo_extraurbano = Column(Float)
    consumo_medio = Column(Float)

    # Prestazioni
    accelerazione = Column(Float)
    velocita = Column(Integer)
    peso_potenza = Column(String)

    # Cambio e trazione
    descrizione_marce = Column(String)
    cambio = Column(String)
    trazione = Column(String)
    tipo_guida = Column(String)

    # Dimensioni
    passo = Column(Integer)
    lunghezza = Column(Integer)
    larghezza = Column(Integer)
    altezza = Column(Integer)

    # Capacità e spazio
    bagagliaio = Column(String)
    portata = Column(Integer)
    massa_p_carico = Column(String)

    # Abitabilità
    porte = Column(Integer)
    posti = Column(Integer)

    # Motore e struttura
    cilindri = Column(String)
    valvole = Column(Integer)
    coppia = Column(String)
    numero_giri = Column(Integer)
    architettura = Column(String)

    # Pneumatici
    pneumatici_anteriori = Column(String)
    pneumatici_posteriori = Column(String)

    # Peso
    peso = Column(Integer)
    peso_vuoto = Column(String)

    # Elettrico / ibrido / ricarica
    ricarica_standard = Column(Boolean)
    ricarica_veloce = Column(Boolean)
    sospensioni_pneumatiche = Column(Boolean)

    # Altro
    volumi = Column(String)
    neo_patentati = Column(Boolean)
    paese_prod = Column(String)
    ridotte = Column(Boolean)

# ============================================================
# VIC USATO (vcom)
# ============================================================

class MnetVcomMarche(Base):
    __tablename__ = "mnet_vcom_marche"
    __table_args__ = {"schema": "public"}


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
    __table_args__ = {"schema": "public"}


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
    __table_args__ = {"schema": "public"}

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
    __table_args__ = {"schema": "public"}

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



    
class MnetVcomSyncError(Base):
    __tablename__ = "mnet_vcom_sync_errors"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, primary_key=True, autoincrement=True)

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
    __table_args__ = {"schema": "public"}


    job_name = Column(Text, primary_key=True)
    last_key = Column(Text)

    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    
class MnetModelliCdnPreview(Base):
    __tablename__ = "mnet_modelli_cdn_preview"

    id = Column(Integer, primary_key=True)

    codice_modello = Column(
        String,
        ForeignKey("public.mnet_modelli.codice_modello", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )


    # Parametri CDN risolti
    make = Column(String, nullable=False)
    model_family = Column(String, nullable=False)
    model_variant = Column(String, nullable=True)

    # URL CDN Imagin (preview, NON finale)
    url_cdn = Column(Text, nullable=False)

    # Fonte del mapping
    source = Column(
        String,
        nullable=False,
    )

    # Validazione manuale
    is_valid = Column(Boolean, nullable=False, default=False)
    checked_at = Column(DateTime, nullable=True)
    checked_by = Column(String, nullable=True)
    note = Column(Text, nullable=True)

    # Audit
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        CheckConstraint(
            "source IN ('az_image', 'normalized')",
            name="ck_mnet_modelli_cdn_preview_source",
        ),
    )

    # Relazione (solo lettura, utile in debug)
    modello = relationship(
        "MnetModelli",
        backref="cdn_preview",
        lazy="joined",
    )

class AzImage(Base):
    __tablename__ = "az_image"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, primary_key=True, index=True)
    codice_modello = Column(String, unique=True, index=True, nullable=False)
    marca_alias = Column(String, nullable=True)
    modello_alias = Column(String, nullable=True)
    model_variant = Column(String, nullable=True)

class MnetModelliImgOldAI(Base):
    __tablename__ = "mnet_modelli_img_old_ai"

    codice_modello = Column(String(20), primary_key=True)
    job_id = Column(UUID(as_uuid=True), nullable=False)

    status = Column(
        String(20),
        nullable=False,
        default="queued",  # queued | done | failed
    )

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )