from pandas import DataFrame
import pandas as pd

col_map = {
    "p1":  "id",                         # IDENTIFIKAČNÍ ČÍSLO
    "p2a": "date",                       # date (day, month, year)
    "p2b": "time",                       # time (hour, minute)
    "p4":  "location_admin",             # ÚZEMNÍ MÍSTO (region/district/police unit)
    "p4a": "region",                     # kraj (region)
    "p4b": "district",                   # okres (district)
    "p4c": "police_unit",                # útvar (police unit)

    "p5a": "locality_type",              # LOKALITA NEHODY (in town / outside town)

    "p6":  "accident_type",              # DRUH NEHODY (type of accident)
    "p7":  "collision_type",             # DRUH SRÁŽKY JEDOUCÍCH VOZIDEL (collision with moving vehicles)
    "p8":  "fixed_obstacle_type",        # DRUH PEVNÉ PŘEKÁŽKY
    "p8a": "animal_type",                # DRUH ZVĚŘE / ZVÍŘETE

    "p9":  "accident_character",         # CHARAKTER NEHODY (seriousness: life loss / property only)
    "p10": "responsibility",             # ZAVINĚNÍ NEHODY (who caused it)
    "p11": "alcohol_at_driver",          # ALKOHOL U VINÍKA NEHODY (alcohol presence/levels)
    "p11a":"drugs_at_driver",            # DROGY U VINÍKA NEHODY

    "p12": "main_cause",                 # HLAVNÍ PŘÍČINY NEHODY (coded causes)
    "p13": "consequences_24h",           # NÁSLEDKY NEHODY (state within 24h)
    "p13a":"fatalities",                 # p13a usmrceno osob
    "p13b":"serious_injuries",           # p13b těžce zraněno osob
    "p13c":"minor_injuries",             # p13c lehce zraněno osob

    "p14": "total_material_damage",      # CELKOVÁ HMOTNÁ ŠKODA (CZK)
    "p15": "road_surface_type",          # DRUH POVRCHU VOZOVKY
    "p16": "road_surface_condition",     # STAV POVRCHU VOZOVKY V DOBĚ NEHODY
    "p17": "road_condition",             # STAV KOMUNIKACE
    "p18": "weather_conditions",         # POVĚTRNOSTNÍ PODMÍNKY
    "p19": "visibility",                 # VIDITELNOST
    "p20": "sight_conditions",           # ROZHLEDOVÉ POMĚRY
    "p21": "road_division",              # DĚLENÍ KOMUNIKACE

    "p22": "accident_position_on_road",  # SITUOVÁNÍ NEHODY NA KOMUNIKACI
    "p23": "traffic_control",            # ŘÍZENÍ PROVOZU V DOBĚ NEHODY
    "p24": "local_priority_arrangement", # MÍSTNÍ ÚPRAVA PŘEDNOSTI V JÍZDĚ
    "p27": "specific_places",            # SPECIFICKÁ MÍSTA A OBJEKTY
    "p28": "road_geometry",              # SMĚROVÉ POMĚRY
    "p29": "pedestrian_category",        # KATEGORIE CHODCE
    "p29a":"pedestrian_reflective",      # REFLEXNÍ PRVKY U CHODCE
    "p29b":"pedestrian_on_micromobility",# CHODEC NA OSOBNÍM PŘEPRAVNÍKU

    "p30": "pedestrian_state",           # STAV CHODCE
    "p30a":"pedestrian_alcohol",         # ALKOHOL U CHODCE PŘÍTOMEN
    "p30b":"pedestrian_drug_type",       # DRUH DROGY U CHODCE
    "p31": "pedestrian_behaviour",       # CHOVÁNÍ CHODCE
    "p32": "situation_at_place",         # SITUACE V MÍSTĚ NEHODY
    "p33": "pedestrian_outcome",         # NÁSLEDKY NA ŽIVOTECH A ZDRAVÍ CHODCŮ
    "p33c":"pedestrian_gender",          # pohlaví osoby
    "p33d":"pedestrian_age",             # věk chodce
    "p33e":"pedestrian_nationality",     # státní příslušnost
    "p33f":"first_aid_given",            # poskytnutí první pomoci
    "p33g":"pedestrian_consequences",    # následky (usmrcení/ranění/...)

    "p34": "number_vehicles",            # POČET ZÚČASTNĚNÝCH VOZIDEL
    "p35": "place_of_accident",          # MÍSTO DOPRAVNÍ NEHODY (in/out of intersection, zone etc.)
    "p36": "road_category",              # DRUH POZEMNÍ KOMUNIKACE (motorway, 1st class... )
    "p37": "road_number",                # ČÍSLO POZEMNÍ KOMUNIKACE

    "p39": "crossing_road_type",         # DRUH KŘIŽUJÍCÍ KOMUNIKACE
    "p44": "vehicle_type",               # DRUH VOZIDLA
    "p45a":"vehicle_make",               # VÝROBNÍ ZNAČKA MOTOROVÉHO VOZIDLA (make)
    "p45b":"vehicle_info",               # ÚDAJE O VOZIDLE (engine size, weight, capacity)
    "p45d":"fuel_type",                  # DRUH POHONU / PALIVA
    "p45f":"tire_type",                  # DRUH PNEUMATIK NA VOZIDLE
    "p47": "vehicle_year",               # ROK VÝROBY VOZIDLA (last two digits)
    "p48a":"vehicle_owner_type",         # CHARAKTERISTIKA VOZIDLA (owner / usage)
    "p49": "skid",                       # SMYK (skid occurred yes/no)

    "p50a":"post_accident_vehicle_state",# VOZIDLO PO NEHODĚ (fire, driver fled, ...)
    "p50b":"leakage",                    # ÚNIK PROVOZNÍCH HMOT (leakage of fuel/other)
    "p51": "extrication_method",         # ZPŮSOB VYPROŠTĚNÍ OSOB Z VOZIDLA
    "p52": "vehicle_position",           # SMĚR JÍZDY NEBO POSTAVENÍ VOZIDLA

    "p53": "damage_on_vehicle",          # ŠKODA NA VOZIDLE (amount in CZK)
    "p55a":"driver_category",            # KATEGORIE ŘIDIČE (license group)
    "p57": "driver_condition",           # STAV ŘIDIČE (fatigue, alcohol, illness...)
    "p58": "external_influence_on_driver",# VNĚJŠÍ OVLIVNĚNÍ ŘIDIČE
    "p59": "in_vehicle_consequences",    # NÁSLEDKY VE VOZIDLE
    "p59a":"person_role",                # OZNAČENÍ OSOBY (driver, front passenger, ...)
    "p59b":"person_detail",              # BLIŽŠÍ OZNAČENÍ OSOBY (helmet, seatbelt, child seat, ...)
    "p59c":"person_gender",              # POHLAVÍ OSOBY
    "p59d":"person_age",                 # VĚK OSOBY
    "p59e":"person_nationality",         # STÁTNÍ PŘÍSLUŠNOST
    "p59f":"person_first_aid",           # POSKYTNUTÍ PRVNÍ POMOCI
    "p59g":"person_consequences"         # NÁSLEDKY (usmrcení/ těžké/ lehké/bez zranění)
}

from typing import Callable, Literal
from pathlib import Path
import pandas as pd

#possilbe data resources (police or waze datasets)
type Dataset = Literal["police", "waze"]
#type for preprocessing data
type Preprocessor = Callable[[pd.DataFrame], pd.DataFrame]


#in case of running in online notebook
try:
    BASE_DIR = Path(__file__).resolve().parent
except NameError:
    BASE_DIR = Path.cwd()

DATA_DIR = BASE_DIR / "data"

def load_data(dataset: Dataset) -> DataFrame:
  """
    Load a DataFrame for the specified dataset.

    Parameters
    ----------
    dataset : Dataset
        Dataset identifier ("police" or "waze").

    Returns
    -------
    pandas.DataFrame
        Loaded dataset as a DataFrame.

    Raises
    ------
    ValueError
        If the dataset identifier is unknown.
  """
  match dataset:
    case "police":
      path = DATA_DIR / "brno_nehody.csv"
      df = police_dataset(path)
    case "waze":
      path = DATA_DIR / "brno_alerts.csv"
      df = pd.read_csv(path)
    case _:
      raise ValueError(f"Unknown dataset: {dataset}")
  return df


def police_dataset(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    preprocessors = [
        rename_columns,
        add_locality_label,
        add_accident_type_label,
        add_collision_type_label,
        add_fixed_obstacle_type_label,
        add_accident_character_label,
        add_driver_alcohol_features
    ]

    df = apply_preprocessors(df, preprocessors)
    return df

def apply_preprocessors(
    df: pd.DataFrame,
    preprocessors: list[Preprocessor],
) -> pd.DataFrame:
    for p in preprocessors:
        df = p(df)
    return df

#list of preprocessors
def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns=col_map)

def add_locality_label(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["locality_type_label"] = (
        df["locality_type"]
        .map({1: "in_city", 2: "out_of_city"})
        .astype("category")
    )
    return df

def add_accident_type_label(df: DataFrame) -> DataFrame:

  ACCIDENT_TYPE_MAP = {
    0: "Other",
    1: "Moving vehicle",
    2: "Parked vehicle",
    3: "Fixed object",
    4: "Pedestrian",
    5: "Wild animal",
    6: "Domestic animal",
    7: "Train",
    8: "Tram",
    9: "Crash",
  }

  df = df.copy()
  df["accident_type_label"] = (
      df["accident_type"]
        .map(ACCIDENT_TYPE_MAP)
        .astype("category")
  )
  
  return df

def add_collision_type_label(df: DataFrame) -> DataFrame:
  COLLISION_TYPE_MAP = {
    0: "N/A",
    1: "Frontal",
    2: "Side",
    3: "From side",
    4: "Rear",
  }

  df = df.copy()
  df["collision_type_label"] = (
    df["collision_type"]
      .map(COLLISION_TYPE_MAP)
      .astype("category")
  )
   
  return df

def add_fixed_obstacle_type_label(df: DataFrame) -> DataFrame:
  FIXED_OBSTACLE_TYPE_MAP = {
      0: "N/A",
      1: "Tree",
      2: "Pole/Utility",
      3: "Bollard/Sign Post",
      4: "Guardrail",
      5: "Other Vehicle Related Obstacle",
      6: "Wall/Bridge",
      7: "Railway Crossing Barrier",
      8: "Construction Obstacle",
      9: "Other",
  }

  df = df.copy()
  df["fixed_obstacle_type_label"] = (
      df["fixed_obstacle_type"]
      .map(FIXED_OBSTACLE_TYPE_MAP)
      .astype("category")
  )

  return df

def add_accident_character_label(df: DataFrame) -> DataFrame:
    ACCIDENT_CHARACTER_MAP = {
        1: "Injury/Fatality",
        2: "Material Damage Only",
    }

    df = df.copy()
    df["accident_character_label"] = (
        df["accident_character"]
        .map(ACCIDENT_CHARACTER_MAP)
        .astype("category")
    )

    return df

def add_driver_alcohol_features(df: DataFrame) -> DataFrame:
    ALCOHOL_LEVEL_MAP = {
        0: "not_tested",
        1: "bac_0_00_to_0_24",
        2: "no_alcohol",
        3: "bac_0_24_to_0_50",
        4: "refused_test",
        5: "unknown",
        6: "bac_0_50_to_0_80",
        7: "bac_0_80_to_1_00",
        8: "bac_1_00_to_1_50",
        9: "bac_1_50_plus",
    }

    df = df.copy()
    df["driver_alcohol_level"] = (
        df["alcohol_at_driver"]
        .map(ALCOHOL_LEVEL_MAP)
        .astype("category")
    )

    alcohol_positive_codes = {1, 3, 6, 7, 8, 9}

    df["driver_alcohol_detected"] = pd.NA
    df.loc[df["alcohol_at_driver"] == 2, "driver_alcohol_detected"] = 0
    df.loc[df["alcohol_at_driver"].isin(alcohol_positive_codes), "driver_alcohol_detected"] = 1
    df["driver_alcohol_detected"] = df["driver_alcohol_detected"].astype("Int8")

    return df
