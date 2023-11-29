
::: {.cell 0=‘h’ 1=‘i’ 2=‘d’ 3=‘e’}

``` python
from humble_database.core import *
```

:::

# humble-database

> A simple interface for managing database connections and queries

This file will become your README and also the index of your
documentation.

## Install

``` sh
pip install humble_database
```

## How to use

::: {.cell 0=‘h’ 1=‘i’ 2=‘d’ 3=‘e’}

``` python
from humble_database.database import Database
from humble_database.utils import delegates
import sqlalchemy
from sqlalchemy import create_engine, URL, Engine
from sqlalchemy.orm import Session
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional,Union
from abc import ABC, abstractproperty,abstractmethod
from contextlib import contextmanager
import inspect
import pandas as pd
from json2html import json2html
from IPython.display import HTML
import logging
```

:::

::: {.cell 0=‘h’ 1=‘i’ 2=‘d’ 3=‘e’}

``` python
from nbdev.showdoc import show_doc
```

:::

## Database Example

``` python
import pandas as pd
import os
from sqlalchemy import text
from urllib.request import urlretrieve
```

``` python
urlretrieve(
    "http://2016.padjo.org/files/data/starterpack/census-acs-1year/acs-1-year-2015.sqlite",
    filename='acs.db'
)
db = Database(drivername='sqlite',database = 'acs.db')
```

``` python
db.query_to_df("select * from sqlite_schema").head(2)
```

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>type</th>
      <th>name</th>
      <th>tbl_name</th>
      <th>rootpage</th>
      <th>sql</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>table</td>
      <td>states</td>
      <td>states</td>
      <td>2</td>
      <td>CREATE TABLE states (\n    year INTEGER , \n  ...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>table</td>
      <td>congressional_districts</td>
      <td>congressional_districts</td>
      <td>3</td>
      <td>CREATE TABLE congressional_districts (\n    ye...</td>
    </tr>
  </tbody>
</table>
</div>

``` python
db.query_to_df("""select * from states limit 5""")
```

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>year</th>
      <th>name</th>
      <th>geo_id</th>
      <th>total_population</th>
      <th>white</th>
      <th>black</th>
      <th>hispanic</th>
      <th>asian</th>
      <th>american_indian</th>
      <th>pacific_islander</th>
      <th>other_race</th>
      <th>median_age</th>
      <th>total_households</th>
      <th>owner_occupied_homes_median_value</th>
      <th>per_capita_income</th>
      <th>median_household_income</th>
      <th>below_poverty_line</th>
      <th>foreign_born_population</th>
      <th>state</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2015</td>
      <td>Alabama</td>
      <td>04000US01</td>
      <td>4858979</td>
      <td>3204076</td>
      <td>1296681</td>
      <td>192870</td>
      <td>58918</td>
      <td>19069</td>
      <td>2566</td>
      <td>5590</td>
      <td>38.7</td>
      <td>1846390</td>
      <td>134100</td>
      <td>44765</td>
      <td>44765</td>
      <td>876016</td>
      <td>169972</td>
      <td>01</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2015</td>
      <td>Alaska</td>
      <td>04000US02</td>
      <td>738432</td>
      <td>452472</td>
      <td>24739</td>
      <td>51825</td>
      <td>45753</td>
      <td>98300</td>
      <td>6341</td>
      <td>2201</td>
      <td>33.3</td>
      <td>250185</td>
      <td>259600</td>
      <td>73355</td>
      <td>73355</td>
      <td>74532</td>
      <td>58544</td>
      <td>02</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2015</td>
      <td>Arizona</td>
      <td>04000US04</td>
      <td>6828065</td>
      <td>3802263</td>
      <td>282718</td>
      <td>2098411</td>
      <td>210922</td>
      <td>276132</td>
      <td>9963</td>
      <td>6951</td>
      <td>37.4</td>
      <td>2463008</td>
      <td>194300</td>
      <td>51492</td>
      <td>51492</td>
      <td>1159043</td>
      <td>914400</td>
      <td>04</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2015</td>
      <td>Arkansas</td>
      <td>04000US05</td>
      <td>2978204</td>
      <td>2174934</td>
      <td>466486</td>
      <td>207743</td>
      <td>41932</td>
      <td>18221</td>
      <td>7551</td>
      <td>3826</td>
      <td>37.9</td>
      <td>1144663</td>
      <td>120700</td>
      <td>41995</td>
      <td>41995</td>
      <td>550508</td>
      <td>142841</td>
      <td>05</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2015</td>
      <td>California</td>
      <td>04000US06</td>
      <td>39144818</td>
      <td>14815122</td>
      <td>2192844</td>
      <td>15184545</td>
      <td>5476958</td>
      <td>135866</td>
      <td>143408</td>
      <td>87813</td>
      <td>36.2</td>
      <td>12896357</td>
      <td>449100</td>
      <td>64500</td>
      <td>64500</td>
      <td>5891678</td>
      <td>10688336</td>
      <td>06</td>
    </tr>
  </tbody>
</table>
</div>

``` python
db.query_to_records(
    "select * from states limit 2",
)[0]
```

    {'year': 2015, 'name': 'Alabama', 'geo_id': '04000US01', 'total_population': 4858979, 'white': 3204076, 'black': 1296681, 'hispanic': 192870, 'asian': 58918, 'american_indian': 19069, 'pacific_islander': 2566, 'other_race': 5590, 'median_age': 38.7, 'total_households': 1846390, 'owner_occupied_homes_median_value': 134100, 'per_capita_income': 44765, 'median_household_income': 44765, 'below_poverty_line': 876016, 'foreign_born_population': 169972, 'state': '01'}

## ORM Example

### SQL Alchemy Models

``` python
from pydantic import BaseModel,computed_field,field_validator,ConfigDict,Field
from sqlalchemy import ForeignKey
from sqlalchemy.orm import DeclarativeBase,Mapped, mapped_column,relationship
from typing import List
```

``` python

class Base(DeclarativeBase):
    year:  Mapped[int]
    name: Mapped[str]
    geo_id: Mapped[str]
    total_population: Mapped[int]
    white: Mapped[int]
    black: Mapped[int]
    hispanic: Mapped[int]
    asian: Mapped[int]
    american_indian: Mapped[int]
    pacific_islander: Mapped[int]
    other_race: Mapped[int]
    median_age: Mapped[int]
    total_households: Mapped[int]
    owner_occupied_homes_median_value: Mapped[int]
    per_capita_income: Mapped[int]
    median_household_income: Mapped[int]
    below_poverty_line: Mapped[int]
    foreign_born_population: Mapped[int]

class State(Base):
    __tablename__ = 'states'
    state: Mapped[str] = mapped_column(primary_key=True)
    total_population: Mapped[int]

    places: Mapped[List['Place']] = relationship(back_populates='state_')
    congressional_districts: Mapped[List['CongressionalDistrict']] = relationship(back_populates='state_')

class Place(Base):
    __tablename__ = 'places'
    place: Mapped[str] = mapped_column(primary_key=True)
    total_population: Mapped[int]
    state: Mapped[str] = mapped_column(ForeignKey("states.state"))
    
    state_: Mapped['State'] = relationship(back_populates='places')

class CongressionalDistrict(Base):
    __tablename__ = 'congressional_districts'
    
    congressional_district: Mapped[str] = mapped_column(primary_key=True)
    state: Mapped[str] = mapped_column(ForeignKey("states.state"))
    
    state_: Mapped['State'] = relationship(back_populates='congressional_districts')
```

``` python
with db.session_scope() as session:
    s = session.query(State).first()
    print(s,'\n')
    for place in s.places:
        print(place.name,'::',place.median_household_income)
```

    <__main__.State object> 

    Birmingham city, Alabama :: 32378
    Dothan city, Alabama :: 44208
    Hoover city, Alabama :: 77365
    Huntsville city, Alabama :: 46769
    Mobile city, Alabama :: 38678
    Montgomery city, Alabama :: 41836
    Tuscaloosa city, Alabama :: 44125

``` python
with db.session_scope() as session:
    result = session.query(State).limit(7).all()
    for state in result:
        print(
            state.name,
            len(state.places),
            len(state.congressional_districts)
        )
```

    Alabama 7 7
    Alaska 1 1
    Arizona 16 9
    Arkansas 6 4
    California 137 53
    Colorado 16 7
    Connecticut 8 5

## Pydantic Models

``` python
class ACSBase(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    year: int = Field()
    name: str = Field()
    geo_id: str = Field()
    total_population: Optional[int] = Field(None)
    white: Optional[int] = Field(None)
    black: Optional[int] = Field(None)
    hispanic: Optional[int] = Field(None)
    asian: Optional[int] = Field(None)
    american_indian: Optional[int] = Field(None)
    pacific_islander: Optional[int] = Field(None)
    other_race: Optional[int] = Field(None)
    median_age: float = Field()
    total_households: Optional[int] = Field(None)
    owner_occupied_homes_median_value: int = Field()
    per_capita_income: int = Field()
    median_household_income: int = Field()
    below_poverty_line: Optional[int] = Field(None)
    foreign_born_population: Optional[int] = Field(None)    
    state: int = Field()

class PlaceModel(ACSBase):
    """A Model for a record from the 'places' table"""
    place: str

class CDModel(ACSBase):
    """A Model for a record from the 'congressional_districts' table"""
    congressional_district: str 

class StateModel(ACSBase):
    """A Model for a record from the 'states' table"""

    places: List[PlaceModel]
    congressional_districts: List[CDModel]
    
    @computed_field(return_type=float,title='People per District',)
    def avg_people_per_cd(self) -> float:
        return sum([cd.total_population for cd in self.congressional_districts]) / len(self.congressional_districts)
```

``` python
from IPython.display import JSON
```

``` python
# mode = serialization includes computed fields
JSON(StateModel.model_json_schema(mode='serialization'))
```

    <IPython.core.display.JSON object>

``` python
from humble_database.data_model import DataModel
```

``` python
ACSDataModel = DataModel[StateModel]
JSON(ACSDataModel.model_json_schema(mode='serialization'))
```

    <IPython.core.display.JSON object>

``` python
with db.session_scope() as session:
    orm_result = session.query(State).all()
    result = ACSDataModel(data=orm_result)

result
```

<header><b>title</b>: DataModel[StateModel]
</header><header><b>description</b>: None
</header><header><b>DataFrame</b>: </header><div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>year</th>
      <th>name</th>
      <th>geo_id</th>
      <th>total_population</th>
      <th>white</th>
      <th>black</th>
      <th>hispanic</th>
      <th>asian</th>
      <th>american_indian</th>
      <th>pacific_islander</th>
      <th>...</th>
      <th>total_households</th>
      <th>owner_occupied_homes_median_value</th>
      <th>per_capita_income</th>
      <th>median_household_income</th>
      <th>below_poverty_line</th>
      <th>foreign_born_population</th>
      <th>state</th>
      <th>places</th>
      <th>congressional_districts</th>
      <th>avg_people_per_cd</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2015</td>
      <td>Alabama</td>
      <td>04000US01</td>
      <td>4858979</td>
      <td>3204076</td>
      <td>1296681</td>
      <td>192870</td>
      <td>58918</td>
      <td>19069</td>
      <td>2566</td>
      <td>...</td>
      <td>1846390</td>
      <td>134100</td>
      <td>44765</td>
      <td>44765</td>
      <td>876016.0</td>
      <td>169972.0</td>
      <td>1</td>
      <td>[{'year': 2015, 'name': 'Birmingham city, Alab...</td>
      <td>[{'year': 2015, 'name': 'Congressional Distric...</td>
      <td>694139.857143</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2015</td>
      <td>Alaska</td>
      <td>04000US02</td>
      <td>738432</td>
      <td>452472</td>
      <td>24739</td>
      <td>51825</td>
      <td>45753</td>
      <td>98300</td>
      <td>6341</td>
      <td>...</td>
      <td>250185</td>
      <td>259600</td>
      <td>73355</td>
      <td>73355</td>
      <td>74532.0</td>
      <td>58544.0</td>
      <td>2</td>
      <td>[{'year': 2015, 'name': 'Anchorage municipalit...</td>
      <td>[{'year': 2015, 'name': 'Congressional Distric...</td>
      <td>738432.000000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2015</td>
      <td>Arizona</td>
      <td>04000US04</td>
      <td>6828065</td>
      <td>3802263</td>
      <td>282718</td>
      <td>2098411</td>
      <td>210922</td>
      <td>276132</td>
      <td>9963</td>
      <td>...</td>
      <td>2463008</td>
      <td>194300</td>
      <td>51492</td>
      <td>51492</td>
      <td>1159043.0</td>
      <td>914400.0</td>
      <td>4</td>
      <td>[{'year': 2015, 'name': 'Avondale city, Arizon...</td>
      <td>[{'year': 2015, 'name': 'Congressional Distric...</td>
      <td>711564.777778</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2015</td>
      <td>Arkansas</td>
      <td>04000US05</td>
      <td>2978204</td>
      <td>2174934</td>
      <td>466486</td>
      <td>207743</td>
      <td>41932</td>
      <td>18221</td>
      <td>7551</td>
      <td>...</td>
      <td>1144663</td>
      <td>120700</td>
      <td>41995</td>
      <td>41995</td>
      <td>550508.0</td>
      <td>142841.0</td>
      <td>5</td>
      <td>[{'year': 2015, 'name': 'Fayetteville city, Ar...</td>
      <td>[{'year': 2015, 'name': 'Congressional Distric...</td>
      <td>695398.750000</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2015</td>
      <td>California</td>
      <td>04000US06</td>
      <td>39144818</td>
      <td>14815122</td>
      <td>2192844</td>
      <td>15184545</td>
      <td>5476958</td>
      <td>135866</td>
      <td>143408</td>
      <td>...</td>
      <td>12896357</td>
      <td>449100</td>
      <td>64500</td>
      <td>64500</td>
      <td>5891678.0</td>
      <td>10688336.0</td>
      <td>6</td>
      <td>[{'year': 2015, 'name': 'Alameda city, Califor...</td>
      <td>[{'year': 2015, 'name': 'Congressional Distric...</td>
      <td>735426.811321</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 22 columns</p>
</div>