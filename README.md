
# Pipeline de Datos Meteorológicos – API a Base de Datos (Histórico)

## 📌 Descripción del proyecto
Este proyecto implementa un **pipeline ETL** que consume datos meteorológicos desde una API pública,
los transforma y los almacena en una base de datos relacional, construyendo un **histórico temporal**
para poder realizar análisis de evolución (tendencias, medias, máximos, etc.).

A diferencia del proyecto anterior, este pipeline está diseñado para **cargas incrementales**
y para trabajar con **datos con timestamp**.

---

## 🎯 Objetivo
- Extraer datos meteorológicos de una API pública (sin autenticación)
- Guardar los datos con marca temporal (timestamp)
- Cargar datos de forma incremental (sin borrar histórico)
- Permitir análisis temporal con consultas SQL

---

## 🛠️ Tecnologías previstas
- Python 3
- Requests
- Pandas
- SQLite (fase inicial)
- Git y GitHub

---

## 🔄 Flujo ETL
API REST (meteorología)
↓  
Extracción de datos (JSON)
↓  
Transformación y normalización (pandas)
↓  
Carga incremental en SQLite (histórico)
↓  
Consultas SQL para análisis temporal

---

## 🧱 Diseño de base de datos (propuesto)

### Tabla: `weather_observations`
Datos meteorológicos por ciudad y timestamp.

Campos:
- `id` (PK autoincrement)
- `city` (TEXT, NOT NULL)
- `timestamp_utc` (TEXT, NOT NULL)  ← ISO 8601
- `temperature_c` (REAL)
- `wind_speed_kmh` (REAL)
- `humidity` (REAL) *(si está disponible en la API)*
- `source` (TEXT) *(nombre de la API)*

**Restricción recomendada:**
- UNIQUE(`city`, `timestamp_utc`) → evita duplicados en cargas incrementales

---

## 📊 Consultas SQL previstas
- Última lectura por ciudad
- Media diaria de temperatura por ciudad
- Máximos y mínimos por semana
- Comparativa entre ciudades (ranking por temperatura media)

---

## 📊 Consultas SQL

El proyecto incluye consultas SQL para analizar datos meteorológicos históricos:

- Número de registros por ciudad
- Última lectura disponible por ciudad
- Ranking de ciudades por temperatura en la última lectura

Las consultas completas están disponibles en `sql/consultas.sql`.

---

## 📌 Estado del proyecto
Diseño inicial creado. Próximo paso: implementación de extracción, transformación y carga incremental.