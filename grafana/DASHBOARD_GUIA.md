# Guía del Dashboard — SCADA Turbina G1

**Para quién es esta guía:** Operadores, supervisores y cualquier persona que necesite monitorear la turbina sin conocimientos técnicos de ingeniería de datos o machine learning.

---

## ¿Qué hace este dashboard?

Muestra en tiempo real el **estado de salud de la Turbina G1**. Combina dos sistemas de detección:

- **Capa 1 — Reglas:** compara temperaturas, vibraciones y presiones contra límites predefinidos.
- **Capa 2 — Inteligencia Artificial:** un modelo de machine learning (Isolation Forest) analiza patrones complejos que las reglas simples no pueden detectar.

---

## FILA SUPERIOR — Estado General

### 🟢🟡🔴 ESTADO TURBINA G1

El indicador más importante del dashboard. Resume todo en una sola palabra:

| Color | Texto | Qué significa |
|-------|-------|---------------|
| 🟢 Verde | **NORMAL** | La turbina opera dentro de parámetros normales |
| 🟡 Naranja | **PRECAUCIÓN** | Alguna variable está elevada. Monitorear de cerca |
| 🔴 Rojo | **CRÍTICO** | Se detectó una anomalía. Revisar inmediatamente |

> **Regla:** pasa a PRECAUCIÓN cuando el Score Global supera el 30%. Pasa a CRÍTICO cuando supera el 60% **o** cuando la IA detecta una anomalía.

---

### 🟢🔴 Detección ML — Capa 2

Resultado del modelo de Inteligencia Artificial (Isolation Forest):

| Color | Texto | Qué significa |
|-------|-------|---------------|
| 🟢 Verde | **NORMAL** | El patrón de operación es normal |
| 🔴 Rojo | **ANOMALÍA** | El modelo detectó un comportamiento inusual |

> La IA analiza 49 variables estadísticas (medias y desviaciones) durante la última hora. Si el patrón no encaja con la operación histórica normal, dispara la alerta.

---

### Score Global — Capa 1 *(medidor semicircular)*

Promedio de los 5 sub-scores de reglas. Va de **0% a 100%**:

- **0–30%** → Verde: operación normal
- **30–60%** → Naranja: alguna variable en zona de alerta
- **60–100%** → Rojo: condición crítica por reglas

---

### IF Score (ML)

Número que produce el modelo de IA. Cuanto **más negativo**, más anómalo:

| Valor | Color | Interpretación |
|-------|-------|----------------|
| Mayor a −0.50 | 🟢 Verde | Completamente normal |
| Entre −0.50 y −0.70 | 🟡 Naranja | Zona de atención |
| Menor a −0.6998 | 🔴 Rojo | **Anomalía detectada** |

> Muestra "sin datos" durante la primera hora de operación tras un reinicio. Esto es normal — el modelo necesita acumular 60 minutos de historia para funcionar.

---

## SEGUNDA FILA — Sensores Clave en Tiempo Real

Valores actuales de los principales sensores. El color indica el estado:
🟢 Normal · 🟡 Precaución · 🔴 Alarma

### Vibración LO y Vibración LA *(mm/s RMS)*

Mide la vibración mecánica del generador en dos puntos:
- **LO** = Lado Opuesto al acople
- **LA** = Lado del Acople

| Rango | Estado |
|-------|--------|
| < 0.45 mm/s | 🟢 Normal |
| 0.45 – 0.60 mm/s | 🟡 Precaución |
| > 0.60 mm/s | 🔴 Alarma |

> Una vibración alta puede indicar desbalance en el rotor, problemas de alineación o desgaste en rodamientos.

---

### ΔT Aire °C *(Diferencia de temperatura del aire de enfriamiento)*

Diferencia entre el aire caliente que sale y el aire frío que entra al generador. Indica el calor que está disipando la máquina.

| Rango | Estado |
|-------|--------|
| < 8 °C | 🟢 Normal |
| 8 – 15 °C | 🟡 Precaución |
| > 15 °C | 🔴 Alarma |

> Un ΔT muy alto sugiere sobrecalentamiento interno o fallo en el sistema de ventilación.

---

### Temp. Devanados Avg °C

Temperatura promedio de los **6 bobinados del estátor** (devanados U1, U2, V1, V2, W1, W2). Es la temperatura eléctrica más crítica del generador.

| Rango | Estado |
|-------|--------|
| < 63 °C | 🟢 Normal |
| 63 – 69 °C | 🟡 Precaución |
| > 69 °C | 🔴 Alarma |

> Temperaturas altas en los devanados aceleran el envejecimiento del aislamiento eléctrico y pueden causar cortocircuitos.

---

### Rodamiento Empuje °C

Temperatura del rodamiento de empuje axial. Es el componente más caliente del sistema de rodamientos.

| Rango | Estado |
|-------|--------|
| < 55 °C | 🟢 Normal |
| 55 – 58 °C | 🟡 Precaución |
| > 58 °C | 🔴 Alarma |

> También sirve como indicador del sistema de lubricación. Una temperatura muy alta puede significar falta de aceite o aceite contaminado.

---

### Presión Turbina *(bar)*

Presión del agua que mueve la turbina. Se muestra como referencia de carga operativa.

> Este sensor no tiene umbral de alarma en el sistema actual — se usa principalmente para contexto operativo.

---

## TERCERA FILA — Sub-scores por Dimensión *(Medidores)*

Seis medidores que muestran el nivel de riesgo en cada área. Todos van de **0% a 100%**:

| Medidor | Qué mide |
|---------|----------|
| **Score Vibración** | Riesgo por vibraciones excesivas |
| **Score Cojinetes** | Riesgo por sobrecalentamiento de rodamientos |
| **Score Devanados** | Riesgo por sobrecalentamiento eléctrico |
| **Score Enfriamiento** | Riesgo por fallo en ventilación |
| **Score Lubricación** | Riesgo por sobrecalentamiento del rodamiento de empuje |
| **Score Global** | Promedio de los 5 anteriores |

**Cómo leer los medidores:**
- La aguja en verde (0–30%) → sin problemas
- La aguja en naranja (30–70%) → condición a vigilar
- La aguja en rojo (70–100%) → acción requerida

> En operación completamente normal, los medidores permanecen en 0% o cerca de 0%. Solo suben cuando los sensores se acercan a los límites.

---

## GRÁFICAS HISTÓRICAS

Las gráficas muestran la **evolución en el tiempo** de cada variable (última hora por defecto).

### Vibraciones (mm/s RMS)
Dos líneas: verde = vib_lo, amarillo = vib_la. Los picos abruptos indican eventos de anomalía.

### Presiones (bar)
Verde = presión turbina, amarillo = presión tubería. Variaciones bruscas pueden indicar golpes de ariete o cambios de carga.

### Cojinetes y Rodamientos (°C)
Cuatro sensores de temperatura. La línea `rod_empuje` suele ser la más alta (~56 °C normal). La línea roja en el gráfico marca el umbral de alarma a 58 °C.

### Devanados Estátor por Fase (°C)
Seis líneas (U1, U2, V1, V2, W1, W2). Deben seguir trayectorias similares. Si una línea se separa mucho del resto, puede indicar un problema en esa fase específica.

### Enfriamiento — Aire Frío / Caliente / ΔT (°C)
Muestra las tres variables de enfriamiento. La línea de ΔT tiene marcado el umbral de 8 °C.

### Sub-scores por Dimensión (0–100%)
Histórico de los 5 scores de reglas. Permite ver cuándo ocurrieron los eventos y qué dimensión los causó.

### IF Score — Capa 2
Histórico del score del modelo de IA. La línea horizontal marca el umbral (−0.6998). Cuando la línea del score cae POR DEBAJO, se activa la alerta de anomalía.

### IF Anomalías Detectadas
Barras rojas = momentos en que el modelo de IA confirmó una anomalía. Cada barra representa una muestra de 10 segundos con anomalía detectada.

---

## Preguntas frecuentes

**¿Con qué frecuencia se actualiza?**
Cada 5 segundos automáticamente.

**¿Qué hago si veo CRÍTICO?**
1. Identifica qué sub-score está alto (fila de medidores)
2. Revisa la gráfica correspondiente para ver si es un pico puntual o una tendencia
3. Si persiste más de 2–3 minutos, notificar al equipo de mantenimiento

**¿Por qué el IF Score no aparece?**
Tras un reinicio del sistema, el modelo necesita acumular **60 minutos** de datos antes de poder emitir predicciones. Durante ese tiempo el panel muestra "sin datos" — esto es normal.

**¿El sistema puede fallar?**
La Capa 1 (reglas) funciona desde el primer dato. La Capa 2 (IA) requiere 60 min de historia. Si ambas capas están en NORMAL, la turbina opera correctamente.

---

*Dashboard desarrollado como parte del Proyecto de Maestría — Sistema SCADA con detección de anomalías en tiempo real.*
