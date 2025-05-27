# Mežu cirsmu atvērumu modelēšana

## Par projektu

Koda realizācija veikta LAD inovāciju projekta "Mežizstrādes automatizētas plānošanas algoritmu, datu apstrādes un analīzes IT rīka izstrāde rūpnieciskā pētījuma ietvaros" Nr.23-00-A01612-000004 ietvaros.

Kods ir izstrādāts, lai veiktu cirsmu atvērumu automatizētu modelēšanu, plānošanu un analīzi mežaudzēs. Koda kopums ir bāzēts Python realizācijā ar uzdevumu norādītajām teritorijām veikt modelēšanu un datu analīzi pēc izstrādātās metodikas.

## Funkcionalitāte

Koda bibliotēka norādītajām teritorijām veic:

- **Datu ielāde:** ārējo datu avotu datu ielādes izvēlētajām teritorijām (VMD MVR, LGIA LAS dati, SILAVA DTW, DAP OZOLS, ZMNI Meliorācijas kadastrs, u.c. datu kopas)
- **Datu apstrāde:** lejupielādēto datu apstrādes darbības izvēlētajām teritorijām, atvasinājumu un starprezultātu sagatavošanu
- **Datu integrācija:** atvasinājumu un starprezultātu savstarpēju savietošanu pēc izstrādātās metodikas
- **Cirsmu modelēšana:** potenciāli iespējamo cirsmu atvērumu datu kopas (teselāciju) sagatavošanu un klasificēšanu pēc izstrādes iespējamības, sadalot izstrādi vismaz 3 secīgos izstrādes ciklos

## Repozitorija struktūra

### Galvenās mapes

- **basedata/** - globāli lietotās datu kopas:
  - Norādes datu avotiem uz data.gov.lv API resursiem
  - Norādes datu avotiem uz citiem fiksētiem ārējiem resursiem (OGC WFS, ArcGIS REST)
  - Datu faili kopām, kuras nav publiski pieejamas, bet ir nepieciešamas datu lejupielādēm un apstrādēm

- **scripts/** - programmatūras izpildāmie faili:
  - Datu lejupielādes skripti
  - Datu atvasināšanas un apstrādes darbības
  - Instalācijas un izpildes instrukcijas
  - Konfigurācijas parametru paraugi

- **pilot/** - testa/pilotteritorijas dati:
  - Konfigurācijas parametri
  - Paraugu dati
  - Noklusējuma parametri, ko sistēma izmanto, ja nav norādīti specifiski

### Datu kopas basedata/ mapē

- **baltic_grid** - EPSG:25884 - ETRS89 / TM Baltic93 (arī saistītie DTWxx dati ir šajā sistēmā)
- **tks_*** - EPSG:3059 - LKS92 / Latvia TM
- **sugu_plani** - mape, kura satur sugu plānu datus

## Instalācija un lietošana

Detalizētas instrukcijas par instalācijas gaitu un skriptu izpildi atrodamas [scripts/README.md](scripts/README.md) failā.

### Īsa instrukcija

1. Instalējiet nepieciešamās atkarības (Micromamba vai Miniconda)
2. Izveidojiet Python vidi no environment.yml faila
3. Aktivizējiet vidi: `conda activate mezi`
4. Lejupielādējiet datus, izmantojot kādu no pieejamajām metodēm:
   ```bash
   # Izmantojot GPKG failu
   python -m mezi.download -g "../pilot/teritorija.gpkg##parcels"

   # Izmantojot BBOX (EPSG:3059)
   python -m mezi.download -b "513875,322782,515253,323533"

   # Izmantojot WKT (EPSG:3059)
   python -m mezi.download --wkt "Polygon ((514261.23531709046801552 323513.3520204636733979, ...))"
   ```

5. Lejupielādētie dati būs pieejami `resources/inputdata` mapē

## Konfigurācija

Datu lejupielādei un apstrādei ir iespējams norādīt dažādus konfigurācijas parametrus JSON formātā. Detalizēts parametru apraksts atrodams [scripts/README.md](scripts/README.md) failā.

## Licence un autortiesības

Koda realizācija veikta LAD inovāciju projekta "Mežizstrādes automatizētas plānošanas algoritmu, datu apstrādes un analīzes IT rīka izstrāde rūpnieciskā pētījuma ietvaros" Nr.23-00-A01612-000004 ietvaros.

### Licence

Šis projekts ir licencēts zem [GNU General Public License v3.0](LICENSE.txt) (GPL-3.0).

Licence paredz:
- Brīvību lietot, mainīt un izplatīt programmatūru
- Pienākumu saglabāt atklātā pirmkoda principus: visiem pārveidojumiem un atvasinātajiem darbiem jābūt publicētiem ar to pašu licenci
- Pienākumu norādīt autortiesību informāciju un izmaiņas

Licences pilns teksts atrodams [LICENSE.txt](LICENSE.txt) failā.
