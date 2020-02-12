# Scripts de trabajo con demo Murcia

## rm_ent.py

Este script borra todas las entidades de tipo *Streetlight* y *StreetlightControlCabinet* de un subservicio.

## entities.py

Este script lee los datos de farolas y cuadros de mandos exportados de capas GIS, y los aplica a la plantilla (./streetlight.tmpl), generando un fichero de configuración para [urbo-cli](https://github.com/telefonicasc/urbo-cli)

## Instalación

Es necesario python 3.8 o superior y [pipenv](https://pipenv.kennethreitz.org/en/latest/).

```bash
pipenv install
pipenv shell
```

## Ficheros

- streetlight.tmpl: Plantilla del fichero .json que utiliza [urbo-cli](https://github.com/telefonicasc/urbo-cli). Este fichero está en formato [jinja2](https://jinja.palletsprojects.com/).
- puntosluz.csv: Export CSV de los datos de farolas, en el sistema de referencia WGS84, realizado con [QGIS](https://qgis.org/en/site/) como se muestra en la captura (./img/export.png).
- cm.csv: Export CSV de los datos de cuadros de mandos, en el sistema de referencia WGS84, igual que el anterior.
