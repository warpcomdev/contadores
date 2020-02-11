# streetlight

Este script lee el fichero **exportado.csv**, con un export de los atributos de las farolas de la zona de interés, y lo aplica a la plantilla (./streetlight.tmpl), generando un fichero de configuración para [urbo-cli](https://github.com/telefonicasc/urbo-cli)

## Instalación

Es necesario python 3.8 o superior y [pipenv](https://pipenv.kennethreitz.org/en/latest/).

```bash
pipenv install
pipenv shell
python entities.py
```

## Ficheros

- streetlight.tmpl: Plantilla del fichero .json que utiliza [urbo-cli](https://github.com/telefonicasc/urbo-cli). Este fichero está en formato [jinja2](https://jinja.palletsprojects.com/).
- exportado.csv: Export CSV de los datos de farolas, en el sistema de referencia WGS84, realizado con [QGIS](https://qgis.org/en/site/) como se muestra en la captura (./img/export.png).
