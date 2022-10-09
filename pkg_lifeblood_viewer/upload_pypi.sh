#!/usr/bin/env bash
../venv/bin/python -m twine upload --repository $2 -u __token__ -p $(<../$1) dist/*