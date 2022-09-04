#!/usr/bin/env bash
../venv/bin/python -m twine upload --repository testpypi -u __token__ -p $(<../$1) dist/*