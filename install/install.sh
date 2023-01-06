#!/usr/bin/env bash

# put this script into an empty folder where you want lifeblood to be installed
# a subfolder with commit hash will be created here containing that specific lifeblood version
# REQUIREMENTS: python3 (with venv), wget, unzip

branch=dev

set -e
install_viewer=true

for arg in "$@"; do
  if [ $arg == "--no-viewer" ]; then
    install_viewer=false
  fi
  if [ $arg == "--help" -o $arg == "-h" ]; then
    echo "`basename $0` usage:"
    echo "    --no-viewer    do NOT install lifeblood_viewer. SHOULD be used for headless machines!"
    echo "    -h / --help      shot this message"
    exit 0
  fi
done

if [ -x $(which python3) ]; then
  PYTHON=$(which python3)
elif [ -x $(which python) ]; then
  PYTHON=$(which python)
else
  echo "python executable not found in PATH. checked for python3, python" > 2
  exit 1
fi
echo "python found at $PYTHON"

if $PYTHON -c "import venv"; then
  echo "venv is available"
else
  echo "venv not found! install it, for example with pip install venv"
  exit 1
fi

if [ ! -x $(which wget) ]; then
  echo "wget not found in PATH" > 2
  exit 1
fi

if [ ! -x $(which unzip) ]; then
  echo "unzip not found in PATH" > 2
  exit 1
fi

echo "----------------------------------------"
echo "ALL PRE FLIGHT CHECKS PASSED, PROCEEDING"
echo "----------------------------------------"
echo "downloading latest $branch"
wget https://github.com/pedohorse/lifeblood/archive/refs/heads/${branch}.zip
echo "downloading completed"
archname=${branch}.zip
if [ ! -e $archname ]; then
  echo "downloaded archive $archname not found!"
  exit 1
fi

hash=$(unzip -qz $archname)
if [ -z "hash" ]; then
  echo "cannot determine hash ($hash) , is this script outdated?"
  exit 1
fi
hash=${hash::13}
echo "latest version hash is $hash"

echo "unzipping..."
unzip -q $archname  # github creates folder named repo-branch inside the arch
echo "unzip done"

echo "sort of installing..."
mkdir $hash
mv lifeblood-${branch}/src/lifeblood $hash/
mv lifeblood-${branch}/src/lifeblood_viewer $hash/
mv lifeblood-${branch}/entry.py $hash/.

# THIS IS FOR TESTING ONLY !!!             !!!!
cp requirements.txt $hash/.
cp requirements_viewer.txt $hash/.

pushd $hash
echo "initializing venv"
$PYTHON -m venv venv
echo "activating venv"
source venv/bin/activate

echo "installing dependencies"
pip install -r requirements.txt
if $install_viewer; then
  pip install -r requirements_viewer.txt
fi

echo "deactivating venv"
deactivate

popd

echo "making links"
if [ -e current ]; then
  rm current
fi
ln -s $hash current

echo "#!/bin/sh" > lifeblood
echo "exec current/entry.py "\"\$@\" > lifeblood
chmod +x lifeblood

if $install_viewer; then
  echo "#!/bin/sh" > lifeblood_viewer
  echo "exec current/entry.py viewer "\"\$@\" > lifeblood_viewer
  chmod +x lifeblood_viewer
fi

echo "cleaning up..."
rm -rf lifeblood-${branch}
rm $archname

echo "done"
