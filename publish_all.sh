#!/usr/bin/env bash

cd pkg_lifeblood
./build_pypi.sh && ./upload_pypi.sh $1 $2
pubd1=$?
tmp=$(ls dist/lifeblood-*.tar.gz)
tmp=${tmp%.tar.gz}
ver1=${tmp##*-}
cd ..

cd pkg_lifeblood_viewer
./build_pypi.sh && ./upload_pypi.sh $1 $2
pubd2=$?
tmp=$(ls dist/lifeblood-*.tar.gz)
tmp=${tmp%.tar.gz}
ver2=${tmp##*-}
cd ..

if [ $((pubd2 && pubd1)) -eq 1 ]; then
  echo "nothing was published"
  exit 1
fi

maj1=${ver1%%.*}
tmp=${ver1%.*}
min1=${tmp#*.}
pch1=${ver1##*.}
maj2=${ver2%%.*}
tmp=${ver2%.*}
min2=${tmp#*.}
pch2=${ver2##*.}

tag_version=$(( maj1 > maj2 ? maj1 : maj2 )).$(( min1+min2 )).$(( pch1+pch2))
echo creating tag for version: $tag_version

git tag -a "release/v${tag_version}" -m "auto tag for pypi release:\nlifeblood: ${ver1}\nlifeblood-viewer: ${ver2}"

git push origin "release/v${tag_version}"