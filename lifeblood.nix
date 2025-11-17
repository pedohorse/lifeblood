{
  lib,
  python311Packages,
}:
python311Packages.buildPythonPackage {
  name = "lifeblood";
  pyproject = true;
  src = ./.;

  patchPhase = ''
    cp ./pkg_lifeblood/{MANIFEST.in,pyproject.toml,setup.cfg} .
  '';

  build-system = with python311Packages; [ setuptools ];

  pythonImportsCheck = [ "lifeblood" ];
  propagatedBuildInputs = with python311Packages; [
    aiofiles
    aiosqlite
    aiorwlock
    lz4
    psutil
    semantic-version
    toml
    watchdog
  ];
}
