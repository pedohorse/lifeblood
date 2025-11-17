{
  lib,
  python311Packages,
}:
python311Packages.buildPythonPackage {
  name = "lifeblood";
  pyproject = true;
  src = ./pkg_lifeblood;
  build-system = with python311Packages; [ setuptools ];

  pythonImportChecks = [ "lifeblood" ];
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
