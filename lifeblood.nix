{
  lib,
  buildPythonPackage,
  setuptools,
  aiofiles,
  aiosqlite,
  aiorwlock,
  lz4,
  psutil,
  semantic-version,
  toml,
  watchdog,
}:
buildPythonPackage {
  name = "lifeblood";
  pyproject = true;
  src = ./.;

  patchPhase = ''
    cp ./pkg_lifeblood/{MANIFEST.in,pyproject.toml,setup.cfg} .
  '';

  build-system = [ setuptools ];

  pythonImportsCheck = [ "lifeblood" ];
  propagatedBuildInputs = [
    aiofiles
    aiosqlite
    aiorwlock
    lz4
    psutil
    semantic-version
    toml
    watchdog
  ];

  meta = {
    description = "";
    license = lib.licenses.gpl3;
    mainProgram = "lifeblood";
  };
}
