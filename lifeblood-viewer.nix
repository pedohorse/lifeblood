
{
  lib,
  fetchPypi,
  python310Packages,
  python311Packages,
  lifeblood,
}:
let
  imgui = python311Packages.buildPythonPackage {
    name = "imgui";
    pythonImportsCheck = [ "imgui" ];
    src = fetchPypi {
      pname = "imgui";
      version = "2.0.0";
      hash = "sha256-L7247tO429fqmK+eTBxlgrC8TalColjeFjM9jGU9Z+E=";
    };
    nativeBuildInputs = with python311Packages; [
      cython_0
    ];
  };
in python311Packages.buildPythonPackage {
  name = "lifeblood-viewer";
  pyproject = true;
  src = ./.;

  patchPhase = ''
    cp ./pkg_lifeblood_viewer/{pyproject.toml,setup.cfg} .
  '';

  build-system = with python311Packages; [ setuptools ];

  pythonImportsCheck = [ "lifeblood" ];
  propagatedBuildInputs = with python311Packages; [
    glfw
    pyopengl
    pyside6
    lz4
    grandalf
    numpy_1
  ] ++ [
    imgui
    lifeblood
  ];
}
