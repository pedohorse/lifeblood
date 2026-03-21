{
  lib,
  fetchPypi,
  buildPythonPackage,
  buildPythonApplication,
  lifeblood,
  setuptools,
  glfw,
  pyopengl,
  click,
  cython_0,
  pyside6,
  lz4,
  grandalf,
  numpy_1,
  qt6,
}:
let
  imgui = buildPythonPackage {
    name = "imgui";
    pythonImportsCheck = [ "imgui" ];
    pyproject = true;
    build-system = [ setuptools ];
    src = fetchPypi {
      pname = "imgui";
      version = "2.0.0";
      hash = "sha256-L7247tO429fqmK+eTBxlgrC8TalColjeFjM9jGU9Z+E=";
    };
    dependencies = [
      glfw
      pyopengl
      click
    ];
    nativeBuildInputs = [
      cython_0
    ];
  };

in buildPythonApplication {
  name = "lifeblood-viewer";
  pyproject = true;
  src = ./.;

  patchPhase = ''
    cp ./pkg_lifeblood_viewer/{pyproject.toml,setup.cfg} .
  '';

  build-system = [ setuptools ];

  buildInputs = [
    qt6.qtbase
    qt6.qtwayland
  ];
  nativeBuildInputs = [
    qt6.wrapQtAppsHook
  ];
  dontWrapQtApps = true;
  makeWrapperArgs = [
    "\${qtWrapperArgs[@]}"
  ];

  pythonImportsCheck = [ "lifeblood" ];
  propagatedBuildInputs = [
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

  meta = {
    description = "";
    license = lib.licenses.gpl3;
    mainProgram = "lifeblood_viewer";
  };
}
